"""
Parte C (producer): publica en Kafka los registros de Backblaze día por día, con reloj acelerado.

- Un día de datos cada SEGUNDOS_POR_DIA segundos (por defecto 10).
- Para que el producer en Python aguante, se filtran NUM_MODELOS modelos (por defecto los 5 con más
  discos el primer día) o los de MODELOS="m1,m2". MUESTRA_PCT toma un % de los discos, elegido por
  serial para que sean siempre los mismos discos todos los días.
- Tardíos: antes del último día se esperan ESPERA_TARDIOS segundos (por defecto 40, cuatro triggers de
  Spark) para que el streaming procese el día 13 y su watermark quede en el día 11. Entonces se envía el
  último día junto con TARDIOS registros (por defecto 200) con la fecha del PRIMER día, cuya ventana ya
  cerró: Spark debe descartarlos. No se mandan antes porque, si Spark va atrasado, el watermark que usa
  un lote es el del lote anterior y la ventana podría seguir abierta todavía. Y van mezclados con datos
  normales porque, si un lote solo trae tardíos, Spark lo informa como lote de 0 filas.
  Usan seriales falsos (TARDIO-...) y falla 0: si entraran, el conteo del streaming no cuadraría con
  el de lotes.

Mensaje (JSON): {"date": "2026-04-01", "serial": "...", "model": "...", "failure": 0}
"""
import glob
import json
import os
import time
import zlib
from datetime import date

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA = os.environ.get("KAFKA", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "discos")
SEG = float(os.environ.get("SEGUNDOS_POR_DIA", "10"))
MODELOS = [m.strip() for m in os.environ.get("MODELOS", "").split(",") if m.strip()]
NUM_MODELOS = int(os.environ.get("NUM_MODELOS", "5"))
MUESTRA = float(os.environ.get("MUESTRA_PCT", "100"))
TARDIOS = int(os.environ.get("TARDIOS", "200"))
ESPERA_TARDIOS = float(os.environ.get("ESPERA_TARDIOS", "40"))
RAW = os.environ.get("RAW_DIR", "/data/raw")


def log(m):
    print(f"[producer {time.strftime('%H:%M:%S')}] {m}", flush=True)


def leer_dia(ruta):
    d = pd.read_csv(ruta, usecols=["date", "serial_number", "model", "failure"], dtype=str)
    d["model"] = d["model"].str.strip()
    d["failure"] = pd.to_numeric(d["failure"], errors="coerce").fillna(0).astype(int)
    return d.rename(columns={"serial_number": "serial"})


def en_muestra(serial: str) -> bool:
    return MUESTRA >= 100 or (zlib.crc32(serial.encode()) % 10000) < MUESTRA * 100


def crear_topic():
    admin = AdminClient({"bootstrap.servers": KAFKA})
    for intento in range(30):
        try:
            existentes = admin.list_topics(timeout=5).topics
            if TOPIC not in existentes:
                admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])[TOPIC].result()
                log(f"Topic '{TOPIC}' creado (3 particiones)")
            else:
                log(f"Topic '{TOPIC}' ya existe")
            return
        except Exception as e:  # noqa: BLE001  Kafka puede tardar en arrancar
            log(f"Esperando a Kafka ({type(e).__name__})... {intento + 1}/30")
            time.sleep(3)
    raise SystemExit("Kafka no respondió")


def main():
    archivos = sorted(glob.glob(os.path.join(RAW, "*.csv")))
    if not archivos:
        raise SystemExit(f"No hay CSV en {RAW}: correr primero 'docker compose run --rm descarga'")
    primero = leer_dia(archivos[0])
    modelos = MODELOS or primero["model"].value_counts().head(NUM_MODELOS).index.tolist()
    log(f"{len(archivos)} días. Modelos: {modelos}. Muestra: {MUESTRA:g} % de los discos. "
        f"Reloj: 1 día cada {SEG:g} s. Tardíos con el último día: {TARDIOS}")
    crear_topic()
    p = Producer({"bootstrap.servers": KAFKA, "linger.ms": 50, "batch.num.messages": 10000,
                  "queue.buffering.max.messages": 500000, "compression.type": "lz4"})

    enviados_total, tardios_total = 0, 0
    primer_dia = date.fromisoformat(primero["date"].iloc[0])
    for i, ruta in enumerate(archivos):
        ultimo = i == len(archivos) - 1
        if ultimo and TARDIOS:
            log(f"Esperando {ESPERA_TARDIOS:g} s antes del último día, para que Spark cierre la primera ventana...")
            time.sleep(ESPERA_TARDIOS)
        t0 = time.time()
        d = leer_dia(ruta) if i else primero
        d = d[d["model"].isin(modelos)]
        d = d[d["serial"].map(en_muestra)]
        for fila in d.itertuples(index=False):
            p.produce(TOPIC, json.dumps({"date": fila.date, "serial": fila.serial,
                                         "model": fila.model, "failure": int(fila.failure)}).encode())
            p.poll(0)
        enviados_total += len(d)
        if ultimo and TARDIOS:
            # tardíos: fecha del primer día, cuya ventana ya cerró el watermark; van mezclados con el último
            # día para que el lote tenga datos normales y Spark informe cuántos descartó
            for k in range(TARDIOS):
                p.produce(TOPIC, json.dumps({"date": primer_dia.isoformat(), "serial": f"TARDIO-{k}",
                                             "model": modelos[k % len(modelos)], "failure": 0}).encode())
            tardios_total = TARDIOS
        p.flush()
        dur = time.time() - t0
        log(f"Día {d['date'].iloc[0]}: {len(d):,} registros ({int(d['failure'].sum())} fallas) "
            f"en {dur:.1f} s ({len(d) / max(dur, 0.001):,.0f}/s)"
            + (f" + {TARDIOS} TARDÍOS con fecha {primer_dia} (su ventana ya está cerrada)" if ultimo and TARDIOS else ""))
        if i < len(archivos) - 1:
            time.sleep(max(0.0, SEG - dur))

    log(f"Fin: {enviados_total:,} registros y {tardios_total} tardíos publicados en '{TOPIC}'")


if __name__ == "__main__":
    main()

"""
Descarga el ZIP trimestral de Backblaze Drive Stats y extrae los primeros DIAS archivos diarios.

- El ZIP (unos 2,1 GB) se guarda en /data/zip y no se vuelve a bajar si ya está.
- Se extraen DIAS días consecutivos (por defecto 14) a /data/raw/AAAA-MM-DD.csv.
- Se revisa el encabezado del primer CSV contra las columnas que usa el ejercicio
  (date, serial_number, model, capacity_bytes, failure y smart_9_raw).

Variables: ZIP_URL, DIAS, DESDE (opcional, AAAA-MM-DD: primer día a extraer).
Fuente de los datos: Backblaze Drive Stats, https://www.backblaze.com/drivestats
"""
import csv
import json
import os
import shutil
import sys
import time
import zipfile

import requests

ZIP_URL = os.environ.get("ZIP_URL",
                         "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q2_2026.zip")
DIAS = int(os.environ.get("DIAS", "14"))
DESDE = os.environ.get("DESDE", "").strip()
ZIP_DIR, RAW_DIR = "/data/zip", "/data/raw"
NECESARIAS = ["date", "serial_number", "model", "capacity_bytes", "failure"]
OPCIONALES = ["smart_9_raw"]  # horas encendido: la "edad" del disco para Kaplan-Meier


def log(m):
    print(f"[descarga {time.strftime('%H:%M:%S')}] {m}", flush=True)


def bajar_zip() -> str:
    os.makedirs(ZIP_DIR, exist_ok=True)
    destino = os.path.join(ZIP_DIR, os.path.basename(ZIP_URL))
    if os.path.exists(destino) and zipfile.is_zipfile(destino):
        log(f"ZIP ya descargado: {destino} ({os.path.getsize(destino) / 1e9:.2f} GB)")
        return destino
    tmp = destino + ".part"
    log(f"Descargando {ZIP_URL}")
    t0, hecho = time.time(), 0
    with requests.get(ZIP_URL, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(tmp, "wb") as f:
            for bloque in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(bloque)
                hecho += len(bloque)
                if total:
                    print(f"\r  {hecho / 1e9:.2f} / {total / 1e9:.2f} GB "
                          f"({hecho / total:.0%}, {hecho / 1e6 / max(time.time() - t0, 1):.1f} MB/s)",
                          end="", flush=True)
    print()
    os.replace(tmp, destino)
    log(f"Descarga lista en {time.time() - t0:.0f} s")
    return destino


def extraer(zip_path: str) -> list[str]:
    os.makedirs(RAW_DIR, exist_ok=True)
    with zipfile.ZipFile(zip_path) as z:
        diarios = sorted(n for n in z.namelist()
                         if n.lower().endswith(".csv") and "__MACOSX" not in n
                         and os.path.basename(n)[:4].isdigit())
        if not diarios:
            sys.exit("El ZIP no tiene archivos diarios AAAA-MM-DD.csv")
        if DESDE:
            diarios = [n for n in diarios if os.path.basename(n) >= DESDE]
        elegidos = diarios[:DIAS]
        log(f"El ZIP trae {len(diarios)} días; se extraen {len(elegidos)}: "
            f"{os.path.basename(elegidos[0])} a {os.path.basename(elegidos[-1])}")
        for viejo in os.listdir(RAW_DIR):
            if viejo.endswith(".csv") and viejo not in {os.path.basename(n) for n in elegidos}:
                os.remove(os.path.join(RAW_DIR, viejo))
        salida = []
        for n in elegidos:
            dest = os.path.join(RAW_DIR, os.path.basename(n))
            if not os.path.exists(dest):
                with z.open(n) as src, open(dest, "wb") as dst:
                    shutil.copyfileobj(src, dst, 16 * 1024 * 1024)
            salida.append(dest)
            log(f"  {os.path.basename(dest)}  {os.path.getsize(dest) / 1e6:.0f} MB")
        return salida


def revisar_esquema(primer_csv: str) -> dict:
    with open(primer_csv, newline="") as f:
        encabezado = next(csv.reader(f))
    faltan = [c for c in NECESARIAS if c not in encabezado]
    info = {
        "archivo": os.path.basename(primer_csv),
        "columnas_totales": len(encabezado),
        "necesarias_presentes": [c for c in NECESARIAS if c in encabezado],
        "faltan": faltan,
        "smart_9_raw": "smart_9_raw" in encabezado,
        "primeras_columnas": encabezado[:12],
    }
    log("Esquema: " + json.dumps(info, ensure_ascii=False))
    if faltan:
        sys.exit(f"ERROR: faltan columnas necesarias {faltan}. Revisar el esquema del trimestre.")
    if not info["smart_9_raw"]:
        log("AVISO: no viene smart_9_raw; Kaplan-Meier usará los días observados en vez de la edad.")
    return info


def main():
    archivos = extraer(bajar_zip())
    info = revisar_esquema(archivos[0])
    info["dias_extraidos"] = [os.path.basename(a) for a in archivos]
    info["fuente"] = ZIP_URL
    os.makedirs("/resultados", exist_ok=True)
    with open("/resultados/00_esquema_y_muestra.json", "w") as f:
        json.dump(info, f, indent=2, ensure_ascii=False)
    log(f"Listo: {len(archivos)} archivos en {RAW_DIR}. Resumen en resultados/00_esquema_y_muestra.json")


if __name__ == "__main__":
    main()

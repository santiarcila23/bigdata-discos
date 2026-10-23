"""
Parte B: Balanceo de carga.

1. Tiempos: el MapReduce de la Parte A con 2, 8, 32 y 200 particiones, REPETICIONES veces cada uno.
   El reparto (repartition + cache) se hace antes y no se cronometra: se mide solo el cálculo.
2. Sesgo: se reparte por modelo (repartition(n, "model")) y sin clave (repartition(n), round-robin),
   y se cuentan las filas de cada partición con spark_partition_id().

Salidas: resultados/parte_b_tiempos.csv, parte_b_filas_por_particion.csv, parte_b_modelos_top.csv,
         parte_b_resumen.json
"""
import json
import os
import statistics

from pyspark.sql import functions as F

from comun import PARQUET, RES, Cronometro, guardar_csv, log, sesion

PARTICIONES = [int(x) for x in os.environ.get("PARTICIONES", "2,8,32,200").split(",")]
REPETICIONES = int(os.environ.get("REPETICIONES", "3"))
N_SESGO = int(os.environ.get("N_SESGO", "8"))

spark = sesion("parte_b_balanceo")
sc = spark.sparkContext
base = spark.read.parquet(PARQUET).select("model", "failure").cache()
total = base.count()
nucleos = sc.defaultParallelism
log("B", f"Filas: {total:,}. Núcleos disponibles (defaultParallelism): {nucleos}. "
         f"spark.sql.shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")


def mapreduce(df, n):
    return (df.rdd.map(lambda r: (r[0], (1, r[1])))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]), numPartitions=n)
            .collect())


# ------------------------------------------------------------------ 1. tiempos por número de particiones
filas_t, resumen_t = [], []
referencia = None
for n in PARTICIONES:
    datos = base.repartition(n).cache()
    datos.count()                                   # materializa el reparto (no se cronometra)
    tiempos = []
    for rep in range(1, REPETICIONES + 1):
        with Cronometro() as c:
            res = mapreduce(datos, n)
        tiempos.append(c.segundos)
        filas_t.append([n, rep, round(c.segundos, 3), len(res)])
        log("B", f"  {n:>3} particiones, repetición {rep}: {c.segundos:.2f} s ({len(res)} modelos)")
    resultado = sorted(res)
    if referencia is None:
        referencia = resultado
    iguales = resultado == referencia
    med = statistics.median(tiempos)
    resumen_t.append({"particiones": n, "mediana_s": round(med, 3), "min_s": round(min(tiempos), 3),
                      "max_s": round(max(tiempos), 3), "resultado_igual": iguales})
    log("B", f"{n:>3} particiones -> mediana {med:.2f} s (min {min(tiempos):.2f}, max {max(tiempos):.2f}); "
             f"mismo resultado: {iguales}")
    datos.unpersist()
guardar_csv("parte_b_tiempos.csv", ["particiones", "repeticion", "segundos", "modelos"], filas_t)

# ------------------------------------------------------------------ 2. sesgo: filas por partición
def filas_por_particion(df):
    return {r["pid"]: r["filas"] for r in
            df.groupBy(F.spark_partition_id().alias("pid")).agg(F.count("*").alias("filas")).collect()}


por_clave = filas_por_particion(base.repartition(N_SESGO, "model"))
sin_clave = filas_por_particion(base.repartition(N_SESGO))

filas_p = []
for pid in range(N_SESGO):
    filas_p.append([pid, por_clave.get(pid, 0), sin_clave.get(pid, 0)])
guardar_csv("parte_b_filas_por_particion.csv", ["particion", "por_modelo", "sin_clave"], filas_p)


def estadisticas(d):
    v = [d.get(i, 0) for i in range(N_SESGO)]
    media = sum(v) / len(v)
    return {"max": max(v), "min": min(v), "media": round(media), "max_sobre_media": round(max(v) / media, 2),
            "particiones_vacias": sum(1 for x in v if x == 0)}


est_clave, est_sin = estadisticas(por_clave), estadisticas(sin_clave)
log("B", f"Reparto POR MODELO en {N_SESGO} particiones: {est_clave}")
log("B", f"Reparto SIN CLAVE  en {N_SESGO} particiones: {est_sin}")

# qué modelos caen en la partición más cargada
pesada = max(por_clave, key=por_clave.get)
en_pesada = (base.repartition(N_SESGO, "model")
             .where(F.spark_partition_id() == pesada)
             .groupBy("model").count().orderBy(F.desc("count")).limit(5).collect())
log("B", f"Partición más cargada ({pesada}): " +
    ", ".join(f"{r['model']} ({r['count']:,})" for r in en_pesada))

# concentración: cuánto pesan los modelos más grandes
top = base.groupBy("model").count().orderBy(F.desc("count")).collect()
acum, filas_m = 0, []
for i, r in enumerate(top, 1):
    acum += r["count"]
    filas_m.append([i, r["model"], r["count"], round(r["count"] / total * 100, 2), round(acum / total * 100, 2)])
guardar_csv("parte_b_modelos_top.csv", ["puesto", "modelo", "filas", "pct", "pct_acumulado"], filas_m)
for k in (1, 3, 5, 10):
    if len(filas_m) >= k:
        log("B", f"Los {k:>2} modelos más grandes concentran el {filas_m[k - 1][4]:.1f} % de las filas "
                 f"(de {len(filas_m)} modelos)")

resumen = {"filas": total, "nucleos": nucleos, "repeticiones": REPETICIONES, "tiempos": resumen_t,
           "sesgo_particiones": N_SESGO, "por_modelo": est_clave, "sin_clave": est_sin,
           "particion_mas_cargada": pesada,
           "modelos_en_particion_mas_cargada": [[r["model"], r["count"]] for r in en_pesada],
           "concentracion_top": {str(k): filas_m[k - 1][4] for k in (1, 3, 5, 10) if len(filas_m) >= k},
           "modelos": len(filas_m)}
with open(os.path.join(RES, "parte_b_resumen.json"), "w") as f:
    json.dump(resumen, f, indent=2)
spark.stop()

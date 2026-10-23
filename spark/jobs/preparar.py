"""
Preparación: lee los CSV diarios de Backblaze y deja en Parquet solo las columnas que se usan.

Los CSV traen unas 190 columnas (todas las SMART); el ejercicio usa 6. Leerlas una vez y guardarlas
en Parquet hace que las partes A y B midan el cálculo y no el parseo de texto.
"""
import json
import os

from pyspark.sql import functions as F

from comun import PARQUET, RAW, RES, log, sesion

spark = sesion("preparar")
cols = ["date", "serial_number", "model", "capacity_bytes", "failure", "smart_9_raw"]

crudo = spark.read.option("header", True).csv(os.path.join(RAW, "*.csv"))
presentes = [c for c in cols if c in crudo.columns]
log("preparar", f"Columnas del CSV: {len(crudo.columns)}; se usan: {presentes}")

df = crudo.select(
    F.to_date("date").alias("date"),
    F.col("serial_number").alias("serial"),
    F.trim("model").alias("model"),
    F.col("capacity_bytes").cast("long").alias("capacity_bytes"),
    F.col("failure").cast("int").alias("failure"),
    (F.col("smart_9_raw").cast("double") if "smart_9_raw" in crudo.columns
     else F.lit(None).cast("double")).alias("power_on_hours"),
).where(F.col("serial").isNotNull() & F.col("model").isNotNull() & F.col("date").isNotNull())

df.write.mode("overwrite").partitionBy("date").parquet(PARQUET)

p = spark.read.parquet(PARQUET)
resumen = p.agg(F.count("*").alias("filas"), F.countDistinct("serial").alias("discos"),
                F.countDistinct("model").alias("modelos"), F.sum("failure").alias("fallas"),
                F.min("date").alias("desde"), F.max("date").alias("hasta"),
                F.countDistinct("date").alias("dias")).first().asDict()
resumen = {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in resumen.items()}
resumen["filas_por_dia_promedio"] = round(resumen["filas"] / resumen["dias"])
log("preparar", "Resumen: " + json.dumps(resumen))
os.makedirs(RES, exist_ok=True)
with open(os.path.join(RES, "01_resumen_muestra.json"), "w") as f:
    json.dump(resumen, f, indent=2)
spark.stop()

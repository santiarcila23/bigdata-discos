"""
Parte A: MapReduce.

1. Con RDD: map -> (modelo, (1, falla)); reduceByKey suma días-disco y fallas por modelo.
2. Con DataFrames: groupBy(modelo) con count y sum. Se comprueba que ambos coinciden modelo a modelo.
3. Tasa de falla anualizada: AFR (%) = fallas / (días-disco / 365) * 100, por modelo y total.
4. Censura: días observados por serial. En una ventana corta casi todos los discos siguen vivos:
   solo se sabe que aún no han fallado (censura por la derecha).

Salidas: resultados/parte_a_afr_por_modelo.csv, parte_a_resumen.json, parte_a_censura.csv,
         /data/parquet/vidas (una fila por disco, la usa Kaplan-Meier) y la colección afr_modelos en MongoDB.
"""
import json
import os

from pyspark.sql import functions as F

from comun import PARQUET, RES, VIDAS, Cronometro, afr, guardar_csv, log, sesion

MIN_DIAS_DISCO = int(os.environ.get("MIN_DIAS_DISCO", "10000"))
spark = sesion("parte_a_mapreduce")
df = spark.read.parquet(PARQUET).select("date", "serial", "model", "failure", "power_on_hours").cache()
total_filas = df.count()
num_dias = df.select("date").distinct().count()
log("A", f"Filas (días-disco): {total_filas:,} en {num_dias} días")

# ------------------------------------------------------------------ 1. MapReduce con RDD
with Cronometro() as t_rdd:
    por_modelo_rdd = (df.select("model", "failure").rdd
                      .map(lambda fila: (fila[0], (1, fila[1])))                 # Map
                      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))       # Shuffle + Reduce
                      .collect())                                                 # acción
rdd_dict = {m: (d, f) for m, (d, f) in por_modelo_rdd}
log("A", f"RDD reduceByKey: {len(rdd_dict)} modelos en {t_rdd.segundos:.2f} s")

# ------------------------------------------------------------------ 2. El mismo cálculo con DataFrames
with Cronometro() as t_df:
    por_modelo_df = (df.groupBy("model")
                     .agg(F.count("*").alias("dias_disco"), F.sum("failure").alias("fallas"))
                     .collect())
df_dict = {r["model"]: (r["dias_disco"], r["fallas"]) for r in por_modelo_df}
log("A", f"DataFrame groupBy: {len(df_dict)} modelos en {t_df.segundos:.2f} s")

distintos = [m for m in set(rdd_dict) | set(df_dict) if rdd_dict.get(m) != df_dict.get(m)]
coinciden = len(distintos) == 0
log("A", f"¿Coinciden RDD y DataFrame? {'SÍ' if coinciden else 'NO'}: "
         f"{len(rdd_dict) - len(distintos)} de {len(rdd_dict)} modelos iguales")
if not coinciden:
    for m in distintos[:10]:
        log("A", f"  diferencia en {m}: RDD={rdd_dict.get(m)} DF={df_dict.get(m)}")

# ------------------------------------------------------------------ 3. AFR por modelo y total
filas = []
for m, (dias, fallas) in sorted(rdd_dict.items(), key=lambda kv: -kv[1][0]):
    filas.append([m, dias, fallas, round(afr(fallas, dias), 3),
                  "sí" if dias >= MIN_DIAS_DISCO else "no"])
guardar_csv("parte_a_afr_por_modelo.csv",
            ["modelo", "dias_disco", "fallas", "afr_pct", f"confiable_(>= {MIN_DIAS_DISCO} dias_disco)"], filas)

total_dias = sum(d for d, _ in rdd_dict.values())
total_fallas = sum(f for _, f in rdd_dict.values())
afr_total = afr(total_fallas, total_dias)
log("A", f"TOTAL: {total_fallas:,} fallas en {total_dias:,} días-disco -> AFR = {afr_total:.2f} %")
log("A", "Comprobación de la fórmula con las cifras de Backblaze Q2 2026: "
         f"1.498 fallas / 31.553.350 días-disco -> {afr(1498, 31553350):.2f} % (reportado: 1,73 %)")

confiables = [f for f in filas if f[1] >= MIN_DIAS_DISCO]
log("A", f"Modelos: {len(filas)} en total, {len(confiables)} con al menos {MIN_DIAS_DISCO:,} días-disco")
log("A", "Top 10 por AFR entre los confiables:")
for f in sorted(confiables, key=lambda x: -x[3])[:10]:
    log("A", f"  {f[0]:<28} días-disco={f[1]:>9,}  fallas={f[2]:>4}  AFR={f[3]:>6.2f} %")

# ------------------------------------------------------------------ 4. Censura: días observados por serial
vidas = (df.groupBy("serial", "model")
         .agg(F.count("*").alias("dias_observados"),
              F.max("failure").alias("fallo"),
              F.min("date").alias("primer_dia"), F.max("date").alias("ultimo_dia"),
              F.min("power_on_hours").alias("horas_inicio"), F.max("power_on_hours").alias("horas_fin")))
vidas.write.mode("overwrite").parquet(VIDAS)
vidas = spark.read.parquet(VIDAS)

dist = (vidas.groupBy("dias_observados")
        .agg(F.count("*").alias("discos"), F.sum("fallo").alias("fallaron"))
        .orderBy("dias_observados").collect())
guardar_csv("parte_a_censura.csv", ["dias_observados", "discos", "fallaron"],
            [[r["dias_observados"], r["discos"], r["fallaron"]] for r in dist])
discos = vidas.count()
fallaron = vidas.where("fallo = 1").count()
completos = vidas.where(F.col("dias_observados") == num_dias).count()
log("A", f"Discos distintos: {discos:,}. Fallaron en la ventana: {fallaron:,} "
         f"({fallaron / discos:.3%}). Censurados (siguen vivos): {discos - fallaron:,} "
         f"({(discos - fallaron) / discos:.2%})")
log("A", f"Observados los {num_dias} días completos: {completos:,} ({completos / discos:.1%})")

# Guardar en MongoDB para la Parte D
try:
    (spark.createDataFrame([(r[0], int(r[1]), int(r[2]), float(r[3])) for r in filas],
                           "modelo string, dias_disco long, fallas long, afr_pct double")
     .withColumn("_id", F.col("modelo"))
     .write.format("mongodb").mode("overwrite")
     .option("database", "discos").option("collection", "afr_modelos").save())
    log("A", "Guardado en MongoDB: discos.afr_modelos")
except Exception as e:  # noqa: BLE001  (MongoDB es opcional)
    log("A", f"AVISO: no se pudo guardar en MongoDB ({type(e).__name__}); se sigue sin la Parte D")

resumen = {
    "filas_dias_disco": total_filas, "dias": num_dias, "modelos": len(filas),
    "fallas": total_fallas, "afr_total_pct": round(afr_total, 3),
    "rdd_y_dataframe_coinciden": coinciden, "modelos_distintos": len(distintos),
    "segundos_rdd": round(t_rdd.segundos, 2), "segundos_dataframe": round(t_df.segundos, 2),
    "discos": discos, "fallaron": fallaron, "censurados": discos - fallaron,
    "observados_todos_los_dias": completos,
    "min_dias_disco_confiable": MIN_DIAS_DISCO, "modelos_confiables": len(confiables),
}
with open(os.path.join(RES, "parte_a_resumen.json"), "w") as f:
    json.dump(resumen, f, indent=2)
log("A", "Resumen: " + json.dumps(resumen))
spark.stop()

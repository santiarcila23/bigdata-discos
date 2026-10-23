"""Utilidades compartidas por los trabajos de Spark."""
import csv
import os
import time
from datetime import datetime

from pyspark.sql import SparkSession

RAW = os.environ.get("RAW_DIR", "/data/raw")
PARQUET = os.environ.get("PARQUET_DIR", "/data/parquet/discos")
VIDAS = os.environ.get("VIDAS_DIR", "/data/parquet/vidas")
RES = os.environ.get("RESULTADOS_DIR", "/resultados")


def sesion(nombre: str) -> SparkSession:
    spark = (SparkSession.builder.appName(nombre)
             .config("spark.mongodb.write.connection.uri", os.environ.get("MONGO_URI", "mongodb://mongo:27017"))
             .config("spark.mongodb.read.connection.uri", os.environ.get("MONGO_URI", "mongodb://mongo:27017"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def log(etiqueta: str, msg: str) -> None:
    print(f"[{etiqueta} {datetime.now():%H:%M:%S}] {msg}", flush=True)


def afr(fallas: float, dias_disco: float) -> float:
    """Tasa de falla anualizada (%) = fallas / (días-disco / 365) * 100."""
    return fallas / (dias_disco / 365.0) * 100.0 if dias_disco else float("nan")


def guardar_csv(nombre: str, encabezado: list, filas: list) -> str:
    """Guarda resultados pequeños (ya recogidos en el driver) como CSV en /resultados."""
    os.makedirs(RES, exist_ok=True)
    ruta = os.path.join(RES, nombre)
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(encabezado)
        w.writerows(filas)
    return ruta


class Cronometro:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *_):
        self.segundos = time.perf_counter() - self.t0

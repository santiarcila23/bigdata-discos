"""
Parte D (opcional): consultas en MongoDB.

- discos.afr_ventanas: agregados por modelo y ventana de 7 días (escritos por el streaming, Parte C).
- discos.afr_modelos: AFR por modelo de toda la muestra (escrito por la Parte A).

Se piden los 5 modelos con mayor AFR que tengan al menos MIN_DIAS_DISCO días-disco, para no sacar
conclusiones con muestras pequeñas. Salidas: resultados/parte_d_*.csv
"""
import csv
import os

from pymongo import DESCENDING, MongoClient

MIN_VENTANA = int(os.environ.get("MIN_DIAS_DISCO_VENTANA", "10000"))
MIN_MODELO = int(os.environ.get("MIN_DIAS_DISCO", "10000"))
db = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongo:27017"))["discos"]


def guardar(nombre, docs, campos):
    with open(os.path.join(os.environ.get("RESULTADOS_DIR", "/resultados"), nombre), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(campos)
        for d in docs:
            w.writerow([d.get(c) for c in campos])


def mostrar(titulo, docs, campos):
    print(f"\n{titulo}")
    print("  " + " | ".join(campos))
    for d in docs:
        print("  " + " | ".join(str(d.get(c)) for c in campos))


# índices para las consultas
db.afr_ventanas.create_index([("afr_pct", DESCENDING)])
db.afr_ventanas.create_index([("model", 1), ("inicio", 1)])
db.afr_modelos.create_index([("afr_pct", DESCENDING)])

# 1) todas las ventanas guardadas por el streaming
campos_v = ["model", "inicio", "fin", "dias_disco", "fallas", "afr_pct"]
ventanas = list(db.afr_ventanas.find({}, {"_id": 0}).sort([("inicio", 1), ("model", 1)]))
guardar("parte_d_ventanas.csv", ventanas, campos_v)
mostrar(f"Ventanas guardadas por el streaming: {len(ventanas)}", ventanas, campos_v)

# 2) top 5 por AFR con mínimo de días-disco, por ventana
top_v = list(db.afr_ventanas.aggregate([
    {"$match": {"dias_disco": {"$gte": MIN_VENTANA}}},
    {"$sort": {"afr_pct": -1}},
    {"$limit": 5},
    {"$project": {"_id": 0, **{c: 1 for c in campos_v}}},
]))
guardar("parte_d_top5_ventanas.csv", top_v, campos_v)
mostrar(f"Top 5 modelo-ventana por AFR (días-disco >= {MIN_VENTANA:,})", top_v, campos_v)

# 3) top 5 por AFR con mínimo de días-disco, en toda la muestra (Parte A)
campos_m = ["modelo", "dias_disco", "fallas", "afr_pct"]
top_m = list(db.afr_modelos.find({"dias_disco": {"$gte": MIN_MODELO}}, {"_id": 0})
             .sort("afr_pct", DESCENDING).limit(5))
guardar("parte_d_top5_modelos.csv", top_m, campos_m)
mostrar(f"Top 5 modelos por AFR en toda la muestra (días-disco >= {MIN_MODELO:,})", top_m, campos_m)

# por qué hace falta el mínimo: el top 5 sin filtro
sin_filtro = list(db.afr_modelos.find({}, {"_id": 0}).sort("afr_pct", DESCENDING).limit(5))
mostrar("Para comparar: top 5 SIN mínimo de días-disco (muestras pequeñas)", sin_filtro, campos_m)
guardar("parte_d_top5_sin_filtro.csv", sin_filtro, campos_m)

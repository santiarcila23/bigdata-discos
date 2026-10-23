"""
Reto de investigación: análisis de supervivencia con Kaplan-Meier.

Entrada: /data/parquet/vidas (una fila por disco, generada por la Parte A con Spark).

Tiempo de vida = edad del disco en días, a partir de las horas encendido (SMART 9 / 24).
En la ventana observada cada disco:
  - entra ya con una edad (horas_inicio): truncamiento por la izquierda. Solo vemos discos que
    sobrevivieron hasta esa edad, así que entran al conjunto en riesgo desde ahí (entry=).
  - sale fallando (evento = 1) o sigue vivo al final (censura por la derecha, evento = 0).

Se compara con un ajuste "ingenuo" que ignora el truncamiento, para mostrar el sesgo.
Si no hay horas encendido, se usan los días observados en la ventana (curva muy corta).

Salidas: resultados/km_curvas.png, km_ingenuo_vs_truncado.png, km_resumen.csv, km_datos.json
"""
import json
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from lifelines import KaplanMeierFitter

VIDAS = os.environ.get("VIDAS_DIR", "/data/parquet/vidas")
RES = os.environ.get("RESULTADOS_DIR", "/resultados")
NUM_MODELOS = int(os.environ.get("NUM_MODELOS_KM", "6"))
MAX_ANIOS = float(os.environ.get("MAX_ANIOS", "12"))

v = pd.read_parquet(VIDAS)
print(f"Discos: {len(v):,}  fallaron: {int(v['fallo'].sum()):,}")

tiene_edad = v["horas_inicio"].notna().mean() > 0.5
if tiene_edad:
    v = v.dropna(subset=["horas_inicio", "horas_fin"]).copy()
    v["entrada"] = v["horas_inicio"] / 24.0
    v["salida"] = v["horas_fin"] / 24.0 + 1.0          # el último día observado cuenta completo
    validos = (v["entrada"] >= 0) & (v["salida"] > v["entrada"]) & (v["salida"] <= MAX_ANIOS * 365)
    print(f"Con edad válida (0 a {MAX_ANIOS:g} años): {validos.sum():,} de {len(v):,}")
    v = v[validos]
    unidad, escala = "años", 365.0
else:
    print("AVISO: sin horas encendido; se usan los días observados en la ventana")
    v["entrada"] = 0.0
    v["salida"] = v["dias_observados"].astype(float)
    unidad, escala = "días", 1.0

modelos = v["model"].value_counts().head(NUM_MODELOS).index.tolist()
MIN_EN_RIESGO = int(os.environ.get("MIN_EN_RIESGO", "50"))


def ajustar(d, etiqueta, truncado=True):
    """Ajusta Kaplan-Meier con truncamiento por la izquierda.

    Con truncamiento, a edades muy bajas casi no hay discos en riesgo (pocos discos nuevos) y una sola
    falla puede llevar la curva a 0. Por eso la curva empieza en la edad a0 desde la que hay al menos
    MIN_EN_RIESGO discos en riesgo: es la supervivencia condicionada a haber llegado a a0, S(t | T > a0).
    """
    t, e, a = d["salida"] / escala, d["fallo"], d["entrada"] / escala
    if not (truncado and tiene_edad):
        return KaplanMeierFitter().fit(t, e, label=etiqueta), 0.0
    entradas = np.sort(a.to_numpy())
    a0 = float(entradas[min(MIN_EN_RIESGO, len(entradas)) - 1])
    sel = t > a0
    return KaplanMeierFitter().fit(t[sel], e[sel], entry=np.maximum(a[sel], a0),
                                   label=f"{etiqueta} [desde {a0:.1f} {unidad}]"), a0


filas = []
fig, ax = plt.subplots(figsize=(9, 5.5))
for m in modelos:
    d = v[v["model"] == m]
    try:
        kmf, a0 = ajustar(d, f"{m} (n={len(d):,})")
    except Exception as ex:  # noqa: BLE001
        print(f"AVISO: no se pudo ajustar {m}: {ex}")
        continue
    kmf.plot_survival_function(ax=ax, ci_show=True)
    fila = {"modelo": m, "discos": len(d), "fallas": int(d["fallo"].sum()),
            "censurados": int((d["fallo"] == 0).sum()),
            "edad_mediana_entrada": round(float(d["entrada"].median() / escala), 2),
            f"curva_desde_{unidad}": round(a0, 2)}
    for t in ([1, 3, 5] if tiene_edad else [7, 14]):
        try:
            fila[f"S({t} {unidad})"] = round(float(kmf.survival_function_at_times(t).iloc[0]), 4)
        except Exception:  # noqa: BLE001
            fila[f"S({t} {unidad})"] = None
    filas.append(fila)
ax.set_title("Kaplan-Meier por modelo: probabilidad de seguir funcionando según la edad")
ax.set_xlabel(f"Edad del disco ({unidad})")
ax.set_ylabel("Supervivencia S(t)")
ax.grid(alpha=0.3)
ax.legend(fontsize=8)
fig.tight_layout()
fig.savefig(os.path.join(RES, "km_curvas.png"), dpi=150)

resumen = pd.DataFrame(filas)
resumen.to_csv(os.path.join(RES, "km_resumen.csv"), index=False)
print(resumen.to_string(index=False))

# ingenuo vs con truncamiento, para el modelo con más discos
comparacion = {}
if tiene_edad:
    m = modelos[0]
    d = v[v["model"] == m]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    k1, _ = ajustar(d, "Ingenuo: todos en riesgo desde la edad 0", truncado=False)
    k2, a0 = ajustar(d, "Con truncamiento: en riesgo desde su edad de entrada")
    k1.plot_survival_function(ax=ax)
    k2.plot_survival_function(ax=ax)
    ax.set_title(f"Por qué importa el truncamiento por la izquierda ({m})")
    ax.set_xlabel("Edad del disco (años)")
    ax.set_ylabel("Supervivencia S(t)")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(RES, "km_ingenuo_vs_truncado.png"), dpi=150)
    t_max = float(np.floor(d["salida"].max() / escala))
    t_cmp = max(a0 + 0.5, min(5.0, t_max))
    comparacion = {"modelo": m, "t_anios": t_cmp,
                   "S_ingenuo": round(float(k1.survival_function_at_times(t_cmp).iloc[0]), 4),
                   "S_truncado": round(float(k2.survival_function_at_times(t_cmp).iloc[0]), 4)}
    print(f"\nComparación en {m} a {t_cmp:g} años: ingenuo={comparacion['S_ingenuo']}, "
          f"con truncamiento={comparacion['S_truncado']}")

with open(os.path.join(RES, "km_datos.json"), "w") as f:
    json.dump({"usa_edad_smart9": bool(tiene_edad), "discos_usados": int(len(v)),
               "fallas_usadas": int(v["fallo"].sum()), "modelos": modelos, "comparacion": comparacion},
              f, indent=2, ensure_ascii=False)
print("Listo: resultados/km_curvas.png, km_ingenuo_vs_truncado.png, km_resumen.csv")

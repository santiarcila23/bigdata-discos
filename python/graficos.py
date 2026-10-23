"""
Gráficos a partir de los resultados ya calculados (resultados/*.csv):

1. graf_tiempos_particiones.png  Tiempo del MapReduce según el número de particiones (Parte B).
2. graf_filas_por_particion.png  Filas por partición: reparto por modelo vs sin clave (Parte B).
3. graf_afr_modelos.png          AFR por modelo con intervalo de confianza del 95 % (Parte A).
4. graf_afr_ventanas.png         AFR por ventana de 7 días y modelo (Parte C), si hay datos.
"""
import os

import matplotlib.pyplot as plt
import pandas as pd
from scipy.stats import chi2

R = os.environ.get("RESULTADOS_DIR", "/resultados")
MIN_DIAS = int(os.environ.get("MIN_DIAS_DISCO", "10000"))


def ok(n):
    return os.path.exists(os.path.join(R, n))


if ok("parte_b_tiempos.csv"):
    t = pd.read_csv(os.path.join(R, "parte_b_tiempos.csv"))
    g = t.groupby("particiones")["segundos"].agg(["median", "min", "max"]).reset_index()
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(g["particiones"], g["median"], marker="o", color="black", label="mediana")
    ax.fill_between(g["particiones"], g["min"], g["max"], color="0.8", label="mín–máx")
    for _, r in g.iterrows():
        ax.annotate(f"{r['median']:.2f} s", (r["particiones"], r["median"]), textcoords="offset points",
                    xytext=(0, 8), ha="center", fontsize=9)
    ax.set_xscale("log", base=2)
    ax.set_xticks(g["particiones"], [str(x) for x in g["particiones"]])
    ax.set_xlabel("Particiones")
    ax.set_ylabel("Segundos")
    ax.set_title(f"MapReduce de la Parte A según el número de particiones ({t['repeticion'].max()} repeticiones)")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(R, "graf_tiempos_particiones.png"), dpi=150)
    print("graf_tiempos_particiones.png")

if ok("parte_b_filas_por_particion.csv"):
    p = pd.read_csv(os.path.join(R, "parte_b_filas_por_particion.csv"))
    fig, ax = plt.subplots(figsize=(8, 4.5))
    w = 0.4
    ax.bar(p["particion"] - w / 2, p["por_modelo"], w, color="0.2", label="repartition(n, 'model')")
    ax.bar(p["particion"] + w / 2, p["sin_clave"], w, color="0.65", label="repartition(n), sin clave")
    ax.axhline(p["sin_clave"].mean(), ls="--", color="black", lw=1, label="reparto ideal (media)")
    ax.set_xlabel("Partición (spark_partition_id)")
    ax.set_ylabel("Filas")
    ax.set_title("Filas por partición: repartir por modelo concentra la carga")
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(os.path.join(R, "graf_filas_por_particion.png"), dpi=150)
    print("graf_filas_por_particion.png")

if ok("parte_a_afr_por_modelo.csv"):
    a = pd.read_csv(os.path.join(R, "parte_a_afr_por_modelo.csv"))
    a = a[a["dias_disco"] >= MIN_DIAS].copy()
    # intervalo de confianza exacto de Poisson para el número de fallas
    a["lo"] = [chi2.ppf(0.025, 2 * k) / 2 if k > 0 else 0 for k in a["fallas"]]
    a["hi"] = [chi2.ppf(0.975, 2 * (k + 1)) / 2 for k in a["fallas"]]
    años = a["dias_disco"] / 365
    a["afr_lo"], a["afr_hi"] = a["lo"] / años * 100, a["hi"] / años * 100
    a = a.sort_values("dias_disco", ascending=False).head(15).sort_values("afr_pct")
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.errorbar(a["afr_pct"], range(len(a)), xerr=[a["afr_pct"] - a["afr_lo"], a["afr_hi"] - a["afr_pct"]],
                fmt="o", color="black", ecolor="0.6", capsize=3)
    ax.set_yticks(range(len(a)), [f"{m}  ({int(d):,} d-d, {int(f)} fallas)"
                                  for m, d, f in zip(a["modelo"], a["dias_disco"], a["fallas"])], fontsize=8)
    ax.set_xlabel("AFR (%) con intervalo de confianza del 95 %")
    ax.set_title("Los 15 modelos con más días-disco: AFR y su incertidumbre")
    ax.grid(alpha=0.3, axis="x")
    fig.tight_layout()
    fig.savefig(os.path.join(R, "graf_afr_modelos.png"), dpi=150)
    print("graf_afr_modelos.png")

if ok("parte_d_ventanas.csv"):
    v = pd.read_csv(os.path.join(R, "parte_d_ventanas.csv"))
    if len(v):
        v["inicio"] = pd.to_datetime(v["inicio"]).dt.strftime("%Y-%m-%d")
        tabla = v.pivot_table(index="inicio", columns="model", values="afr_pct")
        fig, ax = plt.subplots(figsize=(9, 4.5))
        tabla.plot(kind="bar", ax=ax, colormap="Greys", edgecolor="black")
        ax.set_xlabel("Inicio de la ventana de 7 días")
        ax.set_ylabel("AFR (%)")
        ax.set_title("AFR por modelo y ventana (Structured Streaming)")
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3, axis="y")
        plt.xticks(rotation=0)
        fig.tight_layout()
        fig.savefig(os.path.join(R, "graf_afr_ventanas.png"), dpi=150)
        print("graf_afr_ventanas.png")

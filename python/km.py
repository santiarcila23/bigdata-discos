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

Curvas por modelo, por marca y para todos los discos juntos.
Salidas: resultados/km_curvas.png, km_curvas_marcas.png, km_ingenuo_vs_truncado.png, km_resumen.csv,
km_datos.json
"""
import json
import os
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from lifelines import KaplanMeierFitter

warnings.filterwarnings("ignore", category=FutureWarning)

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
MIN_EN_RIESGO = int(os.environ.get("MIN_EN_RIESGO", "50"))   # discos en riesgo mínimos en cada falla
MIN_COBERTURA = int(os.environ.get("MIN_COBERTURA", "10"))   # discos en riesgo mínimos en todo el tramo


def tramo_con_riesgo(a, t, e, minimo, cobertura):
    """Tramo de edades [a0, a1] más largo que se puede estimar bien.

    Con truncamiento cada disco solo está en riesgo entre su edad de entrada y su edad de salida
    (unos 14 días). Donde casi no hay discos de cierta edad, una falla con 1 o 2 discos en riesgo
    hunde la curva a 0. El tramo cumple dos condiciones:
      - en todo momento hay al menos `cobertura` discos en riesgo (sin huecos de edades sin datos);
      - toda falla dentro del tramo ocurre con al menos `minimo` discos en riesgo.
    En riesgo en el tramo (x, siguiente]: entrada <= x y salida > x.
    """
    a_ord, t_ord = np.sort(a), np.sort(t)
    puntos = np.unique(np.concatenate([a_ord, t_ord]))
    n = np.searchsorted(a_ord, puntos, side="right") - np.searchsorted(t_ord, puntos, side="right")
    n = n[:-1]                              # tramo i va de puntos[i] a puntos[i+1]
    ok = n >= cobertura
    fallas = np.unique(t[e == 1])
    i_f = np.searchsorted(puntos, fallas) - 1  # tramo que termina en la edad de la falla
    i_f = i_f[(i_f >= 0) & (i_f < len(n))]
    ok[i_f[n[i_f] < minimo]] = False
    mejor, inicio = (None, 0.0), None
    for i, bueno in enumerate(np.append(ok, False)):
        if bueno and inicio is None:
            inicio = i
        elif not bueno and inicio is not None:
            largo = puntos[i] - puntos[inicio]
            if mejor[0] is None or largo > mejor[1]:
                mejor = ((float(puntos[inicio]), float(puntos[i])), largo)
            inicio = None
    return mejor[0]


def ajustar(d, etiqueta, truncado=True):
    """Ajusta Kaplan-Meier con truncamiento por la izquierda y censura por la derecha.

    La curva se estima solo en el tramo de edades [a0, a1] que da tramo_con_riesgo: es la supervivencia condicionada a haber llegado a a0, S(t | T > a0). Los discos
    que siguen vivos más allá de a1 quedan censurados en a1.
    """
    t, e, a = d["salida"] / escala, d["fallo"], d["entrada"] / escala
    if not (truncado and tiene_edad):
        return KaplanMeierFitter().fit(t, e, label=etiqueta), (0.0, float(t.max()))
    tramo = tramo_con_riesgo(a.to_numpy(), t.to_numpy(), e.to_numpy(), MIN_EN_RIESGO, MIN_COBERTURA)
    if tramo is None:
        raise ValueError(f"ningún tramo de edades con {MIN_EN_RIESGO} discos en riesgo")
    a0, a1 = tramo
    sel = (t > a0) & (a < a1)
    t_s = np.minimum(t[sel], a1)
    e_s = np.where(t[sel] > a1, 0, e[sel])            # vivo después de a1: censurado en a1
    kmf = KaplanMeierFitter().fit(t_s, e_s, entry=np.maximum(a[sel], a0),
                                  label=f"{etiqueta} [{a0:.1f}–{a1:.1f} {unidad}]")
    return kmf, (a0, a1)


def marca(modelo):
    """Fabricante a partir del nombre del modelo (los Seagate empiezan por ST o 'Seagate')."""
    m = str(modelo).upper()
    for nombre, prefijos in (("Seagate", ("ST", "SEAGATE")), ("WDC", ("WDC", "WD")),
                             ("Toshiba", ("TOSHIBA",)), ("HGST", ("HGST",))):
        if m.startswith(prefijos):
            return nombre
    return "Otras"


def curvas(grupos, titulo, archivo, columna):
    """Ajusta Kaplan-Meier a cada grupo, dibuja las curvas y devuelve filas de resumen y ajustes."""
    filas, ajustes = [], {}
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for nombre, d in grupos:
        try:
            kmf, (a0, a1) = ajustar(d, f"{nombre} (n={len(d):,})")
        except Exception as ex:  # noqa: BLE001
            print(f"AVISO: no se pudo ajustar {nombre}: {ex}")
            continue
        kmf.plot_survival_function(ax=ax, ci_show=True)
        fila = {columna: nombre, "discos": len(d), "fallas": int(d["fallo"].sum()),
                "censurados": int((d["fallo"] == 0).sum()),
                "edad_mediana_entrada": round(float(d["entrada"].median() / escala), 2),
                f"curva_desde_{unidad}": round(a0, 2), f"curva_hasta_{unidad}": round(a1, 2),
                "fallas_en_la_curva": int(kmf.event_table["observed"].sum())}
        for t in ([1, 3, 5, 7] if tiene_edad else [7, 14]):
            # solo se informa S(t) dentro del tramo estimado; fuera de él la curva no dice nada
            fila[f"S({t} {unidad})"] = (round(float(kmf.survival_function_at_times(t).iloc[0]), 4)
                                        if a0 <= t <= a1 else None)
        fila["S(fin del tramo)"] = round(float(kmf.survival_function_.iloc[-1, 0]), 4)
        ajustes[nombre] = (kmf, a0, a1, d)
        filas.append(fila)
    ax.set_title(titulo)
    ax.set_xlabel(f"Edad del disco ({unidad})")
    ax.set_ylabel("Supervivencia S(t | llegó al inicio de su tramo)")
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(RES, archivo), dpi=150)
    plt.close(fig)
    tabla = pd.DataFrame(filas)
    print(f"\n{titulo}\n{tabla.to_string(index=False)}")
    return tabla, ajustes


# 1) por modelo: tramos cortos, porque cada modelo se compra e instala por lotes de edades parecidas
t_mod, aj_mod = curvas([(m, v[v["model"] == m]) for m in modelos],
                       "Kaplan-Meier por modelo (cada uno solo en sus edades con datos)", "km_curvas.png", "grupo")
# 2) por marca y 3) todos juntos: al mezclar modelos se cubren muchas más edades
v["marca"] = v["model"].map(marca)
marcas = [mk for mk in v["marca"].value_counts().index if mk != "Otras"]
t_mar, aj_mar = curvas([(mk, v[v["marca"] == mk]) for mk in marcas] + [("Todos los discos", v)],
                       "Kaplan-Meier por marca y para todos los discos", "km_curvas_marcas.png", "grupo")
t_mod.insert(0, "nivel", "modelo")
t_mar.insert(0, "nivel", "marca")
pd.concat([x for x in (t_mar, t_mod) if len(x)]).to_csv(os.path.join(RES, "km_resumen.csv"), index=False)

# ingenuo vs con truncamiento, con todos los discos (el tramo de edades más largo)
comparacion = {}
todos = {**aj_mod, **aj_mar}
if tiene_edad and todos:
    nombre = max(todos, key=lambda k: todos[k][2] - todos[k][1])
    k2, a0, a1, d = todos[nombre]
    k1, _ = ajustar(d, "Ingenuo", truncado=False)
    # para comparar en igualdad, la ingenua también se condiciona a haber llegado a a0
    s1_a0 = float(k1.survival_function_at_times(a0).iloc[0])
    rejilla = np.linspace(a0, a1, 300)
    s1 = k1.survival_function_at_times(rejilla).to_numpy() / s1_a0
    s2 = k2.survival_function_at_times(rejilla).to_numpy()
    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.step(rejilla, s1, where="post", color="0.6", lw=2, label="Ingenuo: todos en riesgo desde la edad 0")
    ax.step(rejilla, s2, where="post", color="black", lw=2,
            label="Con truncamiento: en riesgo desde su edad de entrada")
    ax.set_title(f"Por qué importa el truncamiento por la izquierda ({nombre})")
    ax.set_xlabel("Edad del disco (años)")
    ax.set_ylabel(f"Supervivencia S(t | llegó a {a0:.1f} años)")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(RES, "km_ingenuo_vs_truncado.png"), dpi=150)
    comparacion = {"grupo": nombre, "desde_anios": round(a0, 2), "hasta_anios": round(a1, 2),
                   "S_ingenuo": round(float(s1[-1]), 4), "S_truncado": round(float(s2[-1]), 4)}
    print(f"\nComparación en '{nombre}', de {a0:.2f} a {a1:.2f} años: probabilidad de sobrevivir el tramo "
          f"ingenuo={comparacion['S_ingenuo']}, con truncamiento={comparacion['S_truncado']}")

with open(os.path.join(RES, "km_datos.json"), "w") as f:
    json.dump({"usa_edad_smart9": bool(tiene_edad), "discos_usados": int(len(v)),
               "fallas_usadas": int(v["fallo"].sum()), "min_en_riesgo": MIN_EN_RIESGO, "min_cobertura": MIN_COBERTURA,
               "modelos": modelos, "marcas": marcas, "comparacion": comparacion},
              f, indent=2, ensure_ascii=False)
print("\nListo: resultados/km_curvas.png, km_curvas_marcas.png, km_ingenuo_vs_truncado.png, km_resumen.csv")

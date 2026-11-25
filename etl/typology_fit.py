# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==2.0.1", "pandas", "scikit-learn"]
# ///
"""Ajusta a referência da tipologia de municípios e grava `etl/typology.json`.

Roda só quando se quer refazer os grupos: a atualização semanal não reajusta nada, apenas atribui
cada município ao centro mais próximo desta referência (`car_etl.py typology`), para que os grupos
não mudem de sentido de uma semana para outra.

    uv run etl/typology_fit.py perfil.parquet      # perfil gerado por `car_etl.py typology --profile`
"""
import json
import sys
from pathlib import Path

import duckdb
import pandas as pd
from sklearn.cluster import KMeans

FEATURES = ["small_share", "large_area_share", "waiting_share", "notified_share", "analysed_share",
            "overlap_share", "coverage"]
GROUPS = {
    "small_waiting": ("Pequenos imóveis à espera de análise",
                      "Quase só imóveis pequenos, e a grande maioria ainda aguarda a análise."),
    "analysed": ("Análise avançada",
                 "Boa parte dos cadastros já foi analisada ou está em análise."),
    "large_waiting": ("Grandes imóveis à espera de análise",
                      "A maior parte da área declarada está em imóveis grandes, e a análise mal começou."),
    "notified": ("Notificados sem resposta",
                 "A análise andou, mas a maioria dos cadastros espera a resposta do declarante."),
    "overlapping": ("Muita sobreposição",
                    "Mais da metade dos imóveis tem a maior parte da área dentro de outros cadastros."),
}
K, MIN_PROPERTIES = 5, 30

profile = duckdb.sql(f"SELECT * FROM '{sys.argv[1]}'").df()
profile = profile[(profile["properties"] >= MIN_PROPERTIES) & profile["coverage"].notna()]
X = profile[FEATURES]
mean, std = X.mean(), X.std()
Z = ((X - mean) / std).clip(-3, 3)
km = KMeans(K, n_init=50, random_state=0).fit(Z)
centers = pd.DataFrame(km.cluster_centers_, columns=FEATURES)
# Cada centro recebe o nome pelo traço que o distingue.
names, left = {}, set(centers.index)
for code, feature in [("overlapping", "overlap_share"), ("notified", "notified_share"),
                      ("analysed", "analysed_share"), ("large_waiting", "large_area_share")]:
    pick = centers.loc[sorted(left), feature].idxmax()
    names[pick] = code
    left.remove(pick)
names[left.pop()] = "small_waiting"
sizes = pd.Series(km.labels_).map(names).value_counts()
reference = {
    "fitted": pd.Timestamp.now("UTC").strftime("%Y-%m-%d"),
    "min_properties": MIN_PROPERTIES,
    "features": FEATURES,
    "mean": {f: round(float(mean[f]), 6) for f in FEATURES},
    "std": {f: round(float(std[f]), 6) for f in FEATURES},
    "groups": [{"code": names[i], "label": GROUPS[names[i]][0], "description": GROUPS[names[i]][1],
                "center": {f: round(float(centers.loc[i, f]), 4) for f in FEATURES}}
               for i in sorted(names, key=lambda i: -sizes[names[i]])],
}
Path(__file__).with_name("typology.json").write_text(json.dumps(reference, indent=2, ensure_ascii=False) + "\n")
print(sizes.to_dict())
print((centers * std + mean).round(2).assign(group=pd.Series(names)).to_string())

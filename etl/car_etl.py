#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==2.0.1", "boto3==1.44.11"]
# ///
"""ETL dos imóveis do CAR: geoserviço público do Sicar -> GeoParquet -> bucket.

    uv run etl/car_etl.py download PE     # baixa as páginas brutas (retoma de onde parou)
    uv run etl/car_etl.py build PE        # gera os arquivos do estado em data/release/PE/
    uv run etl/car_etl.py publish PE      # envia o estado ao bucket e atualiza o catálogo
    uv run etl/car_etl.py all             # todos os estados que faltam, um por vez,
                                          # apagando os dados locais depois de cada envio
    uv run etl/car_etl.py all BA MG       # só esses (refaz mesmo se já publicados)
    uv run etl/car_etl.py refresh         # atualização completa: refaz todos os estados
    uv run etl/car_etl.py verify          # confere o bucket contra o catálogo e o Sicar
    uv run etl/car_etl.py summary         # (re)gera o resumo pré-calculado a partir do bucket
    uv run etl/car_etl.py web             # envia a página de consulta (web/) à raiz do bucket
    uv run etl/car_etl.py history         # inicia o histórico com a carga publicada (uma vez)
    uv run etl/car_etl.py backup          # copia o histórico do bucket para data/history-backup/
    uv run etl/car_etl.py quality PE      # indicadores de qualidade por imóvel de um estado já publicado
    uv run etl/car_etl.py typology        # tipologia dos municípios do país, a partir do bucket

Fonte: WFS público do Sicar, uma camada por UF (sicar:sicar_imoveis_<uf>).
O download é sequencial, com pausa entre as páginas, para não pesar no servidor.
A paginação é por chave (cod_imovel > último lido): o tempo por página não cresce
e inserções no meio do download não deslocam as páginas.
"""

from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

WFS = "https://geoserver.car.gov.br/geoserver/sicar/wfs"
ROOT = Path(__file__).resolve().parent.parent / "data"
PAGE = 5000
PAUSE_S = 2
USER_AGENT = "carbr-etl/0.1 (+https://github.com/castlaboratory/carbr-etl)"
# Um estado cuja contagem nova fique abaixo desta fração da publicada não é substituído.
MIN_RATIO = 0.95
# O download de um estado tem de trazer ao menos esta fração do total que o Sicar informa.
MIN_COMPLETE = 0.995
# Download parcial mais velho que isto é descartado antes de recomeçar (horas).
STALE_HOURS = 36
# Do menor para o maior (contagem de 24/11/2026).
STATES = ["AP", "DF", "RR", "AC", "RJ", "MS", "AM", "TO", "RN", "SE", "ES", "AL", "RO", "PB",
          "MT", "GO", "PI", "PA", "CE", "MA", "PE", "SC", "SP", "PR", "RS", "MG", "BA"]

# nome publicado -> (nome no Sicar, descrição)
DICTIONARY = [
    ("property_code", "cod_imovel", "Código do imóvel no CAR (UF-município IBGE-identificador)."),
    ("status_code", "status_imovel", "Situação do cadastro: AT ativo, PE pendente, SU suspenso, CA cancelado."),
    ("status", None, "Situação do cadastro por extenso, derivada de status_code."),
    ("condition", "condicao", "Etapa da análise em que o cadastro está, como informada pelo Sicar."),
    ("property_type_code", "tipo_imovel", "IRU imóvel rural, AST assentamento, PCT povos e comunidades tradicionais."),
    ("property_type", None, "Tipo do imóvel por extenso, derivado de property_type_code."),
    ("created_at", "dat_criacao", "Data e hora de criação do cadastro (UTC)."),
    ("area_ha", "area", "Área declarada do imóvel, em hectares."),
    ("fiscal_modules", "m_fiscal", "Área em módulos fiscais do município."),
    ("size_class", None, "small até 4 módulos fiscais, medium acima de 4 até 15, large acima de 15."),
    ("state", "uf", "UF onde o imóvel está localizado."),
    ("municipality", "municipio", "Município onde o imóvel está localizado."),
    ("municipality_code", "cod_municipio_ibge", "Código IBGE do município (7 dígitos)."),
    ("registration_state", None, "UF do prefixo de property_code; pode diferir de state."),
    ("lon", None, "Longitude de um ponto dentro do imóvel (SIRGAS 2000)."),
    ("lat", None, "Latitude de um ponto dentro do imóvel (SIRGAS 2000)."),
    ("geometry_valid", None, "Se a geometria declarada é válida (sem autointerseção etc.)."),
    ("xmin", None, "Limite oeste do retângulo envolvente do imóvel, para filtro espacial na leitura."),
    ("ymin", None, "Limite sul do retângulo envolvente."),
    ("xmax", None, "Limite leste do retângulo envolvente."),
    ("ymax", None, "Limite norte do retângulo envolvente."),
    ("geometry", "geo_area_imovel", "Perímetro declarado do imóvel, WKB, EPSG:4674. Nulo se o Sicar não traz."),
]


# Vai no catálogo, para quem lê os dados saber de onde vêm e em que termos.
SOURCE = {
    "name": "SFB / Sicar — Cadastro Ambiental Rural",
    "url": WFS,
    "legal_basis": "Instrução Normativa MMA nº 3, de 18/12/2014 (arts. 2º, 4º, 12 e 18)",
    "notice": (
        "Informações não sigilosas e não pessoais do Sicar, de livre acesso. Não há dado que "
        "identifique proprietários ou possuidores. Cadastro autodeclarado: há sobreposições, e a "
        "presença no CAR não comprova domínio. Destinam-se a subsidiar políticas, programas, "
        "projetos e atividades de controle, monitoramento, planejamento ambiental e econômico e "
        "combate ao desmatamento. Republicação independente do CastLab/UFPE, sem caráter oficial; "
        "em caso de divergência, vale o Sicar."
    ),
}

# Resumo pré-calculado: uma linha por combinação, para agregações nacionais sem ler os imóveis.
SUMMARY_SQL = """
    SELECT state, status, size_class, property_type, condition,
           year(created_at AT TIME ZONE 'UTC')::INTEGER AS created_year,
           count(*) AS properties, sum(area_ha) AS area_ha
    FROM {src} GROUP BY ALL ORDER BY ALL
"""


def log(msg: str) -> None:
    print(f"{dt.datetime.now():%H:%M:%S} {msg}", flush=True)


def curl(params: dict[str, str], dest: Path) -> int:
    args = ["curl", "-sS", "--fail", "--compressed", "-m", "900", "-A", USER_AGENT, "-G", WFS,
            "-o", str(dest)]
    for k, v in params.items():
        args += ["--data-urlencode", f"{k}={v}"]
    return subprocess.run(args).returncode


def fetch(params: dict[str, str], dest: Path, tries: int = 8) -> dict:
    """Baixa uma página para `dest` e devolve o JSON já conferido."""
    tmp = dest.with_suffix(".part")
    for attempt in range(1, tries + 1):
        rc = curl(params, tmp)
        if rc == 0:
            try:
                page = json.loads(tmp.read_text())
                if "features" in page:
                    tmp.rename(dest)
                    return page
            except json.JSONDecodeError:
                pass
        wait = min(60 * attempt, 600)
        log(f"  falhou (curl rc={rc}), tentativa {attempt}/{tries}; nova tentativa em {wait}s")
        time.sleep(wait)
    raise SystemExit(f"desisti de {dest.name} após {tries} tentativas")


def hits(layer: str, tmp: Path) -> int | None:
    """Total de feições da camada, como o Sicar informa agora."""
    rc = curl({"service": "WFS", "version": "2.0.0", "request": "GetFeature",
               "typeNames": layer, "resultType": "hits"}, tmp)
    m = re.search(r'numberMatched="(\d+)"', tmp.read_text()) if rc == 0 else None
    tmp.unlink(missing_ok=True)
    return int(m.group(1)) if m else None


def download(uf: str) -> None:
    raw = ROOT / "raw" / uf.lower()
    raw.mkdir(parents=True, exist_ok=True)
    manifest_path = raw / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    layer = f"sicar:sicar_imoveis_{uf.lower()}"
    base = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": layer,
        "sortBy": "cod_imovel",
        "outputFormat": "application/json",
        "count": str(PAGE),
    }
    if "started_at" not in manifest:
        manifest |= {"started_at": dt.datetime.now(dt.UTC).isoformat(),
                     "matched_at_start": hits(layer, raw / "hits.tmp"), "paging": "keyset"}
        manifest_path.write_text(json.dumps(manifest, indent=2))
    total, n, last = 0, 0, None
    while True:
        dest = raw / f"page_{n:04d}.geojson"
        if dest.exists():
            page = json.loads(dest.read_text())
        else:
            t0 = time.time()
            params = base if last is None else base | {"cql_filter": f"cod_imovel > '{last}'"}
            page = fetch(params, dest)
            log(f"{uf} {dest.name}: {len(page['features'])} feições em {time.time() - t0:.0f}s "
                f"({total + len(page['features'])} de {manifest.get('matched_at_start')})")
            time.sleep(PAUSE_S)
        got = len(page["features"])
        total += got
        n += 1
        if got < PAGE:
            break
        last = page["features"][-1]["properties"]["cod_imovel"]
    manifest |= {"finished_at": dt.datetime.now(dt.UTC).isoformat(),
                 "matched_at_end": hits(layer, raw / "hits.tmp"),
                 "pages": n, "features": total, "page_size": PAGE, "source": WFS, "layer": layer}
    manifest_path.write_text(json.dumps(manifest, indent=2))
    log(f"{uf}: {total} feições em {n} páginas")
    expected = manifest["matched_at_end"] or manifest.get("matched_at_start")
    if expected and total < MIN_COMPLETE * expected:
        raise RuntimeError(f"{uf}: download incompleto, {total} feições de {expected} informadas pelo Sicar")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build(uf: str) -> None:
    import duckdb

    raw = ROOT / "raw" / uf.lower()
    manifest = json.loads((raw / "manifest.json").read_text())
    if "features" not in manifest:
        raise SystemExit("download incompleto: rode `download` até o fim antes do `build`")
    out = ROOT / "release" / uf
    out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    select = """
        SELECT cod_imovel AS property_code, status_imovel AS status_code, condicao AS condition,
               tipo_imovel AS property_type_code, dat_criacao AS created_at, area AS area_ha,
               m_fiscal AS fiscal_modules, uf AS state, municipio AS municipality,
               cod_municipio_ibge AS municipality_code, geom AS geometry
        FROM ST_Read(?)
    """
    pages = sorted(raw.glob("page_*.geojson"))
    con.execute(f"CREATE TABLE src AS {select}", [str(pages[0])])
    for page in pages[1:]:
        con.execute(f"INSERT INTO src {select}", [str(page)])
    raw_rows, distinct_codes = con.execute(
        "SELECT count(*), count(DISTINCT property_code) FROM src"
    ).fetchone()

    con.execute(
        """
        CREATE TABLE properties AS
        WITH dedup AS (
            SELECT * FROM src
            QUALIFY row_number() OVER (PARTITION BY property_code ORDER BY created_at DESC) = 1
        ), g AS (
            SELECT *, ST_IsValid(geometry) AS geometry_valid,
                   CASE WHEN ST_IsValid(geometry) THEN ST_PointOnSurface(geometry)
                        ELSE ST_Centroid(geometry) END AS pt
            FROM dedup
        )
        SELECT
            property_code,
            status_code,
            CASE status_code WHEN 'AT' THEN 'active' WHEN 'PE' THEN 'pending'
                             WHEN 'SU' THEN 'suspended' WHEN 'CA' THEN 'cancelled' END AS status,
            condition,
            property_type_code,
            CASE property_type_code WHEN 'IRU' THEN 'rural_property' WHEN 'AST' THEN 'settlement'
                                    WHEN 'PCT' THEN 'traditional_community' END AS property_type,
            created_at,
            area_ha,
            fiscal_modules,
            CASE WHEN fiscal_modules <= 4 THEN 'small' WHEN fiscal_modules <= 15 THEN 'medium'
                 ELSE 'large' END AS size_class,
            state,
            municipality,
            municipality_code,
            split_part(property_code, '-', 1) AS registration_state,
            ST_X(pt) AS lon,
            ST_Y(pt) AS lat,
            geometry_valid,
            ST_XMin(geometry) AS xmin, ST_YMin(geometry) AS ymin,
            ST_XMax(geometry) AS xmax, ST_YMax(geometry) AS ymax,
            geometry
        FROM g
        """
    )

    # Ordem: município, depois curva de Hilbert. Grupos de linhas pequenos deixam o leitor
    # remoto pular quase tudo num filtro por município ou por bbox.
    xmin, ymin, xmax, ymax = con.execute(
        "SELECT min(xmin), min(ymin), max(xmax), max(ymax) FROM properties"
    ).fetchone()
    props_file = out / "properties.parquet"
    con.execute(
        f"""
        COPY (
            SELECT * FROM properties
            ORDER BY municipality_code,
                     ST_Hilbert(lon, lat, {{min_x: {xmin}, min_y: {ymin}, max_x: {xmax}, max_y: {ymax}}}::BOX_2D)
        ) TO '{props_file}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 4096)
        """
    )

    muni_file = out / "municipalities.parquet"
    con.execute(
        f"""
        COPY (
            SELECT state, municipality_code, municipality,
                   count(*) AS properties,
                   count(*) FILTER (size_class = 'small') AS small_properties,
                   sum(area_ha) AS area_ha,
                   count(*) FILTER (status = 'active') AS active,
                   count(*) FILTER (status = 'pending') AS pending,
                   count(*) FILTER (status = 'suspended') AS suspended,
                   count(*) FILTER (status = 'cancelled') AS cancelled,
                   count(*) FILTER (NOT geometry_valid) AS invalid_geometries,
                   min(xmin) AS xmin, min(ymin) AS ymin,
                   max(xmax) AS xmax, max(ymax) AS ymax
            FROM properties GROUP BY ALL ORDER BY municipality_code
        ) TO '{muni_file}' (FORMAT parquet, COMPRESSION zstd)
        """
    )

    summary_file = out / "summary.parquet"
    con.execute(f"COPY ({SUMMARY_SQL.format(src='properties')}) TO '{summary_file}' "
                "(FORMAT parquet, COMPRESSION zstd)")

    def count(where: str = "TRUE") -> int:
        return con.execute(f"SELECT count(*) FROM properties WHERE {where}").fetchone()[0]

    quality = {
        "raw_rows": raw_rows,
        "distinct_property_codes": distinct_codes,
        "duplicates_removed": raw_rows - count(),
        "matched_at_start": manifest.get("matched_at_start"),
        "matched_at_end": manifest.get("matched_at_end"),
        "null_geometries": count("geometry IS NULL"),
        "invalid_geometries": count("NOT geometry_valid"),
        "unmapped_status_codes": count("status IS NULL"),
        "unmapped_property_types": count("property_type IS NULL"),
        "registered_in_other_state": count("registration_state <> state"),
    }
    state = {
        "properties": count(),
        "municipalities": con.execute(
            "SELECT count(DISTINCT municipality_code) FROM properties").fetchone()[0],
        "layer": manifest["layer"],
        "downloaded_from": manifest.get("started_at"),
        "downloaded_to": manifest.get("finished_at"),
        "quality": quality,
        "files": [
            {"path": key, "bytes": p.stat().st_size, "sha256": sha256(p)}
            for key, p in ((f"properties/state={uf}/part-0.parquet", props_file),
                           (f"municipalities/state={uf}.parquet", muni_file),
                           (f"summary/state={uf}.parquet", summary_file))
        ],
    }
    (out / "state.json").write_text(json.dumps(state, indent=2, ensure_ascii=False))
    log(f"{uf}: {state['properties']} imóveis, {props_file.stat().st_size / 1e6:.0f} MB em {out}")


def bucket():
    """Cliente S3 e nome do bucket, das credenciais em `.env` na raiz do projeto."""
    import boto3
    from urllib.parse import urlparse

    env = {}
    for line in (ROOT.parent / ".env").read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip("'\"")
    endpoint = urlparse(env["R2_S3API"])
    s3 = boto3.client(
        "s3", endpoint_url=f"{endpoint.scheme}://{endpoint.hostname}", region_name="auto",
        aws_access_key_id=env["ACESS_KEY_ID"], aws_secret_access_key=env["SECRET_ACESS_KEY"],
    )
    return s3, env.get("R2_BUCKET", "car")


def remote_catalog(s3, name: str) -> dict:
    try:
        return json.loads(s3.get_object(Bucket=name, Key="current/catalog.json")["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {"states": {}, "files": []}


def publish(uf: str) -> None:
    """Envia um estado ao bucket, sob `current/`, e acrescenta-o ao catálogo.

    O catálogo vai por último: quem lê nunca vê um catálogo que aponta para arquivo
    ainda não enviado. Cada envio é conferido pelo tamanho antes de seguir.
    """
    s3, name = bucket()
    out = ROOT / "release" / uf
    state = json.loads((out / "state.json").read_text())
    previous = remote_catalog(s3, name)["states"].get(uf, {}).get("properties")
    if previous and state["properties"] < MIN_RATIO * previous:
        raise RuntimeError(
            f"{uf}: {state['properties']} imóveis agora, {previous} publicados; "
            "queda acima do limite, estado não substituído"
        )
    # Histórico: antes de substituir o estado, compara o que está publicado com o que chegou.
    date = state["downloaded_to"][:10]
    published = remote_catalog(s3, name)["states"].get(uf, {}).get("downloaded_to", "")[:10]
    history = {f"history/summary/date={date}/state={uf}.parquet": out / "summary.parquet"}
    if published and published != date:
        write_transitions(name, uf, published, date, out / "properties.parquet", out / "transitions.parquet")
        history[f"history/transitions/date={date}/state={uf}.parquet"] = out / "transitions.parquet"
    local = {f"properties/state={uf}/part-0.parquet": out / "properties.parquet",
             f"municipalities/state={uf}.parquet": out / "municipalities.parquet",
             f"summary/state={uf}.parquet": out / "summary.parquet"}
    for key, path in history.items():
        s3.upload_file(str(path), name, key, ExtraArgs={"ContentType": "application/vnd.apache.parquet"})
        log(f"enviado {key}")
    for key, path in local.items():
        s3.upload_file(str(path), name, f"current/{key}",
                       ExtraArgs={"ContentType": "application/vnd.apache.parquet"})
        size = s3.head_object(Bucket=name, Key=f"current/{key}")["ContentLength"]
        if size != path.stat().st_size:
            raise SystemExit(f"{key}: {size} bytes no bucket, {path.stat().st_size} aqui")
        log(f"enviado current/{key} ({size / 1e6:.1f} MB)")
    s3.put_object(Bucket=name, Key="current/dictionary.json", ContentType="application/json",
                  Body=json.dumps([{"column": c, "sicar_name": n, "description": d}
                                   for c, n, d in DICTIONARY], indent=2, ensure_ascii=False).encode())
    cat = remote_catalog(s3, name)
    files = state.pop("files")
    cat["states"][uf] = state
    cat["states"] = {k: cat["states"][k] for k in sorted(cat["states"])}
    cat["files"] = sorted([f for f in cat["files"] if f["path"] not in local] + files,
                          key=lambda f: f["path"])
    cat["version"] = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d")
    cat["source"] = SOURCE
    cat["crs"] = "EPSG:4674"
    dates = cat.setdefault("history", {}).setdefault(uf, [])
    if date not in dates:
        dates.append(date)
        dates.sort()
    s3.put_object(Bucket=name, Key="current/catalog.json", ContentType="application/json",
                  Body=json.dumps(cat, indent=2, ensure_ascii=False).encode())
    log(f"{uf} no catálogo; {len(cat['states'])} estados publicados, versão {cat['version']}")


def env_file() -> dict:
    env = {}
    for line in (ROOT.parent / ".env").read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip("'\"")
    return env


def remote_duck(name: str):
    """DuckDB que lê o bucket pela API S3 (sem passar pelo endereço público)."""
    import duckdb
    from urllib.parse import urlparse

    env = env_file()
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"CREATE SECRET (TYPE s3, KEY_ID '{env['ACESS_KEY_ID']}', SECRET '{env['SECRET_ACESS_KEY']}', "
        f"ENDPOINT '{urlparse(env['R2_S3API']).hostname}', REGION 'auto', URL_STYLE 'path')"
    )
    return con, lambda key: f"s3://{name}/current/{key}"


def summary(ufs: list[str]) -> None:
    """(Re)gera o resumo de estados já publicados, lendo os imóveis do bucket."""
    s3, name = bucket()
    cat = remote_catalog(s3, name)
    con, url = remote_duck(name)
    tmp = ROOT / "release" / "_summary.parquet"
    tmp.parent.mkdir(parents=True, exist_ok=True)
    for uf in ufs or list(cat["states"]):
        key = f"summary/state={uf}.parquet"
        src = f"read_parquet('{url(f'properties/state={uf}/part-0.parquet')}')"
        con.execute(f"COPY ({SUMMARY_SQL.format(src=src)}) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)")
        rows, props = con.execute(f"SELECT count(*), sum(properties) FROM '{tmp}'").fetchone()
        if props != cat["states"][uf]["properties"]:
            raise SystemExit(f"{uf}: resumo soma {props}, catálogo diz {cat['states'][uf]['properties']}")
        s3.upload_file(str(tmp), name, f"current/{key}",
                       ExtraArgs={"ContentType": "application/vnd.apache.parquet"})
        cat["files"] = [f for f in cat["files"] if f["path"] != key] + [
            {"path": key, "bytes": tmp.stat().st_size, "sha256": sha256(tmp)}]
        log(f"{uf}: resumo com {rows} linhas, {props} imóveis")
    tmp.unlink(missing_ok=True)
    cat["files"].sort(key=lambda f: f["path"])
    cat["version"] = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d")
    s3.put_object(Bucket=name, Key="current/catalog.json", ContentType="application/json",
                  Body=json.dumps(cat, indent=2, ensure_ascii=False).encode())
    log("catálogo atualizado")


def verify(_ufs: list[str]) -> None:
    """Confere cada estado: linhas no arquivo do bucket, catálogo e total atual do Sicar."""
    s3, name = bucket()
    cat = remote_catalog(s3, name)
    con, url = remote_duck(name)
    missing = [uf for uf in STATES if uf not in cat["states"]]
    print(f"estados no catálogo: {len(cat['states'])} de {len(STATES)}; faltando: {missing}")
    print("UF   no_arquivo  códigos_distintos  catálogo  sicar_agora  diferença")
    total = [0, 0]
    for uf in sorted(cat["states"]):
        f = url(f"properties/state={uf}/part-0.parquet")
        rows, codes, states = con.execute(
            f"SELECT count(*), count(DISTINCT property_code), count(DISTINCT state) FROM '{f}'"
        ).fetchone()
        now = hits(f"sicar:sicar_imoveis_{uf.lower()}", ROOT / "hits.tmp")
        declared = cat["states"][uf]["properties"]
        flag = "" if rows == codes == declared and states == 1 else "  <-- conferir"
        print(f"{uf}  {rows:>10}  {codes:>10}  {declared:>10}  {now!s:>10}  {(now or 0) - rows:>+6}{flag}")
        total[0] += rows
        total[1] += now or 0
    print(f"total no bucket {total[0]}; no Sicar agora {total[1]}")


def web(_ufs: list[str]) -> None:
    """Envia a página de consulta (`web/`) à raiz do bucket."""
    s3, name = bucket()
    root = ROOT.parent / "web"
    types = {".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8",
             ".css": "text/css; charset=utf-8"}
    for path in sorted(p for p in root.rglob("*") if p.suffix in types):
        key = str(path.relative_to(root))
        s3.upload_file(str(path), name, key, ExtraArgs={
            "ContentType": types[path.suffix],
            "CacheControl": "public, max-age=300" if path.suffix == ".html" else "public, max-age=86400"})
        log(f"enviado {key} ({path.stat().st_size / 1e3:.0f} kB)")


def write_transitions(name: str, uf: str, from_date: str, to_date: str, new: Path, dest: Path) -> None:
    """Fluxo entre duas cargas: quantos imóveis saíram de cada situação e etapa para cada outra.

    O Sicar não informa quando um cadastro muda de etapa; a diferença entre a carga publicada
    e a nova é o único registro disso. `from_*` nulo é imóvel que entrou; `to_*` nulo, que saiu.
    """
    con, url = remote_duck(name)
    old = url(f"properties/state={uf}/part-0.parquet")
    columns = "property_code, status, condition, size_class, area_ha"
    con.execute(f"""
        COPY (
            SELECT '{uf}' AS state, DATE '{from_date}' AS from_date, DATE '{to_date}' AS to_date,
                   o.status AS from_status, n.status AS to_status,
                   o.condition AS from_condition, n.condition AS to_condition,
                   coalesce(n.size_class, o.size_class) AS size_class,
                   count(*) AS properties, sum(coalesce(n.area_ha, o.area_ha)) AS area_ha
            FROM (SELECT {columns} FROM read_parquet('{old}')) o
            FULL OUTER JOIN (SELECT {columns} FROM read_parquet('{new}')) n USING (property_code)
            GROUP BY ALL ORDER BY ALL
        ) TO '{dest}' (FORMAT parquet, COMPRESSION zstd)
    """)
    moved, entered, left = con.execute(f"""
        SELECT coalesce(sum(properties) FILTER (from_condition IS NOT NULL AND to_condition IS NOT NULL
                                               AND from_condition <> to_condition), 0),
               coalesce(sum(properties) FILTER (from_condition IS NULL), 0),
               coalesce(sum(properties) FILTER (to_condition IS NULL), 0)
        FROM '{dest}'
    """).fetchone()
    log(f"{uf}: de {from_date} a {to_date}, {int(moved)} mudaram de etapa, {int(entered)} entraram, {int(left)} saíram")


def history_init(ufs: list[str]) -> None:
    """Marca a carga publicada como primeiro ponto do histórico dos estados que ainda não têm um."""
    s3, name = bucket()
    cat = remote_catalog(s3, name)
    for uf in ufs or list(cat["states"]):
        dates = cat.setdefault("history", {}).setdefault(uf, [])
        date = cat["states"][uf].get("downloaded_to", "")[:10]
        if dates or not date:
            continue
        key = f"history/summary/date={date}/state={uf}.parquet"
        s3.copy_object(Bucket=name, Key=key, ContentType="application/vnd.apache.parquet", MetadataDirective="REPLACE",
                       CopySource={"Bucket": name, "Key": f"current/summary/state={uf}.parquet"})
        dates.append(date)
        log(f"{uf}: histórico iniciado em {date}")
    s3.put_object(Bucket=name, Key="current/catalog.json", ContentType="application/json",
                  Body=json.dumps(cat, indent=2, ensure_ascii=False).encode())


IBGE_MESH = ("https://servicodados.ibge.gov.br/api/v3/malhas/estados/{uf}"
             "?intrarregiao=municipio&formato=application/vnd.geo%2Bjson&qualidade=maxima")


def mesh(uf: str) -> Path | None:
    """Malha municipal do IBGE do estado, guardada em disco por 30 dias. `None` se a API falhar."""
    path = ROOT / "mesh" / f"{uf}.geojson"
    fresh = path.exists() and time.time() - path.stat().st_mtime < 30 * 86400
    if not fresh:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".part")
        rc = subprocess.run(["curl", "-sS", "--fail", "--compressed", "-m", "300", "-A", USER_AGENT,
                             "-o", str(tmp), IBGE_MESH.format(uf=uf)]).returncode
        try:
            ok = rc == 0 and len(json.loads(tmp.read_text())["features"]) > 0
        except Exception:
            ok = False
        if ok:
            tmp.rename(path)
        else:
            tmp.unlink(missing_ok=True)
            log(f"{uf}: malha do IBGE indisponível")
    return path if path.exists() else None


FUNAI_WFS = ("https://geoserver.funai.gov.br/geoserver/Funai/ows?service=WFS&version=2.0.0&request=GetFeature"
             "&typeName=Funai:tis_poligonais&count=10000&sortBy=gid&outputFormat=application/json")
# O CNUC corrente sai do mesmo pedido que o botão de download do mapa (https://cnuc.mma.gov.br/map)
# faz ao escolher "Shapefile" sem selecionar unidades. Os campos são fixos: são os que a página envia.
CNUC_LIVE = "https://cnuc-backend.mma.gov.br/api/v1/downloadGeo"
CNUC_LIVE_BODY = {"format": "shp", "map": "/var/www/storage/app/mapfiles/ucs.map", "name": "ucs",
                  "typename": "ucs_selected", "ucIds": "null"}
# Florestas públicas: Cadastro Nacional de Florestas Públicas (CNFP), do Serviço Florestal Brasileiro, no portal
# de mapas do SFB. O identificador é o da edição 2025/2 (13/11/2026); a cada edição nova o SFB publica outro
# item, e este endereço precisa ser trocado (a página do cadastro aponta para o item vigente).
CNFP = "https://mapas.florestal.gov.br/portal/sharing/rest/content/items/0bbb2f934edc4336ba185f1b82b83319/data"
CNFP_EDITION = "CNFP 2025/2 (13/11/2026)"
# Reserva: o último shapefile semestral hospedado no portal de dados do MMA.
CNUC_DATASET = "https://dados.mma.gov.br/api/3/action/package_show?id=unidadesdeconservacao"
BROWSER_AGENT = "Mozilla/5.0 (compatible; carbr-etl; +https://github.com/castlaboratory/carbr-etl)"


# O servidor da Funai não envia o certificado intermediário (Sectigo OV R36). O macOS o busca
# sozinho; no Linux a verificação falha. Em vez de desligá-la, o intermediário vai junto das raízes
# do sistema. Ele vale até 2036; se a Funai trocar de autoridade, basta trocar o arquivo.
EXTRA_CA = Path(__file__).resolve().parent / "certs" / "sectigo-ov-r36.pem"
SYSTEM_CA = ["/etc/ssl/certs/ca-certificates.crt", "/etc/pki/tls/certs/ca-bundle.crt", "/etc/ssl/cert.pem"]


def ca_bundle() -> list[str]:
    """Argumentos do curl para confiar nas raízes do sistema mais o intermediário guardado aqui."""
    system = next((Path(p) for p in SYSTEM_CA if Path(p).exists()), None)
    if system is None or not EXTRA_CA.exists():
        return []
    bundle = ROOT / "protected" / "ca-bundle.pem"
    bundle.parent.mkdir(parents=True, exist_ok=True)
    bundle.write_text(system.read_text() + "\n" + EXTRA_CA.read_text())
    return ["--cacert", str(bundle)]


def get(url: str, dest: Path, timeout: int = 900, extra_ca: bool = False) -> bool:
    """Baixa `url` para `dest` (por um arquivo temporário). Devolve se deu certo."""
    tmp = dest.with_name(dest.name + ".part")
    rc = subprocess.run(["curl", "-sS", "--fail", "--compressed", "-L", "-m", str(timeout), "-A", BROWSER_AGENT,
                         *(ca_bundle() if extra_ca else []), "-o", str(tmp), url]).returncode
    if rc != 0 or not tmp.exists() or tmp.stat().st_size == 0:
        tmp.unlink(missing_ok=True)
        return False
    tmp.rename(dest)
    return True


def unpack_shapefile(archive: Path, folder: Path) -> bool:
    """Extrai um zip com shapefile para `folder`, trocando o que havia lá. `False` se não servir."""
    import zipfile

    try:
        with zipfile.ZipFile(archive) as z:
            members = [m for m in z.infolist() if not m.is_dir()]
            if not any(m.filename.lower().endswith(".shp") for m in members):
                return False
            folder.mkdir(parents=True, exist_ok=True)
            for old in folder.iterdir():
                old.unlink()
            for member in members:                      # só o nome do arquivo, nunca o caminho do zip
                (folder / Path(member.filename).name).write_bytes(z.read(member))
    except (zipfile.BadZipFile, OSError):
        return False
    return True


def cnuc_live(folder: Path) -> bool:
    """Baixa o CNUC corrente pelo download do mapa. `False` se o servidor não entregar um shapefile."""
    archive = folder / "cnuc-live.zip"
    tmp = archive.with_name(archive.name + ".part")
    rc = subprocess.run([
        "curl", "-sS", "--fail", "-m", "1200", "-A", BROWSER_AGENT, "-X", "POST", CNUC_LIVE,
        "-H", "Content-Type: application/json", "-H", "Origin: https://cnuc.mma.gov.br",
        "-H", "Referer: https://cnuc.mma.gov.br/map", "-d", json.dumps(CNUC_LIVE_BODY), "-o", str(tmp),
    ]).returncode
    ok = rc == 0 and tmp.exists() and tmp.stat().st_size > 1_000_000 and unpack_shapefile(tmp, folder / "cnuc")
    tmp.unlink(missing_ok=True)
    return ok


def protected() -> Path | None:
    """Terras indígenas (Funai) e unidades de conservação (CNUC/MMA) num Parquet local, por 30 dias.

    Uma linha por área: `kind` (indigenous, strict = proteção integral, sustainable = uso
    sustentável, public_forest = floresta pública não destinada, "tipo B"), `name`, `category` e a
    geometria em SIRGAS 2000. As florestas tipo B vêm do Cadastro Nacional de Florestas Públicas (SFB). O CNUC vem do download do mapa
    (dados correntes) ou, se ele falhar, do último shapefile do portal de dados do MMA. Se uma
    fonte falhar, vale a cópia anterior; `None` se não houver nenhuma. Com `CAR_PROTECTED_OFFLINE=1`
    nada é baixado: o Parquet é refeito com os arquivos que já estão em `data/protected/`.
    """
    import duckdb

    folder = ROOT / "protected"
    out, meta = folder / "areas.parquet", folder / "areas.json"
    if out.exists() and time.time() - out.stat().st_mtime < 30 * 86400:
        return out
    folder.mkdir(parents=True, exist_ok=True)
    funai, listing, archive = folder / "tis.geojson", folder / "cnuc.json", folder / "cnuc.zip"
    info = json.loads(meta.read_text()) if meta.exists() else {}
    today = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d")
    offline = os.environ.get("CAR_PROTECTED_OFFLINE") == "1"
    cnuc = "MMA, Cadastro Nacional de Unidades de Conservação (CNUC)"
    try:
        if not offline:
            if not get(FUNAI_WFS, funai, extra_ca=True) or not json.loads(funai.read_text())["features"]:
                raise ValueError("resposta vazia")
            info["indigenous"] = {"source": "Funai, Terras Indígenas (tis_poligonais)", "downloaded": today}
    except Exception as err:
        log(f"áreas protegidas: Funai indisponível ({err})")
    try:
        if offline:
            pass
        elif cnuc_live(folder):
            info["conservation"] = {"source": cnuc, "edition": f"download do mapa em {today}",
                                    "url": "https://cnuc.mma.gov.br/map", "downloaded": today}
        else:
            log("áreas protegidas: download do mapa do CNUC falhou; usando o portal de dados")
            if not get(CNUC_DATASET, listing, 120):
                raise ValueError("portal de dados do MMA indisponível")
            shapes = [r for r in json.loads(listing.read_text())["result"]["resources"]
                      if "shp" in (r.get("format") or "").lower() and "dados.mma.gov.br" in r["url"]]
            latest = max(shapes, key=lambda r: r.get("last_modified") or "")
            # Uma cópia do mapa mais nova que a edição semestral não é trocada por ela.
            have = info.get("conservation", {})
            newer = have.get("url") == "https://cnuc.mma.gov.br/map" and list(folder.glob("cnuc/*.shp"))
            if not newer and (have.get("url") != latest["url"] or not list(folder.glob("cnuc/*.shp"))):
                if not get(latest["url"], archive) or not unpack_shapefile(archive, folder / "cnuc"):
                    raise ValueError("download do shapefile falhou")
                archive.unlink(missing_ok=True)
                info["conservation"] = {"source": cnuc, "edition": latest["name"], "url": latest["url"],
                                        "downloaded": today}
    except Exception as err:
        log(f"áreas protegidas: CNUC indisponível ({err})")
    try:
        if not offline and (info.get("public_forest", {}).get("edition") != CNFP_EDITION
                            or not list(folder.glob("cnfp/*.shp"))):
            if not get(CNFP, archive, 3600) or not unpack_shapefile(archive, folder / "cnfp"):
                raise ValueError("download do shapefile falhou")
            archive.unlink(missing_ok=True)
            info["public_forest"] = {"source": "SFB, Cadastro Nacional de Florestas Públicas (CNFP)",
                                     "edition": CNFP_EDITION, "url": CNFP, "downloaded": today}
    except Exception as err:
        log(f"áreas protegidas: CNFP indisponível ({err})")
    parts = []
    forests = sorted(folder.glob("cnfp/*.shp"))
    if forests:
        parts.append(f"""SELECT 'public_forest' AS kind, categoria || ' ' || lower(governo) AS name, classe AS category, geom
                         FROM ST_Read('{forests[-1]}') WHERE tipo = 'TIPO B'""")
    if funai.exists():
        parts.append(f"""SELECT 'indigenous' AS kind, terrai_nome AS name, fase_ti AS category, geom
                         FROM ST_Read('{funai}')""")
    shapes = sorted(p for p in folder.glob("cnuc/*.shp") if "pontos" not in p.name)
    if shapes:
        parts.append(f"""SELECT CASE WHEN grupo ILIKE '%integral%' THEN 'strict' ELSE 'sustainable' END AS kind,
                                nome_uc AS name, categoria AS category, geom
                         FROM ST_Read('{shapes[-1]}') WHERE coalesce(situacao, 'Ativo') = 'Ativo'""")
    if not parts:
        return out if out.exists() else None
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET enable_progress_bar = false;")
    con.execute(f"""
        CREATE TABLE area AS
        SELECT row_number() OVER () AS aid, kind, name, category, ST_MakeValid(geom)::GEOMETRY AS g
        FROM ({' UNION ALL '.join(parts)}) WHERE geom IS NOT NULL
    """)
    # Há áreas com centenas de milhares de vértices, e cruzar cada imóvel com o polígono inteiro
    # não termina. As grandes são recortadas numa grade: cada cruzamento vê só um pedaço pequeno.
    # Várias linhas passam a ter o mesmo `aid`.
    cell = 0.2
    con.execute(f"""
        CREATE TABLE piece AS
        WITH big AS (SELECT *, ST_XMin(g) AS x0, ST_XMax(g) AS x1, ST_YMin(g) AS y0, ST_YMax(g) AS y1
                     FROM area WHERE ST_NPoints(g) > 2000),
             cols AS (SELECT *, unnest(generate_series(floor(x0 / {cell})::INT, floor(x1 / {cell})::INT)) AS i FROM big),
             cells AS (SELECT *, unnest(generate_series(floor(y0 / {cell})::INT, floor(y1 / {cell})::INT)) AS j FROM cols),
             cut AS (SELECT aid, kind, name, category,
                            ST_Intersection(g, ST_MakeEnvelope(i * {cell}, j * {cell}, (i + 1) * {cell}, (j + 1) * {cell})) AS g
                     FROM cells)
        SELECT * FROM cut WHERE NOT ST_IsEmpty(g) AND ST_Area(g) > 0
        UNION ALL SELECT aid, kind, name, category, g FROM area WHERE ST_NPoints(g) <= 2000
    """)
    con.execute(f"COPY (SELECT * FROM piece ORDER BY aid) TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
    meta.write_text(json.dumps(info, indent=2, ensure_ascii=False))
    counts = dict(con.execute("SELECT kind, count(*) FROM area GROUP BY 1").fetchall())
    pieces, biggest = con.execute(f"SELECT count(*), max(ST_NPoints(g)) FROM '{out}'").fetchone()
    log(f"áreas protegidas: {counts}, em {pieces} pedaços de até {biggest} vértices")
    return out


# Parcelas do Sigef (Incra): GeoPortal oficial, que substituiu o Acervo Fundiário em 2026.
SIGEF_WFS = "https://geoportal.incra.gov.br/geoserver/wfs"
# Só estes campos são pedidos ao servidor. A camada traz também o responsável técnico, o nome da área e a
# matrícula, que não são lidos.
SIGEF_FIELDS = "parcela_codigo,status,natureza,area_hectares,data_aprovacao,geometry"
IBGE_UF = {"RO": 11, "AC": 12, "AM": 13, "RR": 14, "PA": 15, "AP": 16, "TO": 17, "MA": 21, "PI": 22, "CE": 23,
           "RN": 24, "PB": 25, "PE": 26, "AL": 27, "SE": 28, "BA": 29, "MG": 31, "ES": 32, "RJ": 33, "SP": 35,
           "PR": 41, "SC": 42, "RS": 43, "MS": 50, "MT": 51, "GO": 52, "DF": 53}


def sigef(uf: str) -> Path | None:
    """Parcelas georreferenciadas do Sigef no estado, num Parquet local refeito a cada 30 dias.

    Uma linha por parcela: `id`, `status` (CERTIFICADA, REGISTRADA, TITULACAO…), `kind` (a natureza:
    Particular, Assentamento, Gleba Pública, Território Quilombola…), `area_ha`, `approved` e a
    geometria. Páginas de 5 mil, sequenciais, por chave (`parcela_codigo > '<último>'`), com pausa.
    Se o download falhar no meio, nada é trocado e vale a cópia anterior; `None` se não houver.
    """
    import duckdb

    folder = ROOT / "protected" / "sigef"
    out = folder / f"{uf}.parquet"
    if out.exists() and time.time() - out.stat().st_mtime < 30 * 86400:
        return out
    if os.environ.get("CAR_PROTECTED_OFFLINE") == "1":
        return out if out.exists() else None
    pages = folder / f"{uf}.pages"
    shutil.rmtree(pages, ignore_errors=True)
    pages.mkdir(parents=True, exist_ok=True)
    last, n, total = "", 0, 0
    try:
        while True:
            page = pages / f"{n:05d}.geojson"
            where = f"uf_id={IBGE_UF[uf]}" + (f" AND parcela_codigo > '{last}'" if last else "")
            features = None
            for attempt in range(6):         # o servidor do Incra às vezes derruba a conexão: insistir
                rc = subprocess.run([
                    "curl", "-sS", "--fail", "--compressed", "-m", "900", "-A", BROWSER_AGENT, "-G", SIGEF_WFS,
                    "--data", "service=WFS&version=2.0.0&request=GetFeature&typeNames=geonode:sigef_geo"
                              f"&outputFormat=application/json&propertyName={SIGEF_FIELDS}&sortBy=parcela_codigo&count={PAGE}",
                    "--data-urlencode", f"CQL_FILTER={where}", "-o", str(page)],
                    stderr=subprocess.DEVNULL if attempt < 5 else None).returncode
                try:
                    features = json.loads(page.read_text())["features"] if rc == 0 else None
                except (ValueError, KeyError, OSError):
                    features = None
                if features is not None:
                    break
                time.sleep(15 * (attempt + 1))
            if features is None:
                raise ValueError(f"página {n} falhou (curl {rc})")
            if not features:
                page.unlink()
                break
            last = features[-1]["properties"]["parcela_codigo"]
            total += len(features)
            n += 1
            if len(features) < PAGE:
                break
            time.sleep(PAUSE_S)
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial; SET enable_progress_bar = false;")
        tmp = out.with_suffix(".part")
        if n == 0:
            con.execute(f"""COPY (SELECT NULL::VARCHAR AS id, NULL::VARCHAR AS status, NULL::VARCHAR AS kind, NULL::DOUBLE AS area_ha,
                                          NULL::DATE AS approved, NULL::GEOMETRY AS g WHERE false) TO '{tmp}' (FORMAT parquet)""")
        else:
            files = ", ".join(f"ST_Read('{f}')" for f in sorted(pages.glob("*.geojson")))
            union = " UNION ALL ".join(f"SELECT parcela_codigo, status, natureza, area_hectares, data_aprovacao, geom FROM {f}"
                                       for f in files.split(", "))
            con.execute(f"""
                COPY (SELECT parcela_codigo AS id, status, natureza AS kind, area_hectares AS area_ha,
                             try_cast(data_aprovacao AS TIMESTAMP)::DATE AS approved, ST_MakeValid(geom)::GEOMETRY AS g
                      FROM ({union}) WHERE geom IS NOT NULL)
                TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)
            """)
        tmp.rename(out)
        log(f"{uf}: Sigef com {total} parcelas em {n} páginas")
    except Exception as err:
        log(f"{uf}: Sigef indisponível ({err})")
    finally:
        shutil.rmtree(pages, ignore_errors=True)
    return out if out.exists() else None


IBAMA_EMBARGOES = ("https://stibamadadosabertosprd.blob.core.windows.net/dados-abertos/dados/TERMOS_DE_EMBARGO/"
                   "TERMO_EMBARGO/termo_de_embargo.csv")


def embargoes() -> Path | None:
    """Embargos ambientais ativos do Ibama num Parquet local, refeito a cada 6 dias.

    Fonte: dados abertos do Ibama, "Fiscalização - termo de embargo" (atualização diária). Ficam só os
    termos não cancelados e sem desembargo, com o polígono da área embargada ou, na falta dele, o
    ponto de referência. **O nome e o CPF/CNPJ do autuado, que a tabela traz, não são lidos**, e o
    arquivo original é apagado depois da leitura. Se o download falhar, vale a cópia anterior; `None`
    se não houver nenhuma.
    """
    import duckdb

    folder = ROOT / "protected"
    out = folder / "embargos.parquet"
    if out.exists() and time.time() - out.stat().st_mtime < 6 * 86400:
        return out
    if os.environ.get("CAR_PROTECTED_OFFLINE") == "1":
        return out if out.exists() else None
    folder.mkdir(parents=True, exist_ok=True)
    raw = folder / "embargos.csv"
    try:
        local = os.environ.get("CAR_EMBARGOES_CSV")      # um CSV já baixado, para não repetir o download
        if local:
            shutil.copyfile(local, raw)
        elif not get(IBAMA_EMBARGOES, raw, 3600):
            raise ValueError("download falhou")
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial; SET enable_progress_bar = false;")
        con.execute(f"""
            CREATE TABLE e AS
            SELECT SEQ_TAD::BIGINT AS id, NUM_TAD AS number, try_cast(DAT_EMBARGO AS TIMESTAMP)::DATE AS issued,
                   UF AS state, TIPO_AREA AS kind, GEOM_AREA_EMBARGADA AS wkt,
                   try_cast(replace(NUM_LONGITUDE_TAD, ',', '.') AS DOUBLE) AS lon,
                   try_cast(replace(NUM_LATITUDE_TAD, ',', '.') AS DOUBLE) AS lat
            FROM read_csv('{raw}', delim = ';', header = true, all_varchar = true, quote = '"', escape = '"',
                          ignore_errors = true, max_line_size = 20000000)
            WHERE coalesce(SIT_CANCELADO, 'N') <> 'S' AND coalesce(SIT_DESEMBARGO, 'N') <> 'S'
        """)
        con.execute(f"""
            COPY (
                WITH shaped AS (SELECT *, CASE WHEN length(wkt) > 10 THEN try(ST_MakeValid(ST_GeomFromText(wkt))) END AS poly FROM e)
                SELECT id, number, issued, state, kind,
                       CASE WHEN poly IS NOT NULL AND NOT ST_IsEmpty(poly) THEN poly
                            WHEN lon BETWEEN -75 AND -28 AND lat BETWEEN -35 AND 6 THEN ST_Point(lon, lat) END AS g,
                       (poly IS NOT NULL AND NOT ST_IsEmpty(poly)) AS has_area
                FROM shaped
                WHERE (poly IS NOT NULL AND NOT ST_IsEmpty(poly)) OR (lon BETWEEN -75 AND -28 AND lat BETWEEN -35 AND 6)
            ) TO '{out}' (FORMAT parquet, COMPRESSION zstd)
        """)
        total, areas = con.execute(f"SELECT count(*), count(*) FILTER (has_area) FROM '{out}'").fetchone()
        (folder / "embargos.json").write_text(json.dumps({
            "source": "Ibama, dados abertos: Fiscalização - termo de embargo", "url": IBAMA_EMBARGOES,
            "downloaded": dt.datetime.now(dt.UTC).strftime("%Y-%m-%d"), "active": total, "with_area": areas,
        }, indent=2, ensure_ascii=False))
        log(f"embargos do Ibama: {total} ativos, {areas} com polígono")
    except Exception as err:
        log(f"embargos do Ibama indisponíveis ({err})")
    finally:
        raw.unlink(missing_ok=True)            # o original tem dado pessoal: não fica em disco
        raw.with_name(raw.name + ".part").unlink(missing_ok=True)
    return out if out.exists() else None


def quality(uf: str) -> None:
    """Indicadores de qualidade por imóvel: sobreposição com vizinhos, área fora do município e
    sobreposição com terras indígenas e unidades de conservação.

    Lê os imóveis do que foi gerado localmente ou, se não houver, do bucket. Publica
    `quality/state=<UF>.parquet` (uma linha por imóvel não cancelado) e
    `quality-municipalities/state=<UF>.parquet` (o resumo por município). As parcelas são razões
    entre áreas no mesmo lugar, calculadas direto em graus; `overlap_share` soma as interseções
    com cada vizinho e é limitada a 1.
    """
    import duckdb

    s3, name = bucket()
    out = ROOT / "release" / uf
    out.mkdir(parents=True, exist_ok=True)
    local = out / "properties.parquet"
    if local.exists():
        con = duckdb.connect()
        source = f"read_parquet('{local}')"
    else:
        con, url = remote_duck(name)
        source = f"read_parquet('{url(f'properties/state={uf}/part-0.parquet')}')"
    con.execute("INSTALL spatial; LOAD spatial; SET enable_progress_bar = false;")
    t0 = time.time()
    con.execute(f"""
        CREATE TABLE p AS
        SELECT row_number() OVER () AS rid, property_code, municipality_code, area_ha,
               CASE WHEN geometry_valid THEN geometry ELSE ST_MakeValid(geometry) END AS g
        FROM {source} WHERE status <> 'cancelled' AND geometry IS NOT NULL
    """)
    # Junção espacial só com ST_Intersects (é o que aciona o índice); o par ordenado vem depois.
    con.execute("CREATE TABLE cand AS SELECT a.rid AS ar, b.rid AS br FROM p a JOIN p b ON ST_Intersects(a.g, b.g)")
    con.execute("""
        CREATE TABLE pairs AS
        SELECT c.ar, c.br, ST_Area(ST_Intersection(a.g, b.g)) AS inter
        FROM cand c JOIN p a ON a.rid = c.ar JOIN p b ON b.rid = c.br WHERE c.ar < c.br
    """)
    con.execute("""
        CREATE TABLE overlap AS
        WITH sides AS (SELECT ar AS rid, inter FROM pairs WHERE inter > 0
                       UNION ALL SELECT br, inter FROM pairs WHERE inter > 0)
        SELECT rid, count(*) AS overlap_count, sum(inter) AS inter FROM sides GROUP BY rid
    """)
    boundaries = mesh(uf)
    if boundaries:
        con.execute(f"""
            CREATE TABLE m AS
            SELECT codarea::INTEGER AS municipality_code, ST_MakeValid(geom) AS mg FROM ST_Read('{boundaries}')
        """)
        outside = """CASE WHEN m.mg IS NULL THEN NULL
                          ELSE greatest(0.0, 1.0 - ST_Area(ST_Intersection(p.g::GEOMETRY, m.mg::GEOMETRY))
                                                   / nullif(ST_Area(p.g), 0)) END"""
        join = "LEFT JOIN m USING (municipality_code)"
    else:
        outside, join = "NULL::DOUBLE", ""
    # Terras indígenas e unidades de conservação: a parcela do imóvel dentro de cada tipo (a união
    # das interseções, porque as áreas de um mesmo tipo se sobrepõem) e a área que mais pesa.
    areas = protected()
    if areas:
        # As duas tabelas sem o sistema de referência no tipo (um vem como CRS84, o outro como
        # EPSG:4674; as coordenadas são as mesmas), senão a junção espacial recusa o par.
        con.execute(f"CREATE TABLE a AS SELECT aid, kind, name, category, g::GEOMETRY AS ag FROM '{areas}'")
        con.execute("CREATE TABLE IF NOT EXISTS pu AS SELECT rid, g::GEOMETRY AS g FROM p")
        con.execute("CREATE TABLE hit AS SELECT pu.rid, a.aid FROM pu JOIN a ON ST_Intersects(pu.g, a.ag)")
        con.execute("""
            CREATE TABLE cut AS
            SELECT h.rid, a.kind, a.name, a.category, ST_Intersection(pu.g, a.ag) AS ig, ST_Area(pu.g) AS area
            FROM hit h JOIN pu ON pu.rid = h.rid JOIN a ON a.aid = h.aid
        """)
        # A parcela é a união das interseções; o nome, o da área que soma mais (uma área pode vir
        # em vários pedaços).
        con.execute("""
            CREATE TABLE shares AS
            WITH whole AS (SELECT rid, kind, least(1.0, ST_Area(ST_Union_Agg(ig)) / nullif(any_value(area), 0)) AS share
                           FROM cut WHERE ST_Area(ig) > 0 GROUP BY rid, kind),
                 named AS (SELECT rid, kind, name, any_value(category) AS category, sum(ST_Area(ig)) AS inside
                           FROM cut WHERE ST_Area(ig) > 0 GROUP BY rid, kind, name),
                 main AS (SELECT rid, kind, arg_max(name, inside) AS name, arg_max(category, inside) AS category
                          FROM named GROUP BY rid, kind)
            SELECT w.rid, w.kind, w.share, m.name, m.category FROM whole w JOIN main m USING (rid, kind)
        """)
    else:
        con.execute("""CREATE TABLE shares (rid BIGINT, kind VARCHAR, share DOUBLE, name VARCHAR, category VARCHAR)""")
    # Embargos do Ibama: quantos termos ativos tocam o imóvel e que parcela da área está embargada.
    # Um polígono só conta se cobre ao menos 1% do imóvel ou meio hectare; um ponto, se cai dentro.
    embargo_file = embargoes()
    if embargo_file:
        con.execute("CREATE TABLE IF NOT EXISTS pu AS SELECT rid, g::GEOMETRY AS g FROM p")
        con.execute(f"CREATE TABLE em AS SELECT id, has_area, g::GEOMETRY AS eg FROM '{embargo_file}' WHERE state = '{uf}'")
        con.execute("CREATE TABLE ehit AS SELECT pu.rid, em.id FROM pu JOIN em ON ST_Intersects(pu.g, em.eg)")
        con.execute("""
            CREATE TABLE ecut AS
            SELECT h.rid, h.id, em.has_area, p.area_ha, ST_Area(pu.g) AS area,
                   CASE WHEN em.has_area THEN ST_Intersection(pu.g, em.eg) END AS ig
            FROM ehit h JOIN pu ON pu.rid = h.rid JOIN em ON em.id = h.id JOIN p ON p.rid = h.rid
        """)
        con.execute("""
            CREATE TABLE embargo AS
            WITH kept AS (
                SELECT * FROM ecut
                WHERE NOT has_area OR ST_Area(ig) / nullif(area, 0) >= 0.01
                      OR ST_Area(ig) / nullif(area, 0) * area_ha >= 0.5)
            SELECT rid, count(DISTINCT id)::INTEGER AS embargo_count,
                   coalesce(least(1.0, ST_Area(ST_Union_Agg(ig) FILTER (has_area)) / nullif(any_value(area), 0)), 0) AS embargo_share
            FROM kept GROUP BY rid
        """)
    else:
        con.execute("CREATE TABLE embargo (rid BIGINT, embargo_count INTEGER, embargo_share DOUBLE)")
    em_count = "coalesce(eb.embargo_count, 0)::INTEGER" if embargo_file else "NULL::INTEGER"
    em_share = "round(coalesce(eb.embargo_share, 0), 4)" if embargo_file else "NULL::DOUBLE"
    # Sigef (Incra): que parcela do imóvel está coberta por parcelas georreferenciadas, quantas são, e
    # quanto cai em parcela cuja natureza é território quilombola ou assentamento.
    sigef_file = sigef(uf)
    if sigef_file:
        con.execute("CREATE TABLE IF NOT EXISTS pu AS SELECT rid, g::GEOMETRY AS g FROM p")
        con.execute(f"CREATE TABLE sg AS SELECT id, kind, g::GEOMETRY AS sgg FROM '{sigef_file}' WHERE g IS NOT NULL")
        con.execute("CREATE TABLE shit AS SELECT pu.rid, sg.id FROM pu JOIN sg ON ST_Intersects(pu.g, sg.sgg)")
        con.execute("""
            CREATE TABLE scut AS
            SELECT h.rid, h.id, sg.kind, p.area_ha, ST_Area(pu.g) AS area, ST_Intersection(pu.g, sg.sgg) AS ig
            FROM shit h JOIN pu ON pu.rid = h.rid JOIN sg ON sg.id = h.id JOIN p ON p.rid = h.rid
        """)
        con.execute("""
            CREATE TABLE certified AS
            WITH kept AS (
                SELECT * FROM scut
                WHERE ST_Area(ig) / nullif(area, 0) >= 0.01 OR ST_Area(ig) / nullif(area, 0) * area_ha >= 0.5)
            SELECT rid, count(DISTINCT id)::INTEGER AS sigef_count,
                   least(1.0, ST_Area(ST_Union_Agg(ig)) / nullif(any_value(area), 0)) AS sigef_share,
                   coalesce(least(1.0, ST_Area(ST_Union_Agg(ig) FILTER (kind = 'Território Quilombola'))
                                       / nullif(any_value(area), 0)), 0) AS quilombola_share,
                   coalesce(least(1.0, ST_Area(ST_Union_Agg(ig) FILTER (kind LIKE 'Assentamento%'))
                                       / nullif(any_value(area), 0)), 0) AS settlement_share
            FROM kept GROUP BY rid
        """)
    else:
        con.execute("""CREATE TABLE certified (rid BIGINT, sigef_count INTEGER, sigef_share DOUBLE,
                                               quilombola_share DOUBLE, settlement_share DOUBLE)""")
    sg_count = "coalesce(ct.sigef_count, 0)::INTEGER" if sigef_file else "NULL::INTEGER"
    sg_col = (lambda c: f"round(coalesce(ct.{c}, 0), 4)") if sigef_file else (lambda c: "NULL::DOUBLE")
    # Sem as camadas, as parcelas ficam nulas (não calculadas), não zero.
    share = (lambda k: f"round(coalesce({k}.share, 0), 4)") if areas else (lambda k: "NULL::DOUBLE")
    quality_file = out / "quality.parquet"
    con.execute(f"""
        COPY (
            SELECT p.property_code, p.municipality_code,
                   coalesce(o.overlap_count, 0)::INTEGER AS overlap_count,
                   round(least(1.0, coalesce(o.inter, 0) / nullif(ST_Area(p.g), 0)), 4) AS overlap_share,
                   round({outside}, 4) AS outside_share,
                   {share('ti')} AS indigenous_share,
                   {share('pi')} AS strict_protection_share,
                   {share('us')} AS sustainable_use_share,
                   {share('fb')} AS public_forest_share,
                   ti.name AS indigenous_land,
                   coalesce(pi.name, us.name) AS conservation_unit,
                   coalesce(pi.category, us.category) AS conservation_category,
                   {em_count} AS embargo_count,
                   {em_share} AS embargo_share,
                   {sg_count} AS sigef_count,
                   {sg_col('sigef_share')} AS sigef_share,
                   {sg_col('quilombola_share')} AS quilombola_share,
                   {sg_col('settlement_share')} AS settlement_share
            FROM p LEFT JOIN overlap o USING (rid) {join}
                 LEFT JOIN embargo eb ON eb.rid = p.rid
                 LEFT JOIN certified ct ON ct.rid = p.rid
                 LEFT JOIN shares ti ON ti.rid = p.rid AND ti.kind = 'indigenous'
                 LEFT JOIN shares pi ON pi.rid = p.rid AND pi.kind = 'strict'
                 LEFT JOIN shares us ON us.rid = p.rid AND us.kind = 'sustainable'
                 LEFT JOIN shares fb ON fb.rid = p.rid AND fb.kind = 'public_forest'
            ORDER BY p.municipality_code, p.property_code
        ) TO '{quality_file}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 8192)
    """)
    towns_file = out / "quality_municipalities.parquet"
    con.execute(f"""
        COPY (
            SELECT '{uf}' AS state, q.municipality_code, count(*) AS properties,
                   count(*) FILTER (q.overlap_share > 0.01) AS overlapping,
                   count(*) FILTER (q.overlap_share > 0.5) AS overlapping_half,
                   round(sum(p.area_ha * q.overlap_share), 2) AS overlap_ha,
                   count(*) FILTER (q.outside_share > 0.5) AS outside_half,
                   round(sum(p.area_ha * q.outside_share), 2) AS outside_ha,
                   count(*) FILTER (q.indigenous_share > 0.01) AS indigenous,
                   round(sum(p.area_ha * q.indigenous_share), 2) AS indigenous_ha,
                   count(*) FILTER (q.strict_protection_share > 0.01) AS strict_protection,
                   round(sum(p.area_ha * q.strict_protection_share), 2) AS strict_protection_ha,
                   count(*) FILTER (q.sustainable_use_share > 0.01) AS sustainable_use,
                   round(sum(p.area_ha * q.sustainable_use_share), 2) AS sustainable_use_ha,
                   count(*) FILTER (q.public_forest_share > 0.01) AS public_forest,
                   round(sum(p.area_ha * q.public_forest_share), 2) AS public_forest_ha,
                   count(*) FILTER (q.embargo_count > 0) AS embargoed,
                   round(sum(p.area_ha * q.embargo_share), 2) AS embargo_ha,
                   count(*) FILTER (q.sigef_share >= 0.5) AS certified,
                   round(sum(p.area_ha * q.sigef_share), 2) AS sigef_ha,
                   count(*) FILTER (q.quilombola_share > 0.01) AS quilombola,
                   count(*) FILTER (q.settlement_share > 0.01) AS settlement
            FROM '{quality_file}' q JOIN p USING (property_code)
            GROUP BY ALL ORDER BY q.municipality_code
        ) TO '{towns_file}' (FORMAT parquet, COMPRESSION zstd)
    """)
    n, touching, half, out_half, in_ti, in_pi, in_us, embargoed, in_fb, in_sg, in_qu = con.execute(f"""
        SELECT count(*), count(*) FILTER (overlap_share > 0.01), count(*) FILTER (overlap_share > 0.5),
               count(*) FILTER (outside_share > 0.5), count(*) FILTER (indigenous_share > 0.01),
               count(*) FILTER (strict_protection_share > 0.01), count(*) FILTER (sustainable_use_share > 0.01),
               count(*) FILTER (embargo_count > 0), count(*) FILTER (public_forest_share > 0.01),
               count(*) FILTER (sigef_share >= 0.5), count(*) FILTER (quilombola_share > 0.01)
        FROM '{quality_file}'
    """).fetchone()
    files = {f"quality/state={uf}.parquet": quality_file,
             f"quality-municipalities/state={uf}.parquet": towns_file}
    for key, path in files.items():
        s3.upload_file(str(path), name, f"current/{key}",
                       ExtraArgs={"ContentType": "application/vnd.apache.parquet"})
    cat = remote_catalog(s3, name)
    cat["files"] = sorted([f for f in cat["files"] if f["path"] not in files] + [
        {"path": key, "bytes": path.stat().st_size, "sha256": sha256(path)} for key, path in files.items()],
        key=lambda f: f["path"])
    cat["states"][uf]["quality_of"] = cat["states"][uf].get("downloaded_to")
    sources = ROOT / "protected" / "areas.json"
    if areas and sources.exists():
        cat["source"]["protected_areas"] = json.loads(sources.read_text())
    embargo_source = ROOT / "protected" / "embargos.json"
    if embargo_file and embargo_source.exists():
        cat["source"]["embargoes"] = json.loads(embargo_source.read_text())
    if sigef_file:
        cat["source"]["sigef"] = {"source": "Incra, Sigef (GeoPortal, camada sigef_geo)", "url": SIGEF_WFS,
                                  "downloaded": dt.datetime.fromtimestamp(sigef_file.stat().st_mtime, dt.UTC).strftime("%Y-%m-%d")}
    s3.put_object(Bucket=name, Key="current/catalog.json", ContentType="application/json",
                  Body=json.dumps(cat, indent=2, ensure_ascii=False).encode())
    log(f"{uf}: qualidade de {n} imóveis em {time.time() - t0:.0f}s; {touching} com sobreposição acima de 1%, "
        f"{half} acima de 50%, {out_half} com mais da metade fora do município; {in_ti} em terra indígena, "
        f"{in_pi} em proteção integral, {in_us} em uso sustentável, {embargoed} com embargo do Ibama, {in_fb} em floresta pública tipo B, {in_sg} com mais da metade no Sigef, "
        f"{in_qu} em território quilombola do Sigef"
        + ("" if boundaries else " (sem malha do IBGE)"))


TYPOLOGY = Path(__file__).resolve().parent / "typology.json"
FEW = {"code": "few", "label": "Poucos cadastros",
       "description": "Menos de 30 imóveis não cancelados: pouco para caracterizar o município."}


def typology(_ufs: list[str] | None = None) -> None:
    """Tipologia dos municípios: cada um vai para o grupo de centro mais próximo.

    O perfil de cada município (porte, etapas da análise, sobreposição e cobertura do território)
    é calculado com o que está publicado no bucket. Os grupos vêm da referência fixa em
    `etl/typology.json` (ajustada por `etl/typology_fit.py`): nada é reajustado aqui, para que um
    grupo signifique a mesma coisa em todas as semanas. Publica `typology/municipalities.parquet`,
    guarda uma cópia datada em `history/typology/` e descreve os grupos no catálogo.
    """
    s3, name = bucket()
    cat = remote_catalog(s3, name)
    ref = json.loads(TYPOLOGY.read_text())
    features, floor = ref["features"], ref["min_properties"]
    con, url = remote_duck(name)
    con.execute("INSTALL spatial; LOAD spatial; SET enable_progress_bar = false;")
    con.execute("SET geometry_always_xy = true")      # a área na superfície da Terra lê longitude, latitude
    con.execute("""CREATE TABLE town (state VARCHAR, municipality_code INTEGER, municipality VARCHAR,
        properties BIGINT, area_ha DOUBLE, small BIGINT, large_ha DOUBLE, waiting BIGINT, notified BIGINT,
        analysed BIGINT)""")
    con.execute("CREATE TABLE territory (municipality_code INTEGER, territory_ha DOUBLE)")
    t0 = time.time()
    states = sorted(cat["states"])
    for uf in states:
        con.execute(f"""
            INSERT INTO town
            SELECT '{uf}', municipality_code, any_value(municipality), count(*), sum(area_ha),
                   count(*) FILTER (size_class = 'small'), coalesce(sum(area_ha) FILTER (size_class = 'large'), 0),
                   count(*) FILTER (condition = 'Aguardando análise'),
                   count(*) FILTER (condition LIKE '%aguardando atendimento a notificação%'),
                   count(*) FILTER (condition <> 'Aguardando análise' AND condition NOT LIKE 'Cancelado%'
                                    AND condition NOT LIKE '%aguardando atendimento a notificação%')
            FROM read_parquet('{url(f'properties/state={uf}/part-0.parquet')}')
            WHERE status <> 'cancelled' GROUP BY municipality_code
        """)
        boundaries = mesh(uf)
        if boundaries:
            con.execute(f"""INSERT INTO territory SELECT codarea::INTEGER,
                ST_Area_Spheroid(ST_MakeValid(geom)::GEOMETRY) / 1e4 FROM ST_Read('{boundaries}')""")
    quality_files = [url(f"quality-municipalities/state={uf}.parquet") for uf in states
                     if any(f["path"] == f"quality-municipalities/state={uf}.parquet" for f in cat["files"])]
    con.execute(f"""
        CREATE TABLE profile AS
        SELECT t.state, t.municipality_code, t.municipality, t.properties,
               round(t.small / t.properties, 4) AS small_share,
               round(least(1.0, t.large_ha / nullif(t.area_ha, 0)), 4) AS large_area_share,
               round(t.waiting / t.properties, 4) AS waiting_share,
               round(t.notified / t.properties, 4) AS notified_share,
               round(t.analysed / t.properties, 4) AS analysed_share,
               round(q.overlapping_half / nullif(q.properties, 0), 4) AS overlap_share,
               round(least(2.0, t.area_ha / nullif(r.territory_ha, 0)), 4) AS coverage
        FROM town t LEFT JOIN territory r USING (municipality_code)
             LEFT JOIN read_parquet({quality_files}, union_by_name = true) q USING (municipality_code)
    """)
    rows = con.execute(f"SELECT municipality_code, properties, {', '.join(features)} FROM profile").fetchall()
    assigned = []
    for code, n, *values in rows:
        if n < floor or any(v is None for v in values):
            assigned.append((code, FEW["code"]))
            continue
        z = [max(-3.0, min(3.0, (v - ref["mean"][f]) / ref["std"][f])) for f, v in zip(features, values)]
        best = min(ref["groups"], key=lambda g: sum((a - g["center"][f]) ** 2 for f, a in zip(features, z)))
        assigned.append((code, best["code"]))
    con.execute("CREATE TABLE assigned (municipality_code INTEGER, typology VARCHAR)")
    con.executemany("INSERT INTO assigned VALUES (?, ?)", assigned)
    groups = [*ref["groups"], FEW]
    con.execute("CREATE TABLE label (typology VARCHAR, typology_label VARCHAR)")
    con.executemany("INSERT INTO label VALUES (?, ?)", [(g["code"], g["label"]) for g in groups])
    out = ROOT / "typology"
    out.mkdir(parents=True, exist_ok=True)
    path = out / "municipalities.parquet"
    con.execute(f"""
        COPY (SELECT p.state, p.municipality_code, p.municipality, a.typology, l.typology_label,
                     p.* EXCLUDE (state, municipality_code, municipality)
              FROM profile p JOIN assigned a USING (municipality_code) JOIN label l USING (typology)
              ORDER BY p.municipality_code)
        TO '{path}' (FORMAT parquet, COMPRESSION zstd)
    """)
    counts = dict(con.execute(f"SELECT typology, count(*) FROM '{path}' GROUP BY 1").fetchall())
    totals = dict(con.execute(f"SELECT typology, sum(properties) FROM '{path}' GROUP BY 1").fetchall())
    key = "typology/municipalities.parquet"
    s3.upload_file(str(path), name, f"current/{key}", ExtraArgs={"ContentType": "application/vnd.apache.parquet"})
    cat = remote_catalog(s3, name)
    s3.upload_file(str(path), name, f"history/typology/date={cat['version']}.parquet",
                   ExtraArgs={"ContentType": "application/vnd.apache.parquet"})
    cat["files"] = sorted([f for f in cat["files"] if f["path"] != key] + [
        {"path": key, "bytes": path.stat().st_size, "sha256": sha256(path)}], key=lambda f: f["path"])
    cat["typology"] = {
        "of": cat["version"], "fitted": ref["fitted"], "min_properties": floor, "features": features,
        "groups": [{"code": g["code"], "label": g["label"], "description": g["description"],
                    "municipalities": counts.get(g["code"], 0), "properties": int(totals.get(g["code"], 0))}
                   for g in groups],
    }
    s3.put_object(Bucket=name, Key="current/catalog.json", ContentType="application/json",
                  Body=json.dumps(cat, indent=2, ensure_ascii=False).encode())
    log(f"tipologia de {len(assigned)} municípios em {time.time() - t0:.0f}s: {counts}")


def backup_history() -> None:
    """Copia para o disco local o que há em `history/` no bucket (os arquivos nunca mudam).

    Os dados correntes podem ser baixados de novo do Sicar; o histórico, não.
    """
    s3, name = bucket()
    dest = ROOT / "history-backup"
    copied, kw = 0, {"Bucket": name, "Prefix": "history/"}
    while True:
        page = s3.list_objects_v2(**kw)
        for obj in page.get("Contents", []):
            path = dest / obj["Key"]
            if not path.exists() or path.stat().st_size != obj["Size"]:
                path.parent.mkdir(parents=True, exist_ok=True)
                s3.download_file(name, obj["Key"], str(path))
                copied += 1
        if not page.get("IsTruncated"):
            break
        kw["ContinuationToken"] = page["NextContinuationToken"]
    catalog = dest / "catalog" / f"{dt.datetime.now(dt.UTC):%Y-%m-%d}.json"
    catalog.parent.mkdir(parents=True, exist_ok=True)
    catalog.write_text(json.dumps(remote_catalog(s3, name), indent=2, ensure_ascii=False))
    log(f"histórico: {copied} arquivos novos copiados para {dest}")


def exclusive():
    """Só uma execução por vez mexe nos dados locais e no catálogo."""
    ROOT.mkdir(parents=True, exist_ok=True)
    handle = open(ROOT / ".lock", "w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("outra execução do ETL está em andamento; nada foi feito") from None
    return handle


def clean(uf: str) -> None:
    """Apaga os dados locais de um estado (brutos e gerados)."""
    for d in (ROOT / "raw" / uf.lower(), ROOT / "release" / uf):
        if d.exists():
            shutil.rmtree(d)
    log(f"{uf}: dados locais apagados")


def stale(uf: str) -> bool:
    """Há um download parcial antigo deste estado? Misturá-lo com páginas novas daria um
    retrato de duas datas."""
    manifest = ROOT / "raw" / uf.lower() / "manifest.json"
    if not manifest.exists():
        return (ROOT / "raw" / uf.lower()).exists()
    started = dt.datetime.fromisoformat(json.loads(manifest.read_text())["started_at"])
    return dt.datetime.now(dt.UTC) - started > dt.timedelta(hours=STALE_HOURS)


def run_all(ufs: list[str], refresh: bool = False) -> None:
    s3, name = bucket()
    done = set(remote_catalog(s3, name)["states"])
    todo = ufs or [uf for uf in STATES if refresh or uf not in done]
    log(f"publicados: {len(done)}; fila: {todo}")
    failed = {}
    for uf in todo:
        try:
            if stale(uf):
                clean(uf)
            download(uf)
            build(uf)
            publish(uf)
            try:
                quality(uf)
            except Exception as err:
                log(f"{uf}: indicadores de qualidade FALHARAM — {err}")
            clean(uf)
        except (Exception, SystemExit) as err:  # um estado com problema não derruba a fila
            failed[uf] = str(err)
            log(f"{uf}: FALHOU — {err}")
    log(f"fila concluída: {len(todo) - len(failed)} de {len(todo)} estados publicados")
    try:
        typology()
    except Exception as err:  # a tipologia não invalida a rodada
        log(f"tipologia FALHOU — {err}")
    try:
        backup_history()
    except Exception as err:  # a cópia de segurança não invalida a rodada
        log(f"histórico: cópia de segurança FALHOU — {err}")
    if failed:
        raise SystemExit(f"estados com falha: {failed}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("step", choices=["download", "build", "publish", "clean", "all", "refresh", "verify", "summary", "web", "history", "backup", "quality", "typology"])
    ap.add_argument("uf", nargs="*", help="siglas das UFs, ex.: PE")
    a = ap.parse_args()
    ufs = [u.upper() for u in a.uf]
    lock = exclusive() if a.step not in ("verify", "web") else None
    if a.step == "refresh":
        run_all(ufs, refresh=True)
    elif a.step == "backup":
        backup_history()
    elif a.step in ("all", "verify", "summary", "web", "history", "typology"):
        {"all": run_all, "verify": verify, "summary": summary, "web": web, "history": history_init,
         "typology": typology}[a.step](ufs)
    else:
        if not ufs:
            ap.error("informe a UF")
        for uf in ufs:
            {"download": download, "build": build, "publish": publish, "clean": clean,
             "quality": quality}[a.step](uf)

# dags/enexis_gasleidingen.py
# -*- coding: utf-8 -*-
import json, requests, re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

# ─────────────────────────────────────────────────────────────────────
# HARD-CODED KEUZE: laad 'all', alleen 'leiding', of alleen 'service_leiding'
SELECT_LAYER = "service_leiding"  # opties: "all" | "leiding" | "service_leiding"
# ─────────────────────────────────────────────────────────────────────

POSTGRES_CONN_ID = "geoserver"

BBOX_SQL = """
WITH e AS (
  SELECT ST_Extent(geom) AS b
  FROM externe_bronnen.vrzl
  WHERE naam = 'vrzl'
)
SELECT ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b) FROM e;
"""

WFS_URL       = "https://opendata.enexis.nl/geoserver/wfs"
WFS_SRS       = "EPSG:28992"
WFS_PAGE_SIZE = 5000  # 2.0.0: pagina-grootte; 1.1.0: maxFeatures

# ─────────────────────────────────────────────────────────────────────
# Laagconfiguratie per dataset
# Opmerking bij service_leiding:
#   De Enexis-GeoServer retourneert bij WFS 2.0.0 + paging een 400:
#   "Cannot do natural order without a primary key..."
#   Dit is een bekende beperking: WFS 2.0.0 paging vereist een stabiele PK/sortering.
#   Daarom forceren we voor service_leiding WFS 1.1.0 (zonder paging).
# ─────────────────────────────────────────────────────────────────────
LAYERS = {
    "leiding": {
        "typename":      "Enexis_Opendata:asm_g_leiding",
        "target":        "externe_bronnen.enexis_gasleiding",
        "force_version": "2.0.0",  # werkt goed met paging
    },
    "service_leiding": {
        "typename":      "Enexis_Opendata:asm_g_service_leiding",
        "target":        "externe_bronnen.enexis_gasleiding_service",
        "force_version": "1.1.0",  # NIET compatibel met 2.0.0 paging → forceer 1.1.0
    },
}

# ---------- helpers ----------
def _num(v):
    if v in (None, "", "null"): return None
    try: return float(v)
    except Exception: return None

def _bool(v):
    if v is None: return None
    if isinstance(v, bool): return v
    s = str(v).strip().lower()
    if s in ("true","t","1","yes","y","ja"): return True
    if s in ("false","f","0","no","n","nee"): return False
    return None

def _warmteafstand(pressure_bar, diameter_mm):
    p = _num(pressure_bar)
    d = _num(diameter_mm)
    if p is None or d is None:
        return None

    if p <= 0.1:
        row = ["10","15","25","35","50","70",">90"]
    elif p <= 1:
        row = ["15","30","45","65","100","140",">180"]
    elif p <= 4:
        row = ["25","50","80","110","170","230",">300"]
    elif p <= 8:
        row = ["30","65","100","140","220","300",">400"]
    else:
        row = ["30","65","100","140","220","300",">400"]

    if d <= 50: idx = 0
    elif d <= 100: idx = 1
    elif d <= 150: idx = 2
    elif d <= 200: idx = 3
    elif d <= 300: idx = 4
    elif d <= 400: idx = 5
    else: idx = 6

    return row[idx]

def _warmteafstand_to_int(wtxt):
    if wtxt is None:
        return None
    s = str(wtxt).strip()
    try:
        return int(s)
    except Exception:
        pass
    if s.startswith(">"):
        s2 = s[1:].strip()
        try:
            return int(s2)
        except Exception:
            return None
    digits = re.sub(r"[^\d]", "", s)
    return int(digits) if digits else None

def _show_response_prefix(resp, maxlen=500):
    try:
        ct = resp.headers.get("content-type", "")
        snippet = resp.text[:maxlen]
        print(f"[DEBUG] content-type={ct}")
        print(f"[DEBUG] body prefix:\n{snippet}")
    except Exception as e:
        print(f"[DEBUG] cannot print response body: {e!r}")

# ---------- data access ----------
def get_bbox(**_):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    xmin, ymin, xmax, ymax = pg.get_first(BBOX_SQL)
    if None in (xmin, ymin, xmax, ymax):
        raise ValueError("BBOX_SQL gaf NULL terug.")
    return {"xmin": float(xmin), "ymin": float(ymin), "xmax": float(xmax), "ymax": float(ymax)}

def fetch_page(typename: str, bbox: dict, start_index: int, count: int, version: str):
    """
    Haal features op met de juiste parameters per WFS-versie.
    - 2.0.0: typeNames + count + startIndex (paging)
    - 1.1.0: typeName + maxFeatures (geen paging)
    Retourneert (json, version_used)
    """
    if version == "2.0.0":
        params = {
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": typename, "srsName": WFS_SRS, "outputFormat": "application/json",
            "bbox": f"{bbox['xmin']},{bbox['ymin']},{bbox['xmax']},{bbox['ymax']},{WFS_SRS}",
            "startIndex": start_index, "count": count,
        }
    else:  # 1.1.0
        params = {
            "service": "WFS", "version": "1.1.0", "request": "GetFeature",
            "typeName": typename, "srsName": WFS_SRS, "outputFormat": "application/json",
            "bbox": f"{bbox['xmin']},{bbox['ymin']},{bbox['xmax']},{bbox['ymax']},{WFS_SRS}",
            "maxFeatures": count,
        }

    r = requests.get(WFS_URL, params=params, timeout=180)
    if r.status_code != 200:
        print(f"[ERROR] WFS {version} HTTP {r.status_code} for URL: {r.url}")
        _show_response_prefix(r)
        r.raise_for_status()
    return r.json(), version

def _flush(pg: PostgresHook, sql: str, template: str, rows):
    conn = pg.get_conn()
    with conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, template=template)

def load_layer(cfg: dict, bbox: dict):
    """
    Laadt één layer in batches naar de doeltabel.
    - Voor 2.0.0: pagina's ophalen met startIndex/count.
    - Voor 1.1.0: één batch (geen paging).
    """
    typename = cfg["typename"]
    target   = cfg["target"]
    version  = cfg.get("force_version", "2.0.0")

    print(f"[INFO] Start load: {typename} → {target} (WFS {version})")

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    insert_sql = f"""
      INSERT INTO {target} (
        fid, beginlifespanversion, sourcereference, innetwork, fictitious, currentstatus,
        validfrom, verticalposition, utilitydeliverytype, warningtype, geonauwkeurigheidxy,
        pipediameter, pipediameteruom, pressure, pressureuom, oilgaschemicalsproducttype,
        label, omschrijving, toelichting, buismateriaaltype,
        warmteafstand_m, geom, properties
      )
      VALUES %s
      ON CONFLICT (fid) DO UPDATE SET
        beginlifespanversion       = EXCLUDED.beginlifespanversion,
        sourcereference            = EXCLUDED.sourcereference,
        innetwork                  = EXCLUDED.innetwork,
        fictitious                 = EXCLUDED.fictitious,
        currentstatus              = EXCLUDED.currentstatus,
        validfrom                  = EXCLUDED.validfrom,
        verticalposition           = EXCLUDED.verticalposition,
        utilitydeliverytype        = EXCLUDED.utilitydeliverytype,
        warningtype                = EXCLUDED.warningtype,
        geonauwkeurigheidxy        = EXCLUDED.geonauwkeurigheidxy,
        pipediameter               = EXCLUDED.pipediameter,
        pipediameteruom            = EXCLUDED.pipediameteruom,
        pressure                   = EXCLUDED.pressure,
        pressureuom                = EXCLUDED.pressureuom,
        oilgaschemicalsproducttype = EXCLUDED.oilgaschemicalsproducttype,
        label                      = EXCLUDED.label,
        omschrijving               = EXCLUDED.omschrijving,
        toelichting                = EXCLUDED.toelichting,
        buismateriaaltype          = EXCLUDED.buismateriaaltype,
        warmteafstand_m            = EXCLUDED.warmteafstand_m,
        geom                       = EXCLUDED.geom,
        properties                 = EXCLUDED.properties;
    """

    template = "(" + ",".join([
        "%s", "%s", "%s", "%s", "%s", "%s",
        "%s", "%s", "%s", "%s", "%s",
        "%s", "%s", "%s", "%s", "%s",
        "%s", "%s", "%s", "%s",
        "%s",
        "ST_SetSRID(ST_GeomFromGeoJSON(%s), 28992)",
        "%s"
    ]) + ")"

    start = 0
    total = 0
    batch = []

    while True:
        data, ver = fetch_page(typename, bbox, start, WFS_PAGE_SIZE, version)
        feats = data.get("features", [])
        if not feats:
            print(f"[INFO] Geen features in deze batch (ver={ver}, startIndex={start}). Stop.")
            break

        for f in feats:
            p = f.get("properties") or {}
            diameter_mm = _num(p.get("pipediameter"))
            druk_bar    = _num(p.get("pressure"))
            warmte_txt  = _warmteafstand(druk_bar, diameter_mm)
            warmte_int  = _warmteafstand_to_int(warmte_txt)

            batch.append((
                int(p.get("fid")) if p.get("fid") else None,
                p.get("beginlifespanversion"),
                p.get("sourcereference"),
                p.get("innetwork"),
                _bool(p.get("fictitious")),
                p.get("currentstatus"),
                p.get("validfrom"),
                p.get("verticalposition"),
                p.get("utilitydeliverytype"),
                p.get("warningtype"),
                p.get("geonauwkeurigheidxy"),
                diameter_mm,
                p.get("pipediameteruom"),
                druk_bar,
                p.get("pressureuom"),
                p.get("oilgaschemicalsproducttype"),
                p.get("label"),
                p.get("omschrijving"),
                p.get("toelichting"),
                p.get("buismateriaaltype"),
                warmte_int,
                json.dumps(f.get("geometry")),
                json.dumps(p),
            ))

        total += len(feats)

        if len(batch) >= 10000:
            _flush(pg, insert_sql, template, batch); batch.clear()

        # Paginatie alleen bij WFS 2.0.0
        if ver == "2.0.0":
            start += len(feats)
            # Als minder dan page size terugkomt, is dit de laatste pagina
            if len(feats) < WFS_PAGE_SIZE:
                break
        else:
            # 1.1.0: geen paging, dus na één call stoppen
            break

    if batch:
        _flush(pg, insert_sql, template, batch)

    print(f"[INFO] Klaar: {typename} → {target}. Totaal features verwerkt: {total}")

# ---------- Airflow callable ----------
def run_selected_layers(**ctx):
    bbox = ctx["ti"].xcom_pull(task_ids="get_bbox")

    if SELECT_LAYER == "leiding":
        layers_to_run = [LAYERS["leiding"]]
    elif SELECT_LAYER == "service_leiding":
        layers_to_run = [LAYERS["service_leiding"]]
    else:
        layers_to_run = [LAYERS["leiding"], LAYERS["service_leiding"]]

    for cfg in layers_to_run:
        load_layer(cfg, bbox)

# ---- DAG ----
default_args = {"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="enexis_gasleidingen",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # Airflow 2.x: 'schedule' kan ook
    catchup=False,
    tags=["wfs", "geoserver", "postgis", "enexis"],
) as dag:
    t_bbox = PythonOperator(task_id="get_bbox", python_callable=get_bbox)
    t_load = PythonOperator(task_id="run_selected_layers", python_callable=run_selected_layers, provide_context=True)
    t_bbox >> t_load

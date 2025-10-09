# dags/enexis_gasleiding.py
import json, requests, re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

POSTGRES_CONN_ID = "geoserver"

BBOX_SQL = """
WITH e AS (
  SELECT ST_Extent(geom) AS b
  FROM externe_bronnen.vrzl
  WHERE naam = 'vrzl'
)
SELECT ST_XMin(b) AS xmin, ST_YMin(b) AS ymin, ST_XMax(b) AS xmax, ST_YMax(b) AS ymax
FROM e;
"""

# --- WFS parameters
WFS_URL       = "https://opendata.enexis.nl/geoserver/wfs"
WFS_TYPENAME  = "Enexis_Opendata:asm_g_leiding"
WFS_SRS       = "EPSG:28992"
WFS_VERSION   = "2.0.0"
WFS_PAGE_SIZE = 5000
TARGET        = "externe_bronnen.enexis_gasleiding"

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
    """Berekent veiligheidafstand (warmtebelasting) volgens tabel.
       Retourneert string zoals '25' of '>300'
    """
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
    """
    Converteer 'warmteafstand' tekst naar integer voor opslag:
    - Probeer direct te parsen als int ('300' -> 300).
    - Lukt dat niet en staat er een '>' voor ('>300'), verwijder '>' en parse opnieuw ('>300' -> 300).
      LET OP: Dit kan een *onderschatting* zijn; bij '>N' moet operationeel met
      een grotere afstand rekening worden gehouden. (Bewust in comments vastgelegd.)
    - Bij None of onparseerbaar: return None.
    """
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
    # laatste redmiddel: strip niet-cijfers aan begin/einde
    digits = re.sub(r"[^\d]", "", s)
    return int(digits) if digits else None

# ---------- main tasks ----------
def get_bbox(**_):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    xmin, ymin, xmax, ymax = pg.get_first(BBOX_SQL)
    if None in (xmin, ymin, xmax, ymax):
        raise ValueError("BBOX_SQL gaf NULL terug.")
    return {"xmin": float(xmin), "ymin": float(ymin), "xmax": float(xmax), "ymax": float(ymax)}

def fetch_page(bbox, start_index: int, count: int):
    params = {
        "service": "WFS",
        "version": WFS_VERSION,
        "request": "GetFeature",
        "typeNames": WFS_TYPENAME,
        "srsName": WFS_SRS,
        "outputFormat": "application/json",
        "bbox": f"{bbox['xmin']},{bbox['ymin']},{bbox['xmax']},{bbox['ymax']},{WFS_SRS}",
        "startIndex": start_index,
        "count": count,
    }
    r = requests.get(WFS_URL, params=params, timeout=180)
    r.raise_for_status()
    return r.json()

def load_all(**ctx):
    bbox = ctx["ti"].xcom_pull(task_ids="get_bbox")
    pg   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    insert_sql = f"""
      INSERT INTO {TARGET} (
        fid, beginlifespanversion, sourcereference, innetwork, fictitious, currentstatus,
        validfrom, verticalposition, utilitydeliverytype, warningtype, geonauwkeurigheidxy,
        pipediameter, pipediameteruom, pressure, pressureuom, oilgaschemicalsproducttype,
        label, omschrijving, toelichting, buismateriaaltype,
        warmteafstand_m, geom, properties
      )
      VALUES %s
      ON CONFLICT (fid) DO UPDATE SET
        beginlifespanversion        = EXCLUDED.beginlifespanversion,
        sourcereference             = EXCLUDED.sourcereference,
        innetwork                   = EXCLUDED.innetwork,
        fictitious                  = EXCLUDED.fictitious,
        currentstatus               = EXCLUDED.currentstatus,
        validfrom                   = EXCLUDED.validfrom,
        verticalposition            = EXCLUDED.verticalposition,
        utilitydeliverytype         = EXCLUDED.utilitydeliverytype,
        warningtype                 = EXCLUDED.warningtype,
        geonauwkeurigheidxy         = EXCLUDED.geonauwkeurigheidxy,
        pipediameter                = EXCLUDED.pipediameter,
        pipediameteruom             = EXCLUDED.pipediameteruom,
        pressure                    = EXCLUDED.pressure,
        pressureuom                 = EXCLUDED.pressureuom,
        oilgaschemicalsproducttype  = EXCLUDED.oilgaschemicalsproducttype,
        label                       = EXCLUDED.label,
        omschrijving                = EXCLUDED.omschrijving,
        toelichting                 = EXCLUDED.toelichting,
        buismateriaaltype           = EXCLUDED.buismateriaaltype,
        warmteafstand_m             = EXCLUDED.warmteafstand_m,
        geom                        = EXCLUDED.geom,
        properties                  = EXCLUDED.properties
    """

    template = "(" + ",".join([
        "%s", "%s", "%s", "%s", "%s", "%s",
        "%s", "%s", "%s", "%s", "%s",
        "%s", "%s", "%s", "%s", "%s",
        "%s", "%s", "%s", "%s",
        "%s",  # warmteafstand_m (INTEGER in DB)
        "ST_SetSRID(ST_GeomFromGeoJSON(%s), 28992)",
        "%s"
    ]) + ")"

    start = 0
    batch = []
    while True:
        data = fetch_page(bbox, start, WFS_PAGE_SIZE)
        feats = data.get("features", [])
        if not feats:
            break

        for f in feats:
            p = f.get("properties") or {}
            diameter_mm = _num(p.get("pipediameter"))
            druk_bar    = _num(p.get("pressure"))
            warmte_txt  = _warmteafstand(druk_bar, diameter_mm)
            warmte_int  = _warmteafstand_to_int(warmte_txt)  # ← parse naar integer

            row = (
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
                warmte_int,  # ← integer naar DB (NB: bij '>N' is dit N; operationeel grotere afstand overwegen)
                json.dumps(f.get("geometry")),
                json.dumps(p),
            )
            batch.append(row)

        if len(batch) >= 10000:
            _flush(pg, insert_sql, template, batch); batch.clear()
        start += len(feats)

    if batch:
        _flush(pg, insert_sql, template, batch)

def _flush(pg: PostgresHook, sql: str, template: str, rows):
    conn = pg.get_conn()
    with conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, template=template)

default_args = {"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)}
with DAG(
    dag_id="enexis_gasleiding",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["wfs","geoserver","postgis","enexis"],
) as dag:
    t_bbox = PythonOperator(task_id="get_bbox", python_callable=get_bbox)
    t_load = PythonOperator(task_id="load_all", python_callable=load_all, provide_context=True)
    t_bbox >> t_load

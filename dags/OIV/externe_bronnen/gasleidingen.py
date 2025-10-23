# -*- coding: utf-8 -*-
import json, re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.wfs_utils import (
    _num, _bool, iter_wfs_batches, flush_values, build_cql_intersects, wfs_hits
)

# ─────────────────────────────────────────────────────────────────────
# Instellingen
# ─────────────────────────────────────────────────────────────────────
SELECT_LAYER = "all"  # opties: "all" | "leiding" | "service_leiding"
POSTGRES_CONN_ID = "geoserver"

# Haal exact de polygo(o)n(en) op met naam = 'vrzl' (en simplificeer WKT iets voor kortere CQL)
POLYGONEN_SQL = """
SELECT
  fid::text AS poly_id,
  ST_AsText(ST_SimplifyPreserveTopology(geom, 2.0)) AS wkt,  -- ≈ 2 meter tolerantie
  ST_XMin(ST_Extent(geom)) AS xmin,
  ST_YMin(ST_Extent(geom)) AS ymin,
  ST_XMax(ST_Extent(geom)) AS xmax,
  ST_YMax(ST_Extent(geom)) AS ymax
FROM externe_bronnen.polygonen_van_grenzen
WHERE naam = 'vrzl' AND geom IS NOT NULL
GROUP BY fid, geom;
"""

WFS_URL       = "https://opendata.enexis.nl/geoserver/wfs"
WFS_SRS       = "EPSG:28992"
WFS_PAGE_SIZE = 10000

DEFAULT_GEOM_ATTR = "geom"

LAYERS = {
    "leiding": {
        "typename":      "Enexis_Opendata:asm_g_leiding",
        "target":        "externe_bronnen.enexis_gasleiding",
        "force_version": "2.0.0",
        "max_start_index": None, # Enexis heeft geen limiet zoals pdok
    },
    "service_leiding": {
        "typename":      "Enexis_Opendata:asm_g_service_leiding",
        "target":        "externe_bronnen.enexis_gasleiding_service",
        "force_version": "1.1.0",   # De service-laag heeft issues met natural order; 1.1.0 vermijdt paging
        "max_start_index": None,   # wordt genegeerd bij 1.1.0
    },
}

# ─────────────────────────────────────────────────────────────────────
# Domeinspecifieke helpers
# ─────────────────────────────────────────────────────────────────────

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

# ─────────────────────────────────────────────────────────────────────
# Data access
# ─────────────────────────────────────────────────────────────────────

def get_polygonen(**_):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = []
    with pg.get_conn() as conn, conn.cursor() as cur:
        cur.execute(POLYGONEN_SQL)
        for poly_id, wkt, xmin, ymin, xmax, ymax in cur.fetchall():
            rows.append({
                "poly_id": poly_id,
                "wkt": wkt,
                "bbox": {
                    "xmin": float(xmin), "ymin": float(ymin),
                    "xmax": float(xmax), "ymax": float(ymax)
                },
            })
    if not rows:
        raise ValueError("Geen polygonen gevonden met naam='vrzl'.")
    return rows

def _flush(pg, sql, template, rows):
    flush_values(pg, sql, template, rows)

# ─────────────────────────────────────────────────────────────────────
# Loader
# ─────────────────────────────────────────────────────────────────────

def load_layer_for_polygon(cfg, poly):
    typename = cfg["typename"]
    target   = cfg["target"]
    version  = cfg.get("force_version", "2.0.0")

    poly_id = poly["poly_id"]
    wkt     = poly["wkt"]

    # Maak CQL met een 'initiele' attribuutnaam; utils corrigeren dit automatisch bij 400 "Illegal property name"
    cql = build_cql_intersects(DEFAULT_GEOM_ATTR, wkt, srs=WFS_SRS)
    print(f"[INFO] Start load {typename} met CQL voor poly_id={poly_id}")
    print(f"[DEBUG] CQL INIT: {cql[:240]}{'...' if len(cql)>240 else ''}")

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
        "%s","%s","%s","%s","%s","%s",
        "%s","%s","%s","%s","%s",
        "%s","%s","%s","%s","%s",
        "%s","%s","%s","%s",
        "%s",
        "ST_SetSRID(ST_GeomFromGeoJSON(%s), 28992)",
        "%s"
    ]) + ")"

    total = 0
    batch = []

    total_hits = wfs_hits(WFS_URL, typename, version, WFS_SRS, cql_filter=cql)
    if total_hits is not None:
        print(f"[INFO] Verwacht {total_hits} features voor {typename} (poly_id={poly_id})")

    for feats in iter_wfs_batches(
        base_url=WFS_URL,
        typename=typename,
        bbox=None,  # Enexis staat bbox + cql niet toe → bbox=None
        version=version,
        srs=WFS_SRS,
        page_size=WFS_PAGE_SIZE,
        output_format="application/json",
        timeout=180,
        extra_params={"cql_filter": cql},
        max_start_index=cfg.get("max_start_index", None), 
    ):
        if not feats:
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

    if batch:
        _flush(pg, insert_sql, template, batch)

    print(f"[INFO] Klaar voor poly_id={poly_id}: {total} features.")

# ─────────────────────────────────────────────────────────────────────
# Airflow orchestration
# ─────────────────────────────────────────────────────────────────────

def clear_existing(**_):
    # Bepaal welke tabellen geleegd moeten worden
    if SELECT_LAYER == "leiding":
        tables = [LAYERS["leiding"]["target"]]
    elif SELECT_LAYER == "service_leiding":
        tables = [LAYERS["service_leiding"]["target"]]
    else:  # "all"
        # Let op: volgorde kan uitmaken bij FK-relaties; begin met kind-tabellen
        tables = [
            LAYERS["service_leiding"]["target"],
            LAYERS["leiding"]["target"],
        ]

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with pg.get_conn() as conn, conn.cursor() as cur:
        for tbl in tables:
            cur.execute(f"DELETE FROM {tbl};")
            print(f"[INFO] Tabel geleegd: {tbl}")
        conn.commit()
    print(f"[INFO] Cleanup voltooid voor SELECT_LAYER='{SELECT_LAYER}'.")

def run_selected_layers(**ctx):
    polygonen = ctx["ti"].xcom_pull(task_ids="get_polygonen")
    if not polygonen:
        print("[WARN] Geen polygonen om te verwerken.")
        return

    if SELECT_LAYER == "leiding":
        layers_to_run = [LAYERS["leiding"]]
    elif SELECT_LAYER == "service_leiding":
        layers_to_run = [LAYERS["service_leiding"]]
    else:
        layers_to_run = [LAYERS["leiding"], LAYERS["service_leiding"]]

    for cfg in layers_to_run:
        for poly in polygonen:
            load_layer_for_polygon(cfg, poly)

default_args = {"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="enexis_gasleidingen",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Airflow 2: je kunt 'schedule' ook gebruiken
    catchup=False,
    tags=["wfs", "geoserver", "postgis", "enexis"],
) as dag:
    t_poly = PythonOperator(task_id="get_polygonen", python_callable=get_polygonen)
    t_load = PythonOperator(task_id="run_selected_layers", python_callable=run_selected_layers, provide_context=True)
    t_clear = PythonOperator(task_id="clear_existing", python_callable=clear_existing)

    t_clear >> t_poly >> t_load

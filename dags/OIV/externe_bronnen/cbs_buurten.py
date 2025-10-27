from __future__ import annotations

from datetime import datetime
import os
import shlex
import subprocess
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ============================================================================
# Config
# ============================================================================
DAG_ID = "cbs_buurten_import"

WFS_BASE = "https://service.pdok.nl/cbs/wijkenbuurten/2023/wfs/v1_0"
TYPENAME = "wijkenbuurten:buurten"
WFS_VERSION = "2.0.0"

# 1e doel (zoals je had)
SCHEMA_GEOSERVER = "externe_bronnen"
TABLE_GEOSERVER = "cbs_buurten"
DST_CONN_ID_GEOSERVER = "geoserver"

# 2e doel (nieuw)
SCHEMA_DWH = "polygonen"
TABLE_DWH = "cbs_buurten"
DST_CONN_ID_DWH = "dwh"

# Zuid-Limburg gemeenten
MUNICIPALITIES: List[str] = [
    "Maastricht", "Sittard-Geleen", "Beek", "Brunssum",
    "Beekdaelen", "Eijsden-Margraten", "Heerlen", "Voerendaal",
    "Kerkrade", "Landgraaf", "Meerssen", "Simpelveld",
    "Stein", "Vaals", "Valkenburg aan de Geul", "Gulpen-Wittem",
]

BATCH_SIZE = 6

# ============================================================================
# Helpers
# ============================================================================
def _get_pg_dsn(conn_id: str) -> str:
    """
    Bouw libpq DSN uit een Airflow Postgres connection.
    Wordt gebruikt als "PG:..." voor ogr2ogr.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    c = hook.get_connection(conn_id)
    parts = [
        f"host={c.host}",
        f"dbname={c.schema}",
        f"user={c.login}",
        f"password={c.password}",
    ]
    if c.port:
        parts.append(f"port={c.port}")
    return " ".join(parts)


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _build_where(names: List[str]) -> str:
    # gemeentenaam = 'X'  OR gemeentenaam = 'Y' ...
    terms = [
        "gemeentenaam = '{}'".format(n.replace("'", "''"))
        for n in names
    ]
    return "(" + " OR ".join(terms) + ")"


def _ogr_import_batchwise(pg_dsn: str, schema: str, table: str) -> None:
    """
    Herbruikbare routine:
    - Loopt alle gemeenten in batches
    - Draait ogr2ogr -append richting opgegeven DB/schema/tabel
    """
    os.environ["PGCLIENTENCODING"] = "UTF8"

    # Let op: GDAL WFS driver verwacht de service root + capabilities
    # (dus niet zelf een GetFeature URL bouwen)
    wfs_ds = (
        f"WFS:{WFS_BASE}?service=WFS&version={WFS_VERSION}&request=GetCapabilities"
    )

    batch_no = 0
    for batch in _chunks(MUNICIPALITIES, BATCH_SIZE):
        batch_no += 1
        where_expr = _build_where(batch)

        cmd = [
            "ogr2ogr",
            "-append",
            "-f", "PostgreSQL", f"PG:{pg_dsn}",
            wfs_ds,
            "-where", where_expr,
            "-nln", f"{schema}.{table}",
            "-lco", "GEOMETRY_NAME=geom",
            "-nlt", "MULTIPOLYGON",
            # CBS levert al RD New (EPSG:28992), dus geen -t_srs nodig
            TYPENAME,
        ]

        print(
            f"[DB={schema}.{table} | BATCH {batch_no}] gemeenten={batch} →",
            " ".join(shlex.quote(x) for x in cmd)
        )

        subprocess.check_call(cmd, env=os.environ.copy())

    print(f"Alle batches succesvol geïmporteerd in {schema}.{table}.")


# ============================================================================
# Tasks
# ============================================================================
def truncate_geoserver(**ctx) -> None:
    """TRUNCATE externe_bronnen.cbs_buurten in geoserver DB."""
    hook = PostgresHook(postgres_conn_id=DST_CONN_ID_GEOSERVER)
    sql = f"TRUNCATE TABLE {SCHEMA_GEOSERVER}.{TABLE_GEOSERVER};"
    hook.run(sql)
    print(f"Table {SCHEMA_GEOSERVER}.{TABLE_GEOSERVER} truncated in {DST_CONN_ID_GEOSERVER}.")


def import_geoserver(**ctx) -> None:
    """Importeer buurten naar geoserver.externe_bronnen.cbs_buurten."""
    pg_dsn = _get_pg_dsn(DST_CONN_ID_GEOSERVER)
    _ogr_import_batchwise(pg_dsn, SCHEMA_GEOSERVER, TABLE_GEOSERVER)


def truncate_dwh(**ctx) -> None:
    """TRUNCATE polygonen.cbs_buurten in dwh DB."""
    hook = PostgresHook(postgres_conn_id=DST_CONN_ID_DWH)
    sql = f"TRUNCATE TABLE {SCHEMA_DWH}.{TABLE_DWH};"
    hook.run(sql)
    print(f"Table {SCHEMA_DWH}.{TABLE_DWH} truncated in {DST_CONN_ID_DWH}.")


def import_dwh(**ctx) -> None:
    """Importeer buurten naar dwh.polygonen.cbs_buurten."""
    pg_dsn = _get_pg_dsn(DST_CONN_ID_DWH)
    _ogr_import_batchwise(pg_dsn, SCHEMA_DWH, TABLE_DWH)


# ============================================================================
# DAG definitie
# ============================================================================
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # alleen handmatig of extern triggeren
    catchup=False,
    tags=["cbs", "wfs", "buurten", "ogr2ogr", "replicate"],
    default_args={"retries": 0},
) as dag:

    t_truncate_geoserver = PythonOperator(
        task_id="truncate_geoserver",
        python_callable=truncate_geoserver,
    )

    t_import_geoserver = PythonOperator(
        task_id="import_geoserver",
        python_callable=import_geoserver,
    )

    t_truncate_dwh = PythonOperator(
        task_id="truncate_dwh",
        python_callable=truncate_dwh,
    )

    t_import_dwh = PythonOperator(
        task_id="import_dwh",
        python_callable=import_dwh,
    )

    # pipeline volgorde
    t_truncate_geoserver >> t_import_geoserver >> t_truncate_dwh >> t_import_dwh

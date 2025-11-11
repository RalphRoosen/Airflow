from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ============================================================================
# Config
# ============================================================================
DAG_ID = "classificeer_inzetten_kazerne"
PG_CONN_ID = "dwh" 

SRC_TABLE = "vdb.inzetten"
DST_TABLE = "vdb.inzetten_kazerne_beroeps_vrijwillig"

# ============================================================================
# SQL
# ============================================================================
CLASSIFY_SQL = f"""
INSERT INTO {DST_TABLE} (
    inzet_eenheid_id, beroeps, beroeps_rv, vrijwillig
)
SELECT
    e.inzet_eenheid_id,

    -- beroeps
    CASE
        WHEN e.kaz_naam IN ('24 Heerlen','24 Sittard West','24 Kerkrade','24 Maastricht Noord') THEN TRUE
        WHEN e.kaz_naam IN ('24 Brunssum','24 Maastricht Zuid') THEN TRUE
        WHEN e.kaz_naam = '24 Beek'
             AND e.dtg_opdracht_inzet::time >= TIME '07:00'
             AND e.dtg_opdracht_inzet::time <  TIME '17:00' THEN TRUE
        WHEN e.kaz_naam = '24 Beek'
             AND (e.dtg_opdracht_inzet::time < TIME '07:00'
                  OR e.dtg_opdracht_inzet::time >= TIME '17:00') THEN FALSE
        ELSE FALSE
    END AS beroeps,

    -- beroeps_rv
    CASE
        WHEN e.kaz_naam IN ('24 Heerlen','24 Sittard West','24 Kerkrade','24 Maastricht Noord') THEN TRUE
        ELSE FALSE
    END AS beroeps_rv,

    -- vrijwillig
    CASE
        WHEN e.kaz_naam IN ('24 Heerlen','24 Sittard West','24 Kerkrade','24 Maastricht Noord',
                             '24 Brunssum','24 Maastricht Zuid') THEN FALSE
        WHEN e.kaz_naam = '24 Beek'
             AND e.dtg_opdracht_inzet::time >= TIME '07:00'
             AND e.dtg_opdracht_inzet::time <  TIME '17:00' THEN FALSE
        WHEN e.kaz_naam = '24 Beek'
             AND (e.dtg_opdracht_inzet::time < TIME '07:00'
                  OR e.dtg_opdracht_inzet::time >= TIME '17:00') THEN TRUE
        ELSE TRUE
    END AS vrijwillig

FROM {SRC_TABLE} e
WHERE NOT EXISTS (
    SELECT 1
    FROM {DST_TABLE} d
    WHERE d.inzet_eenheid_id = e.inzet_eenheid_id
);
"""

# ============================================================================
# Airflow DAG
# ============================================================================
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 0,  # expliciet geen herhalingen
    },
    tags=["vdb", "inzetten", "classificatie"],
) as dag:

    classificeer = PostgresOperator(
        task_id="classificeer_beroeps_vrijwillig",
        postgres_conn_id=PG_CONN_ID,
        sql=CLASSIFY_SQL,
    )

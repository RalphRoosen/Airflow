from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


# Hardcoded tabelnamen
SRC_TABLE_INZETTEN = "vdb.inzetten"
SRC_TABLE_BEZETTING = "vdb.inzetten_voertuig_bezetting_compleet"
SRC_TABLE_WETTIJD = "vdb.incidenten_dekkingsplan_wettelijkeopkomsttijd"
DST_TABLE_INCIDENT_SCORE = "vdb.incidenten_buurtscore_ggo"


# =====================================================================
# SQL: Vul vdb.incident_buurtscore_ggo alleen met NIEUWE incidenten
# (NOT EXISTS toegepast binnen de CTE op het incidentniveau)
# =====================================================================
SQL_INSERT_INCIDENT_BUURTSCORE = f"""
WITH ts4_scores AS (
    --------------------------------------------------------------------
    -- ts4_scores:
    -- Bepaal per incident de score (goed/voldoende/onvoldoende/onbekend)
    -- op basis van de TS-compleet voertuig (vb.compleet = TRUE).
    --
    -- Delta = wettelijkeopkomsttijd - vr_tp_seconden.
    -- Voeg alleen incidenten toe die nog NIET bestaan in
    -- vdb.incident_buurtscore_ggo.
    --------------------------------------------------------------------
    SELECT DISTINCT
        inz.incident_id,
        CASE
            WHEN (w.wettelijkeopkomsttijd - inz.vr_tp_seconden) < -35000 THEN 'Onbekend'
            WHEN (w.wettelijkeopkomsttijd - inz.vr_tp_seconden) <   -180 THEN 'Onvoldoende'
            WHEN (w.wettelijkeopkomsttijd - inz.vr_tp_seconden) <      0 THEN 'Voldoende'
            WHEN (w.wettelijkeopkomsttijd - inz.vr_tp_seconden) <    900 THEN 'Goed'
            ELSE 'Onbekend'
        END AS score
    FROM {SRC_TABLE_BEZETTING} vb
    JOIN {SRC_TABLE_INZETTEN} inz
      ON inz.inzet_eenheid_id = vb.inzet_eenheid_id
    JOIN {SRC_TABLE_WETTIJD} w
      ON w.incident_id = inz.incident_id
    WHERE vb.compleet = TRUE
      AND NOT EXISTS (
          SELECT 1
          FROM {DST_TABLE_INCIDENT_SCORE} dst
          WHERE dst.incident_id = inz.incident_id
      )
)
INSERT INTO {DST_TABLE_INCIDENT_SCORE} (
    incident_id,
    score
)
SELECT
    s.incident_id,
    s.score
FROM ts4_scores s;
"""


# =====================================================================
# DAG-definitie
# =====================================================================
with DAG(
    dag_id="incident_buurtscore_ggo_insert_new_only",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # of bijv. "0 3 * * *"
    catchup=False,
    max_active_runs=1,
    tags=["vdb", "incident", "buurtscore", "ggo", "sql"],
) as dag:

    insert_incident_buurtscore = PostgresOperator(
        task_id="insert_incident_buurtscore_ggo",
        postgres_conn_id="vdb",  # jouw Airflow Postgres connection ID
        sql=SQL_INSERT_INCIDENT_BUURTSCORE,
    )

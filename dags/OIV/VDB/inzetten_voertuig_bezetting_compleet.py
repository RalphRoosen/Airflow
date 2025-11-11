# =====================================================================
# DAG: vdb_inzetten_voertuig_bezetting_compleet
# =====================================================================
# Doel:
#   Repliceert de DAX-logica van TS4-compleetbepaling in SQL.
#   Bepaalt:
#     - per voertuig het aantal personen (uit vdb.inzetten_voertuig_aantal_personen)
#     - per incident de cumulatieve som tot > 5 personen
#     - het voertuig dat die grens bereikt (compleet = TRUE)
#     - vult dit alles in vdb.inzetten_voertuig_bezetting_compleet
#
# =====================================================================

from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# =====================================================================
# Config
# =====================================================================
PG_CONN_ID = "dwh"
DAG_ID = "vdb_inzetten_voertuig_bezetting_compleet"

SRC_TABLE_INZETTEN = "vdb.inzetten"
SRC_TABLE_AANTAL = "vdb.inzetten_voertuig_aantal_personen"
SRC_TABLE_VOERTUIG_AANKOMST = "vdb.inzetten_voertuig_aankomst"
DST_TABLE = "vdb.inzetten_voertuig_bezetting_compleet"

# =====================================================================
# SQL: volledige herbouw van vdb.inzetten_voertuig_bezetting_compleet
# =====================================================================

SQL_BUILD_BEZETTING = f"""
-- ==========================================================================
-- Vul vdb.inzetten_voertuig_bezetting_compleet op basis van alle inzetten.
-- Logica:
--   1. Verzamel per voertuig (inzet) de incident_id, volgorde en aantal personen.
--   2. Bereken cumulatieve som (cum_aantal) per incident in volgorde van aankomst.
--   3. Bepaal per incident de eerste volgorde waarbij cum_aantal > 5 (TS4 compleet).
--   4. Zet 'compleet = TRUE' voor dat voertuig; FALSE voor alle anderen.
--   5. Voeg alleen nog niet-bestaande inzet_eenheid_id's toe.
-- ==========================================================================

WITH per_voertuig AS (
    ----------------------------------------------------------------------
    -- Stap 1: basisgegevens per inzet (voertuig)
    ----------------------------------------------------------------------
    SELECT
        i.inzet_eenheid_id,
        i.incident_id,
        v.volgorde,
        COALESCE(a.aantal, 0) AS aantal
    FROM {SRC_TABLE_INZETTEN} i
    JOIN {SRC_TABLE_VOERTUIG_AANKOMST} v
      ON v.inzet_eenheid_id = i.inzet_eenheid_id
    LEFT JOIN {SRC_TABLE_AANTAL} a
      ON a.inzet_eenheid_id = i.inzet_eenheid_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM {DST_TABLE} b
        WHERE b.inzet_eenheid_id = i.inzet_eenheid_id
    )
),
met_cumulatief AS (
    ----------------------------------------------------------------------
    -- Stap 2: bereken per incident cumulatieve som van 'aantal'
    ----------------------------------------------------------------------
    SELECT
        pv.*,
        SUM(pv.aantal) OVER (
            PARTITION BY pv.incident_id
            ORDER BY pv.volgorde
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_aantal
    FROM per_voertuig pv
),
eerste_compleet_per_incident AS (
    ----------------------------------------------------------------------
    -- Stap 3: bepaal per incident de eerste volgorde waar cum_aantal > 5
    ----------------------------------------------------------------------
    SELECT
        incident_id,
        MIN(volgorde) AS hoeveelste_voertuig
    FROM met_cumulatief
    WHERE cum_aantal > 5
    GROUP BY incident_id
)
-- ======================================================================
-- Stap 4: insert in doel-tabel
-- ======================================================================
INSERT INTO {DST_TABLE} (
    inzet_eenheid_id,
    compleet, 
    hoeveelste_voertuig
)
SELECT
    mc.inzet_eenheid_id,
    CASE
        WHEN ec.hoeveelste_voertuig IS NOT NULL
         AND mc.volgorde = ec.hoeveelste_voertuig THEN TRUE -- dit voertuig maakt TS compleet
        ELSE FALSE
    END AS compleet,
    COALESCE(ec.hoeveelste_voertuig, NULL)::smallint AS hoeveelste_voertuig
FROM met_cumulatief mc
LEFT JOIN eerste_compleet_per_incident ec
  ON ec.incident_id = mc.incident_id;
"""

# =====================================================================
# DAG-definitie
# =====================================================================

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # handmatig draaien
    catchup=False,
    max_active_runs=1,
    tags=["vdb", "inzetten", "bezetting", "ts4"],
) as dag:

    build_bezetting = PostgresOperator(
        task_id="build_inzetten_voertuig_bezetting_compleet",
        postgres_conn_id=PG_CONN_ID,
        sql=SQL_BUILD_BEZETTING,
    )

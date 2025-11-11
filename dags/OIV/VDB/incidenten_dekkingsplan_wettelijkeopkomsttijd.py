from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ============================================================================
# Config
# ============================================================================
DAG_ID = "load_wettelijke_opkomsttijd"
CONN_ID = "dwh"

SRC_AO = "vdb.dekkingsplan_aandachtsobject"
SRC_CBS_BUURT = "polygonen.cbs_buurten_uniek"
SRC_BUURTRAPPORT = "vdb.view_dekkingsplan_buurtenrapport_uniek"
TARGET = "vdb.incidenten_dekkingsplan_wettelijkeopkomsttijd"

# ============================================================================
# Task 1: buurtrapport-categorie vullen
# - pak per incident_id de buurt uit vdb.incidenten_cbs_buurt_wijk
# - join op public.dekkingsplan_buurtenrapport_anw_ts4 via bu_naam
# - schrijf categorie naar vdb.dekkingsplan_wettelijkeopkomsttijd.buurtenrapport_categorie
# ============================================================================
SQL_SET_BUURTRAPPORT_CATEGORIE = f"""
UPDATE {TARGET} tgt
SET buurtenrapport_categorie = br.categorie
FROM {SRC_CBS_BUURT} cbs
JOIN {SRC_BUURTRAPPORT} br
  ON br.bu_naam = cbs.buurt
WHERE tgt.incident_id = cbs.incident_id AND
buurtenrapport_categorie IS NULL;
"""

# ============================================================================
# Task 2: wettelijke_opkomsttijd bepalen
# aandachtsobject = true -> altijd tijd van categorie 1
# else -> tijd is de tijd die bij de buurt hoort (deze kolom nog toevoegen uit buurtrapport)

SQL_WETTELIJKE_OPKOMSTTIJD = f"""
UPDATE {TARGET} w
SET wettelijkeopkomsttijd = CASE
    WHEN w.buurtenrapport_categorie = 1 
            OR a.aandachtsobject = TRUE THEN 7 * 60
    WHEN w.buurtenrapport_categorie = 2 THEN 10 * 60
    WHEN w.buurtenrapport_categorie = 3 THEN 15 * 60
    ELSE w.wettelijkeopkomsttijd
END
FROM {SRC_AO} a
WHERE w.incident_id = a.incident_id AND
wettelijkeopkomsttijd IS NULL;
"""

# ============================================================================

# ============================================================================
# DAG
# ============================================================================
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dekkingsplan", "adresmatch", "aandachtsobject", "buurtrapport"],
) as dag:

    t1_set_buurtcat = PostgresOperator(
        task_id="set_buurtenrapport_categorie",
        postgres_conn_id=CONN_ID,
        sql=SQL_SET_BUURTRAPPORT_CATEGORIE,
    )

    t2_wettelijke_opkomsttijd = PostgresOperator(
        task_id="set_wettelijke_opkomsttijd",
        postgres_conn_id=CONN_ID,
        sql=SQL_WETTELIJKE_OPKOMSTTIJD,
    )

    t1_set_buurtcat >> t2_wettelijke_opkomsttijd

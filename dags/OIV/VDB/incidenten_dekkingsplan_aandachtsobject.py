from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ============================================================================
# Config
# ============================================================================
DAG_ID = "load_aandachtsobject"
CONN_ID = "dwh"

SRC_INC = "vdb.incidenten"
SRC_ANW = "public.dekkingsplan_objectselectie_anw"
TARGET = "vdb.incidenten_dekkingsplan_aandachtsobject"

# ============================================================================
# Task 1: incidenten → objectselectie_adresmatch + objectselectie_omschrijving
# alleen incidenten die nog NIET in target staan
# ============================================================================
SQL_FILL_OBJECTSELECTIE = f"""
WITH inc AS (
    SELECT
        i.incident_id,
        lower(replace(i.postcode, ' ', '')) AS pc_norm,
        i.huis_paal_nr AS huisnummer
    FROM {SRC_INC} i
    WHERE NOT EXISTS (
        SELECT 1
        FROM {TARGET} t
        WHERE t.incident_id = i.incident_id
    )
),
joined AS (
    SELECT DISTINCT ON (inc.incident_id)
        inc.incident_id,
        CASE
            WHEN anw.omschrijving IS NOT NULL THEN true
            ELSE false
        END AS objectselectie_adresmatch,
        COALESCE(anw.omschrijving, '') AS objectselectie_omschrijving
    FROM inc
    LEFT JOIN {SRC_ANW} AS anw
      ON lower(replace(anw.postcode, ' ', '')) = inc.pc_norm
     AND anw.huisnummer::integer = inc.huisnummer
    ORDER BY
        inc.incident_id,
        anw.omschrijving  -- kiest 1 omschrijving bij meerdere matches
)
INSERT INTO {TARGET} (
    incident_id,
    objectselectie_adresmatch,
    objectselectie_omschrijving
)
SELECT
    j.incident_id,
    j.objectselectie_adresmatch,
    j.objectselectie_omschrijving
FROM joined j;
"""

# ============================================================================
# Task 2: aandachtsobject + aandachtsobject_onbekend bepalen
# ============================================================================
SQL_SET_AANDACHTSOBJECT = f"""
-- geen adresmatch → onbekend = true, aandachtsobject = false
UPDATE {TARGET}
SET
    aandachtsobject = false,
    aandachtsobject_onbekend = true
WHERE objectselectie_adresmatch = false;

-- wél adresmatch → onbekend = false
UPDATE {TARGET}
SET
    aandachtsobject_onbekend = false
WHERE objectselectie_adresmatch = true;

-- wél adresmatch én omschrijving in lijst → aandachtsobject = true
UPDATE {TARGET}
SET
    aandachtsobject = true
WHERE objectselectie_adresmatch = true
  AND lower(objectselectie_omschrijving) IN (
        'celfunctie',
        'gebouwen voor niet-zelfredzame personen',
        'woonfunctie portiekflats',
        'woonfunctie portiekwoningen',
        'woongebouw hoger dan 20 meter'
  );

-- wél adresmatch maar NIET in lijst → aandachtsobject = false
UPDATE {TARGET}
SET
    aandachtsobject = false
WHERE objectselectie_adresmatch = true
  AND lower(objectselectie_omschrijving) NOT IN (
        'celfunctie',
        'gebouwen voor niet-zelfredzame personen',
        'woonfunctie portiekflats',
        'woonfunctie portiekwoningen',
        'woongebouw hoger dan 20 meter'
  );
"""

# ============================================================================
# DAG definitie
# ============================================================================
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dekkingsplan", "adresmatch", "aandachtsobject", "buurtrapport"],
) as dag:

    t1_fill_objectselectie = PostgresOperator(
        task_id="fill_objectselectie_adresmatch",
        postgres_conn_id=CONN_ID,
        sql=SQL_FILL_OBJECTSELECTIE,
    )

    t2_set_flags = PostgresOperator(
        task_id="set_aandachtsobject_flags",
        postgres_conn_id=CONN_ID,
        sql=SQL_SET_AANDACHTSOBJECT,
    )

    t1_fill_objectselectie >> t2_set_flags

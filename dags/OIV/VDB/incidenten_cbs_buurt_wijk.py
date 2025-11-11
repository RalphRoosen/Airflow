from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_ID = "load_incidenten_cbs_buurt_wijk"
CONN_ID = "dwh"

SRC_INC_TABLE = "vdb.incidenten_coordinaten_amersfoort_rdnew"  # heeft: incident_id, geom (28992)
CBS_BUURTEN = "polygonen.cbs_buurten"                       # heeft: buurtcode, wijkcode, geom (28992)
CBS_WIJKEN = "polygonen.cbs_wijken"                         # heeft: wijkcode, wijknaam, gemeentenaam (28992)
TARGET_TABLE = "vdb.incidenten_cbs_buurt_wijk"              # doel: incident_id, buurt, wijk, gemeente

SQL_LOAD = f"""

-- 1) pak alle incidenten mét geom die nog niet in de doel-tabel staan
WITH incidenten_nog_niet_gekoppeld AS (
    SELECT i.incident_id,
           i.geom
    FROM {SRC_INC_TABLE} i
    WHERE NOT EXISTS (
        SELECT 1
        FROM {TARGET_TABLE} t
        WHERE t.incident_id = i.incident_id
    )
),

-- 2) ruimtelijke join met CBS-buurten
incidenten_met_buurt AS (
    SELECT
        inc.incident_id,
        b.buurtnaam,
        b.wijkcode
    FROM incidenten_nog_niet_gekoppeld inc
    JOIN {CBS_BUURTEN} b
      ON inc.geom && b.geom AND ST_Intersects(b.geom, inc.geom)
),

-- 3) join met CBS-wijken op wijkcode om namen te pakken
incidenten_met_wijk AS (
    SELECT
        ib.incident_id,
        ib.buurtnaam,
        w.wijknaam,
        w.gemeentenaam
    FROM incidenten_met_buurt ib
    LEFT JOIN {CBS_WIJKEN} w
      ON w.wijkcode = ib.wijkcode
)
-- 4) schrijf weg naar doel
INSERT INTO {TARGET_TABLE} (incident_id, buurt, wijk, gemeente)
SELECT
    incident_id,
    buurtnaam      AS buurt,
    wijknaam       AS wijk,
    gemeentenaam   AS gemeente
FROM incidenten_met_wijk;
"""

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,           # mag je op @hourly / @daily zetten
    catchup=False,
    tags=["vdb", "gis", "cbs", "spatial", "etl"],
) as dag:

    load_incidenten_cbs_buurt_wijk = PostgresOperator(
        task_id="load_incidenten_cbs_buurt_wijk",
        postgres_conn_id=CONN_ID,
        sql=SQL_LOAD,
    )

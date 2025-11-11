from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ============================================================================
# Configuratie
# ============================================================================
DAG_ID = "load_incidentcoordinaten"
CONN_ID = "dwh"  
SCHEMA = "vdb"

TABLE_RD = "incidenten_coordinaten_amersfoort_rdnew"
TABLE_WGS = "incidenten_coordinaten_wgs84"

# ============================================================================
# 1) Alleen nieuwe RD NEW records laden
#    (alleen incidenten die wél x/y hebben in vdb.incidenten,
#     maar nog NIET in vdb.incidentcoordinaten_amersfoort_rdnew staan)
# ============================================================================
SQL_INSERT_RD = f"""
INSERT INTO {SCHEMA}.{TABLE_RD} (incident_id, geom)
SELECT
    src.incident_id,
    ST_SetSRID(ST_MakePoint(src.t_x_coord_loc, src.t_y_coord_loc), 28992) AS geom
FROM {SCHEMA}.incidenten AS src
WHERE src.t_x_coord_loc IS NOT NULL
  AND src.t_y_coord_loc IS NOT NULL
  AND NOT EXISTS (
        SELECT 1
        FROM {SCHEMA}.{TABLE_RD} AS tgt
        WHERE tgt.incident_id = src.incident_id
  );
"""

# ============================================================================
# 2) Alleen nieuwe WGS84 records laden
#    zelfde bronfilter, maar nu tegen vdb.incidentcoordinaat_wgs84 checken
# ============================================================================
SQL_INSERT_WGS = f"""
INSERT INTO {SCHEMA}.{TABLE_WGS} (incident_id, lon, lat, geom)
SELECT
    src.incident_id,
    ST_X(wgs84_geom) AS lon,
    ST_Y(wgs84_geom) AS lat,
    wgs84_geom       AS geom
FROM (
    SELECT
        incident_id,
        ST_Transform(
            ST_SetSRID(
                ST_MakePoint(t_x_coord_loc, t_y_coord_loc),
                28992
            ),
            4326
        ) AS wgs84_geom
    FROM {SCHEMA}.incidenten
    WHERE t_x_coord_loc IS NOT NULL
      AND t_y_coord_loc IS NOT NULL
) AS src
WHERE NOT EXISTS (
    SELECT 1
    FROM {SCHEMA}.{TABLE_WGS} AS tgt
    WHERE tgt.incident_id = src.incident_id
);
"""

# ============================================================================
# DAG-definitie
# ============================================================================
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,     # je kunt hier gewoon cron op zetten
    catchup=False,
    tags=["vdb", "gis", "etl", "incremental"],
) as dag:

    load_rd = PostgresOperator(
        task_id="insert_new_incidentcoordinaten_rdnew",
        postgres_conn_id=CONN_ID,
        sql=SQL_INSERT_RD,
    )

    load_wgs = PostgresOperator(
        task_id="insert_new_incidentcoordinaat_wgs84",
        postgres_conn_id=CONN_ID,
        sql=SQL_INSERT_WGS,
    )

    # kan parallel, maar in volgorde is vaak duidelijker
    load_rd >> load_wgs

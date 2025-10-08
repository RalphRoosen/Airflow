# Ga naar Containers (verticale menu helemaal links) --> apacheairflow-aiflow-webserver 
# Rechter muisklik --> attach shell 
# Nieuwe terminal opent
# run het volgende : airflow dags test OIV_dwh_geo_sync_incidenten_locaties

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from OIV.dwh_geo_sync_wegmoffelen import load_batches, WHERE_CLAUSE, DEFAULT_START, DST_CONN_ID

"""
De tabellen buurten_categorien (van CARE dekkingsplan) en cbs_buurten hebben enkele handmatige aanpassingen gehad
Buurt Euverem, Pesaken, Billinghuizen en Waterop kwam textueel niet overeen en daarom aangepast

De combinatie buurt, gemeente levert enkele niet unieke waarden op
Verspreide huizen Gulpen-Wittem
Verspreide huizen Eijsden-Margraten
Hommert (gedeeltelijk) Beekdaelen
Verspreide huizen Beekdaelen
Verspreide huizen Meerssen

Daarom handmatig aangepast (zie sql onderaan): 
Verspreide huizen Gulpen-Wittem 1, Verspreide huizen Gulpen-Wittem 2, Verspreide huizen Gulpen-Wittem 3, etc
"""

SELECT_SQL = """SELECT
            i.lms_incident_id as incident_id,
            a.incidentadres,
            i.vr_lon,
            i.vr_lat
        FROM
            inci_vr_incident AS i
        JOIN
            view_incidentadres AS a
            ON i.lms_incident_id = a.lms_incident_id
        """ + ' ' + WHERE_CLAUSE

INSERT_SQL = """
        INSERT INTO datawarehouse.incidenten_locatie (
            incident_id,
            incidentadres,
            geom
        )
        VALUES %s
        ON CONFLICT (incident_id) DO NOTHING
    """

def transfer_incident_data():
    dst = PostgresHook(postgres_conn_id=DST_CONN_ID)
    with dst.get_conn() as dst_conn, dst_conn.cursor() as cur:
        cur.execute("""
                        SELECT COALESCE(MAX(i.lms_brw_dtg_start_incident), %s)
                        FROM datawarehouse.incidenten_locatie AS e
                        JOIN datawarehouse.incidenten AS i
                        ON e.incident_id = i.lms_incident_id
                    """, (DEFAULT_START,))
        last_date = cur.fetchone()[0]

    if last_date == DEFAULT_START:
        load_batches(SELECT_SQL,
                     INSERT_SQL,
                     create_geom_puntcoordinaat=True)
    else:
        load_batches(SELECT_SQL + "\n  AND lms_brw_dtg_start_incident > %s",
                     INSERT_SQL,
                     datum_parameter=last_date,
                     create_geom_puntcoordinaat=True)

def update_buurt():
    dst = PostgresHook(postgres_conn_id=DST_CONN_ID)
    with dst.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE datawarehouse.incidenten_locatie AS p
            SET 
                buurt    = b.buurtnaam,
                gemeente = b.gemeentenaam
            FROM externe_bronnen.cbs_buurten AS b
            WHERE p.buurt IS NULL
            AND p.gemeente IS NULL
            AND p.geom IS NOT NULL
            AND p.geom && b.geom            -- bbox filter (sneller)
            AND ST_Intersects(p.geom, b.geom);
        """)
        conn.commit()

default_args = {
    'owner': 'OIV',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=5) ,
}

with DAG(
    dag_id='OIV_dwh_geo_sync_incidenten_locaties',
    default_args=default_args,
    # elke dag om 06:30
    schedule='30 6 * * *',
    start_date=datetime(2019, 1, 1),
    catchup=False,
) as dag:

    transfer_task = PythonOperator(
        task_id='transfer_incident_data',
        python_callable=transfer_incident_data,
    )

    update_buurt_task = PythonOperator(
        task_id='update_buurt_op_basis_van_cbs',
        python_callable=update_buurt,
    )

    transfer_task >> update_buurt_task


"""
BEGIN;

-- A1) cbs_buurten → hernoem dubbelen
WITH d AS (
  SELECT ctid AS rowid, buurtnaam, gemeentenaam,
         ROW_NUMBER() OVER (PARTITION BY gemeentenaam, buurtnaam ORDER BY ctid) AS rn,
         COUNT(*) OVER (PARTITION BY gemeentenaam, buurtnaam) AS cnt
  FROM externe_bronnen.cbs_buurten
  WHERE buurtnaam IN ('Verspreide huizen', 'Hommert (gedeeltelijk)')
)
UPDATE externe_bronnen.cbs_buurten cb
SET buurtnaam = CONCAT(d.buurtnaam, ' ', d.gemeentenaam, ' ', d.rn)
FROM d
WHERE cb.ctid = d.rowid
  AND d.cnt > 1;

-- A2) buurten_categorieen → hernoem dubbelen
WITH d AS (
  SELECT ctid AS rowid, bu_naam, gm_naam,
         ROW_NUMBER() OVER (PARTITION BY gm_naam, bu_naam ORDER BY ctid) AS rn,
         COUNT(*) OVER (PARTITION BY gm_naam, bu_naam) AS cnt
  FROM externe_bronnen.buurten_categorieen
  WHERE bu_naam IN ('Verspreide huizen', 'Hommert (gedeeltelijk)')
)
UPDATE externe_bronnen.buurten_categorieen bc
SET bu_naam = CONCAT(d.bu_naam, ' ', d.gm_naam, ' ', d.rn)
FROM d
WHERE bc.ctid = d.rowid
  AND d.cnt > 1;

COMMIT;

"""

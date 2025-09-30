
# Ga naar Docker --> apacheairflow-aiflow-webserver 
# Rechter muisklik --> attach shell 
# Nieuwe terminal opent
# run het volgende : airflow dags test OIV_dwh_geo_sync_inzetten

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from OIV.dwh_geo_sync_wegmoffelen import load_batches, WHERE_CLAUSE, DEFAULT_START, DST_CONN_ID


SELECT_SQL = """SELECT
            e.inzet_eenheid_id,
            e.incident_id,
            e.kaz_naam,
            e.roepnaam_eenheid,
            e.rol,
            e.code_voertuigsoort,
            e.dtg_opdracht_inzet,
            e.dtg_ontkoppel,
            vr_dtg_ut,
            vr_dtg_tp
        FROM
            inci_vr_inzet_eenheid AS e
        JOIN
            inci_vr_incident AS i
            ON i.lms_incident_id = e.incident_id
        """ + ' ' + WHERE_CLAUSE

INSERT_SQL = """
        INSERT INTO datawarehouse.inzetten_eenheden (
                                inzet_eenheid_id,
                                incident_id,
                                kaz_naam,
                                roepnaam_eenheid,
                                rol,
                                code_voertuigsoort,
                                dtg_opdracht_inzet,
                                dtg_ontkoppel,
                                vr_dtg_ut,
                                vr_dtg_tp
        )
        VALUES %s
        ON CONFLICT (inzet_eenheid_id) DO NOTHING
            """

def transfer_incident_data():
    dst = PostgresHook(postgres_conn_id=DST_CONN_ID)
    with dst.get_conn() as dst_conn, dst_conn.cursor() as cur:
        cur.execute("""
                        SELECT COALESCE(MAX(i.lms_brw_dtg_start_incident), %s)
                        FROM datawarehouse.inzetten_eenheden AS e
                        JOIN datawarehouse.incidenten AS i
                        ON e.incident_id = i.lms_incident_id
                    """, (DEFAULT_START,))
        last_date = cur.fetchone()[0]

    if last_date == DEFAULT_START:
        load_batches(SELECT_SQL, 
                     INSERT_SQL)
    else:
        load_batches(SELECT_SQL + "\n  AND i.lms_brw_dtg_start_incident > %s",
                         INSERT_SQL,
                         datum_parameter=last_date)

default_args = {
    'owner': 'OIV',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='OIV_dwh_geo_sync_inzetten',
    default_args=default_args,
    # elke dag om 06:20
    schedule='20 6 * * *',
    start_date=datetime(2019, 1, 1),
    catchup=False,
) as dag:

    transfer_task = PythonOperator(
        task_id='transfer_incident_data',
        python_callable=transfer_incident_data,
    )

    transfer_task


# Ga naar Docker --> apacheairflow-aiflow-webserver 
# Rechter muisklik --> attach shell 
# Nieuwe terminal opent
# run het volgende : airflow dags test OIV_dwh_geo_sync_incidenten

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from OIV.dwh_geo_sync_wegmoffelen import load_batches, WHERE_CLAUSE, DEFAULT_START, DST_CONN_ID

SELECT_SQL = """
    SELECT 
        lms_incident_id,
        lms_brw_dtg_start_incident,
        lms_brw_dtg_einde_incident,
        lms_naam_gebeurtenis,
        lms_brw_melding_cl,
        lms_brw_melding_cl1,
        lms_brw_melding_cl2,
        lms_prioriteit_incident_brandweer,
        lms_brw_soort_afsluiting,
        lms_nr_incident
    FROM inci_vr_incident
    """ + ' ' + WHERE_CLAUSE

INSERT_SQL = """
    INSERT INTO datawarehouse.incidenten (
        lms_incident_id,
        lms_brw_dtg_start_incident,
        lms_brw_dtg_einde_incident,
        lms_naam_gebeurtenis,
        lms_brw_melding_cl,
        lms_brw_melding_cl1,
        lms_brw_melding_cl2,
        lms_prioriteit_incident_brandweer,
        lms_brw_soort_afsluiting,
        lms_nr_incident
    )
    VALUES %s
    ON CONFLICT (lms_incident_id) DO NOTHING
    """

def transfer_incident_data():
    dst = PostgresHook(postgres_conn_id=DST_CONN_ID)
    with dst.get_conn() as dst_conn, dst_conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(MAX(lms_brw_dtg_start_incident), %s)
            FROM datawarehouse.incidenten
        """, (DEFAULT_START,))
        last_date = cur.fetchone()[0]

    if last_date == DEFAULT_START:
        load_batches(SELECT_SQL, 
                     INSERT_SQL)
    else:
            load_batches(SELECT_SQL + "\n  AND lms_brw_dtg_start_incident > %s",
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
    dag_id='OIV_dwh_geo_sync_incidenten',
    default_args=default_args,
    # elke dag om 06:00
    schedule='0 6 * * *',
    start_date=datetime(2019, 1, 1),
        access_control={
        "Op": {"can_read", "can_edit"},
        "Admin": {"can_read", "can_edit"},
    },
    catchup=False,
) as dag:

    transfer_task = PythonOperator(
        task_id='transfer_incident_data',
        python_callable=transfer_incident_data,
    )

    transfer_task

# Ga naar Docker --> apacheairflow-aiflow-webserver 
# Rechter muisklik --> attach shell 
# Nieuwe terminal opent
# run het volgende : airflow dags test OIV_dwh_geo_sync_incidenten_nr

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from OIV.dwh_geo_sync_wegmoffelen import load_batches, WHERE_CLAUSE

SELECT_SQL = """
    SELECT 
        lms_incident_id,
        lms_nr_incident::BIGINT
    FROM inci_vr_incident 
    """ + ' ' + WHERE_CLAUSE + ' order by lms_brw_dtg_start_incident'

UPDATE_SQL = f"""
            UPDATE datawarehouse.incidenten AS d
            SET    lms_nr_incident = v.lms_nr_incident
            FROM (
                VALUES %s
            ) AS v(lms_incident_id, lms_nr_incident)
            WHERE d.lms_incident_id = v.lms_incident_id
            AND d.lms_nr_incident IS NULL
            AND (d.lms_nr_incident IS DISTINCT FROM v.lms_nr_incident);
        """

def transfer_incident_data():
    load_batches(SELECT_SQL, UPDATE_SQL)

default_args = {
    'owner': 'OIV',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=5),
}

#with DAG(
#    dag_id='OIV_dwh_geo_update_bij_aanpassing_table',
#    default_args=default_args,
#    schedule=None,
#    start_date=datetime(2019, 1, 1),
#    catchup=False,
#) as dag:
#
#    transfer_task = PythonOperator(
#        task_id='transfer_incident_data',
#        python_callable=transfer_incident_data,
#    )
#
#    transfer_task

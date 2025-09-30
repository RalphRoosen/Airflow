# Ga naar Docker --> apacheairflow-aiflow-webserver 
# Rechter muisklik --> attach shell 
# Nieuwe terminal opent
# run het volgende : airflow dags test OIV_dwh_geo_update_bij_aanpassing_table2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from OIV.dwh_geo_sync_wegmoffelen import load_batches, WHERE_CLAUSE

SELECT_SQL = """
            SELECT
            e.inzet_eenheid_id,
            e.incident_id,
            e.vr_dtg_ut,
            e.vr_dtg_tp
            FROM inci_vr_inzet_eenheid AS e
            JOIN inci_vr_incident AS i
            ON i.lms_incident_id = e.incident_id
            """ + ' ' + WHERE_CLAUSE

UPDATE_SQL = """
            UPDATE datawarehouse.inzetten_eenheden AS d
            SET
            vr_dtg_ut = v.vr_dtg_ut,
            vr_dtg_tp = v.vr_dtg_tp
            FROM (
            VALUES %s
            ) AS v(inzet_eenheid_id, incident_id, vr_dtg_ut, vr_dtg_tp)
            WHERE d.inzet_eenheid_id = v.inzet_eenheid_id
            AND d.incident_id = v.incident_id
            AND d.vr_dtg_ut IS NULL AND d.vr_dtg_tp IS NULL;
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
##    dag_id='OIV_dwh_geo_update_bij_aanpassing_table2',
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

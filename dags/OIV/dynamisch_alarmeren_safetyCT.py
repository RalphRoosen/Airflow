from datetime import datetime, timedelta
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from OIV.dwh_geo_sync_wegmoffelen import SRC_CONN_ID
from OIV.dynamisch_alarmeren import get_oauth_token

def sync_incidenten(**context) -> None:
    hook = PostgresHook(postgres_conn_id=SRC_CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(startdtg) FROM public.dynamisch_alarmeren_safetyct")
            result = cursor.fetchone()
            latest_startdtg = result[0]

    # VOLGENS MIJ IS API KAPUT EN DOET DATUM PARAMETER HET NIET.. kRIJG ALLE BESCHIKBARE INCIDENTEN
    # (STRING IS IN ISO-FORMAT ZOALS HET HOORT)
    # OPGELOST DOOR ON CONFLICT TE GEBRUIKEN BIJ INSERT MET UNIQUE CONSTRAINT OP ID
    if latest_startdtg:
        start_date_str = (latest_startdtg + timedelta(seconds=1)).strftime("%Y%m%dT%H%M%S")
        print("____________SINDS LAATSTE DATUM______________")
    else:
        start_date_str = "20250101T000000"
        print("____________VOLLEDIG______________")

    url = (
        f'https://caredrive.safetyct.com/api/public/incidenten'
        f'?includeGearchiveerd=true&onlyWithBetrokkenEenheden=false&start_date={start_date_str}'
    )

    headers = {
        'Authorization': f'Bearer {get_oauth_token()}',
        'Accept': 'application/json'
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise ValueError("API response is geen lijst van incidenten.")

    values = [
        (
            incident.get('id'),
            incident.get('nummer'),
            incident.get('status'),
            incident.get('startDtg'),
            incident.get('prioriteit'),
            incident.get('meldingsclassificatie1'),
            incident.get('meldingsclassificatie2'),
            incident.get('meldingsclassificatie3'),
            incident.get('naamLocatie'),
            incident.get('plaatsnaam'),
            incident.get('gemeentenaam'),
            incident.get('betrokkenEenheden', [])
        )
        for incident in data
    ]

    if not values:
        return

    sql = """
        INSERT INTO public.dynamisch_alarmeren_safetyct (
            ID, nummer, status, startdtg, prioriteit,
            melding1, melding2, melding3,
            locatie, plaatsnaam, gemeente,
            betrokken_eenheden
        ) VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, sql, values, page_size=2000)
        conn.commit()

with DAG(
    dag_id='OIV_dynAlarm_safetyCT',
    schedule='0 2 * * *', # 2 uur snachts
    start_date=datetime(2022, 1, 1),
    max_active_runs=1,
    catchup=False
) as dag:

    sync_incidenten_task = PythonOperator(
        task_id='sync_incidenten',
        python_callable=sync_incidenten,
        provide_context=True
    )

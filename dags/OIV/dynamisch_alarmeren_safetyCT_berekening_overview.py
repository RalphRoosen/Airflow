from datetime import datetime, timedelta
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from OIV.dwh_geo_sync_wegmoffelen import SRC_CONN_ID
from OIV.dynamisch_alarmeren import get_oauth_token, get_api_response
import json

def sync_incidenten(**context) -> None:
    hook = PostgresHook(postgres_conn_id=SRC_CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute( # join met basis table om enkel incidenten te halen die nieuw zijn?
                """SELECT
                    s.id
                    FROM
                    public.dynamisch_alarmeren_safetyct AS s
                    LEFT JOIN
                    public.dynamisch_alarmeren_safetyct_berekeningenoverview AS o
                        ON s.id = o.incident_id
                    WHERE
                    o.incident_id IS NULL;
                """
            )
            result = cursor.fetchall()
            incidenten = [row[0] for row in result]

    headers = {
        'Authorization': f'Bearer {get_oauth_token()}',
        'Accept': 'application/json'
    }

    alle_berekeningen = []
    for incident_id in incidenten:
        url = (
            f"https://caredrive.safetyct.com/api/incidenten/{incident_id}/berekeningen"
            f"?includeOnlyPublished=false&onlyWithBetrokkenEenheden=false"
        )
        data = get_api_response(url, headers)

        berekeningen = [
            (
                b["berekeningId"],
                incident_id,
                b["berekeningTrigger"],
                b["tijdstipBerekend"],
                b["isArtGepubliceerd"],
                b["isKladblokGepubliceerd"],
                b["momentopname"],
                json.dumps(b.get("ongedekteInzetbehoeften", {})),
                b.get("betrokkenEenheden", [])
            )
            for b in data
        ]

        alle_berekeningen.extend(berekeningen)

    sql = """
        INSERT INTO public.dynamisch_alarmeren_safetyct_berekeningenoverview (
            berekening_id,
            incident_id,
            berekening_trigger,
            tijdstip_berekend,
            is_art_gepubliceerd,
            is_kladblok_gepubliceerd,
            momentopname,
            ongedekte_inzetbehoeften,
            betrokken_eenheden
        )
        VALUES %s
        ON CONFLICT (berekening_id) DO NOTHING;
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, sql, alle_berekeningen, page_size=2000)
        conn.commit()

with DAG(
    dag_id='OIV_dynAlarm_safetyCT_berekeningen_overview',
    schedule='0 3 * * *', # 2 uur snachts
    start_date=datetime(2022, 1, 1),
    max_active_runs=1,
    catchup=False
) as dag:

    sync_incidenten_task = PythonOperator(
        task_id='sync_incidenten',
        python_callable=sync_incidenten,
        provide_context=True
    )

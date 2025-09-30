from datetime import datetime
import json
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from OIV.dwh_geo_sync_wegmoffelen import SRC_CONN_ID
from OIV.dynamisch_alarmeren import get_oauth_token, get_api_response

def sync_incidenten(**context) -> None:
    hook = PostgresHook(postgres_conn_id=SRC_CONN_ID)
    sql_berekening_ids = """
                    SELECT o.berekening_id
                    FROM
                        public.dynamisch_alarmeren_safetyct_berekeningenoverview AS o
                    LEFT JOIN
                        public.dynamisch_alarmeren_safetyct_detailsoverview AS a
                        ON o.berekening_id = a.berekening_id
                    WHERE
                        a.berekening_id IS NULL
                    """

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_berekening_ids)
            berekening_ids = [row[0] for row in cursor.fetchall()]

    headers = {
        'Authorization': f'Bearer {get_oauth_token()}',
        'Accept': 'application/json'
    }

    alle_berekeningen = []
    total = len(berekening_ids)
    for idx, berekening_id in enumerate(berekening_ids, start=1):
        print(f"Processing {idx}/{total}")

        url = (
            f"https://caredrive.safetyct.com/api/incidentberekeningen/{berekening_id}"
            f"?includeIncidentMomentopname=true"
            f"&includeWegstatusMomentopname=true"
            f"&includeOnlyInzetvoorstel=false"
            f"&includeRoutes=true"
        )
        data = get_api_response(url, headers)

        details = [(
            berekening_id,
            data["tijdstipBerekend"],
            data["trigger"],
            data["isInclusiefStandaardArtInzetbehoefte"],
            data.get("opmerkingen", []),
            json.dumps(data.get("eenheidMomentopnamen", [])),
            json.dumps(data.get("incidentMomentopname", {})),
            json.dumps(data.get("wegstatusMomentopname", {})),
            json.dumps(data.get("kladblokregels", [])),
            json.dumps(data.get("karakteristieken", [])),
            json.dumps(data.get("voertuigProfielen", []))
        )]

        alle_berekeningen.extend(details)

    sql_insert = """
        INSERT INTO public.dynamisch_alarmeren_safetyct_detailsoverview (
            berekening_id,
            tijdstip_berekend,
            trigger,
            is_inclusief_standaard_art_inzetbehoefte,
            opmerkingen,
            eenheid_momentopnamen,
            incident_momentopname,
            wegstatus_momentopname,
            kladblokregels,
            karakteristieken,
            voertuig_profielen
        )
        VALUES %s
        ON CONFLICT (berekening_id) DO NOTHING;
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, sql_insert, alle_berekeningen, page_size=2000)
        conn.commit()


with DAG(
    dag_id='OIV_dynAlarm_safetyCT_berekeningen_details',
    schedule='0 4 * * *',
    start_date=datetime(2022, 1, 1),
    max_active_runs=1,
    catchup=False
) as dag:

    sync_incidenten_task = PythonOperator(
        task_id='sync_incidenten',
        python_callable=sync_incidenten,
        provide_context=True
    )
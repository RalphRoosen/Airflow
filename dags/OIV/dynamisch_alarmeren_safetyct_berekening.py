from datetime import datetime
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from OIV.dwh_geo_sync_wegmoffelen import SRC_CONN_ID
from OIV.dynamisch_alarmeren import get_oauth_token

def sync_incident_berekeningen(**context) -> None:
    hook = PostgresHook(postgres_conn_id=SRC_CONN_ID)

    # Get berekening_ids we have not yet processed
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT d.berekening_id 
                FROM public.dynamisch_alarmeren_safetyct_berekeningenoverview d
                LEFT JOIN public.dynamisch_alarmeren_safetyct_ib ib
                ON d.berekening_id = ib.incident_id::bigint
                WHERE d.berekening_id IS NOT NULL AND ib.incident_id IS NULL
            """)
            berekening_ids = [row[0] for row in cursor.fetchall()]

    if not berekening_ids:
        print("No new berekening_ids to process.")
        return

    print(f"Found {len(berekening_ids)} berekening_ids to fetch.")

    headers = {
        'Authorization': f'Bearer {get_oauth_token()}',
        'Accept': 'application/json'
    }

    values = []
    for berekening_id in berekening_ids:
        url = f"https://caredrive.safetyct.com/api/public/incidentberekeningen/{berekening_id}"
        print(f"Fetching: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            item = response.json()

            incident = item.get("incidentMomentopname", {})
            locatie = incident.get("locatie", {})
            brw = incident.get("brwDiscipline", {})

            # Defaults
            roepnaam = None
            voertuigsoorten = None
            rangorde = None

            eenheden = item.get("eenheidMomentopnamen", [])
            if eenheden:
                roepnaam = eenheden[0].get("roepnaam")
                voertuigsoorten_list = eenheden[0].get("voertuigSoorten", [])
                voertuigsoorten = ','.join(voertuigsoorten_list) if voertuigsoorten_list else None

                berekeningen = eenheden[0].get("berekeningen", [])
                if berekeningen:
                    zijden = berekeningen[0].get("zijdeRijtijden", [])
                    if zijden:
                        rangorde = zijden[0].get("rangordeInzetvoorstel")

            values.append((
                berekening_id,
                incident.get("incidentNr"),
                incident.get("incidentStatus"),
                brw.get("prioriteit"),
                brw.get("startDtg"),
                brw.get("afsluitDtg"),
                locatie.get("gemeentenaam"),
                locatie.get("plaatsnaam"),
                locatie.get("postcode"),
                locatie.get("naamLocatie"),
                incident.get("tijdstipMessageBus"),
                roepnaam,
                voertuigsoorten,
                rangorde
            ))
        except Exception as e:
            print(f"Error fetching berekening_id {berekening_id}: {e}")

    if not values:
        print("No valid incidentberekeningen to insert.")
        return

    sql = """
        INSERT INTO public.dynamisch_alarmeren_safetyct_ib (
            incident_id, incident_nr, status, prioriteit,
            startdtg, afsluitdtg, gemeente, plaats,
            postcode, locatie_naam, tijdstip_messagebus,
            roepnaam, voertuigsoorten, rangorde_inzetvoorstel
        ) VALUES %s
        ON CONFLICT (incident_id) DO NOTHING
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, sql, values, page_size=1000)
        conn.commit()

    print(f"Inserted {len(values)} rows into safetyct_ib.")

with DAG(
    dag_id='OIV_dynAlarm_safetyCT_IB',
    schedule_interval='0 3 * * *',  # Every day at 3 AM
    start_date=datetime(2022, 1, 1),
    max_active_runs=1,
    catchup=False
) as dag:

    sync_incident_berekeningen_task = PythonOperator(
        task_id='sync_incident_berekeningen',
        python_callable=sync_incident_berekeningen,
        provide_context=True
    )
from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ============================================================================
# Config
# ============================================================================
DAG_ID = "load_voertuigbezetting_lms"

DWH_CONN_ID = "dwh"  # doel-database (DWH)
TGT_TABLE = "vdb.voertuigbezetting_lms"

INZET_TABLE = "vdb.inzetten"
KAZERNE_TABLE = "vdb.inzetten_kazerne_beroeps_vrijwillig"
KLADBLOK_TABLE = "vdb.kladblokregels"


# ============================================================================
# Stap 1: Laden van basisdata in voertuigbezetting_lms
# ============================================================================
def load_voertuigbezetting_lms():
    """
    Vul vdb.voertuigbezetting_lms met inzet_id + kladblokregel.
    Alleen vrijwillige TS/TST-voertuigen.
    """
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            insert_sql = f"""
                INSERT INTO {TGT_TABLE} (
                    inzet_eenheid_id,
                    kladblokregel
                )
                SELECT
                    inzet.inzet_eenheid_id,
                    kladblok.inhoud_kladblok_regel
                FROM
                    {INZET_TABLE} AS inzet
                INNER JOIN 
                    {KAZERNE_TABLE} AS kazerne
                    ON kazerne.inzet_eenheid_id = inzet.inzet_eenheid_id
                INNER JOIN 
                    {KLADBLOK_TABLE} AS kladblok
                    ON kladblok.incident_id = inzet.incident_id
                WHERE
                    inzet.code_voertuigsoort IN ('TS', 'TST')
                    AND kazerne.vrijwillig IS TRUE
                    AND kladblok.inhoud_kladblok_regel ~* '^[0-9]{{4}}\\s*[:;]?\\s*TS\\s*[1-9]$'
                    AND CAST(SUBSTRING(kladblok.inhoud_kladblok_regel FROM '^[0-9]{{4}}') AS integer)
                        = MOD(CAST(inzet.naam_voertuig AS integer), 10000);
            """

            print(f"[voertuigbezetting_lms] INSERT into {TGT_TABLE} gestart...")
            cur.execute(insert_sql)
            print(f"[voertuigbezetting_lms] rows inserted: {cur.rowcount}")

        conn.commit()
        print("[voertuigbezetting_lms] COMMIT OK")


# ============================================================================
# Stap 2: Extracteer TS-nummer en schrijf weg als aantal_personen_totaal
# ============================================================================
def extract_ts_nummer_to_aantal_personen():
    """
    Update kolom aantal_personen_totaal met het cijfer dat achter 'TS' staat
    in de kolom kladblokregel (bijv. TS3 → 3).
    """
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)

    update_sql = f"""
        UPDATE {TGT_TABLE}
        SET aantal_personen_totaal = CAST(
            substring(kladblokregel FROM '(?i)TS\\s*([0-9]+)$')
            AS integer
        )
        WHERE kladblokregel IS NOT NULL AND kladblokregel ~* 'TS\\s*[0-9]+$';
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            print(f"[voertuigbezetting_lms] UPDATE aantal_personen_totaal gestart...")
            cur.execute(update_sql)
            print(f"[voertuigbezetting_lms] rows updated: {cur.rowcount}")

        conn.commit()
        print("[voertuigbezetting_lms] COMMIT OK")


# ============================================================================
# Airflow DAG
# ============================================================================
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
    tags=["vdb", "voertuigbezetting", "kladblok", "LMS"],
) as dag:

    load_task = PythonOperator(
        task_id="load_voertuigbezetting_lms",
        python_callable=load_voertuigbezetting_lms,
    )

    extract_task = PythonOperator(
        task_id="extract_ts_nummer_to_aantal_personen",
        python_callable=extract_ts_nummer_to_aantal_personen,
    )

    # Volgorde: eerst laden, dan TS-nummer extraheren
    load_task >> extract_task

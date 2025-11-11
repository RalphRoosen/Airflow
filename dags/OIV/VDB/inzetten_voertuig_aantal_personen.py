from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

# ============================================================================
# Config
# ============================================================================
DAG_ID = "vdb_inzetten_voertuig_aantal_personen"
PG_CONN_ID = "dwh"

SRC_TABLE = "vdb.inzetten"
BEROEPS_TABLE = "vdb.inzetten_kazerne_beroeps_vrijwillig"
DST_TABLE = "vdb.inzetten_voertuig_aantal_personen"


# ============================================================================
# Functie
# ============================================================================
def insert_inzetten_ag5_kladblok():

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.execute(f"""
        INSERT INTO {DST_TABLE} (
            inzet_eenheid_id,
            aantal,
            reden
        )
        SELECT 
            i.inzet_eenheid_id,
            CASE
                -- 1 beroeps_rv + TS/TST → 4
                WHEN k.beroeps_rv IS TRUE AND k.beroeps IS TRUE
                     AND i.code_voertuigsoort IN ('TS', 'TST')
                THEN 4

                -- 2 Specifieke voertuigtypes → 2
                WHEN i.code_voertuigsoort IN ('AL', 'HV', 'SI', 'SI-2', 'HW', 'HV-RTV', 'RV', 'HV-KR')
                THEN 2

                -- 3 Placeholder: als AG5 > 0, schrijf waarde uit AG5
                WHEN i.ag5_aantal > 0
                THEN i.ag5_aantal

                -- 4 Placeholder: als LMS > 0, schrijf waarde uit LMS
                WHEN i.lms_aantal > 0
                THEN i.lms_aantal

                -- 5 beroeps = TRUE en beroeps_rv = FALSE + TS/TST → 6
                WHEN k.beroeps IS TRUE AND k.beroeps_rv IS FALSE
                     AND i.code_voertuigsoort IN ('TS', 'TST')
                THEN 6

                -- 6 vrijwillig = TRUE + TS/TST → 3
                WHEN k.vrijwillig IS TRUE
                     AND i.code_voertuigsoort IN ('TS', 'TST')
                THEN 3

                -- 7 Default → 0
                ELSE 0
            END AS aantal,

            CASE
                WHEN k.beroeps_rv IS TRUE AND k.beroeps IS TRUE
                     AND i.code_voertuigsoort IN ('TS', 'TST')
                THEN 'Beroeps_RV + TS/TST → aantal 4'

                WHEN i.code_voertuigsoort IN ('AL','HV','SI','SI-2','HW','HV-RTV','RV','HV-KR')
                THEN 'Voertuigtype AL/HV/SI/SI-2/HW/HV-RTV/RV/HV-KR → aantal 2'

                WHEN i.ag5_aantal > 0
                THEN 'AG5 aantal > 0 → waarde uit AG5 gebruikt'

                WHEN i.lms_aantal > 0
                THEN 'LMS aantal > 0 → waarde uit LMS gebruikt'

                WHEN k.beroeps IS TRUE AND k.beroeps_rv IS FALSE
                     AND i.code_voertuigsoort IN ('TS', 'TST')
                THEN 'Beroeps zonder RV + TS/TST → aantal 6'

                WHEN k.vrijwillig IS TRUE
                     AND i.code_voertuigsoort IN ('TS', 'TST')
                THEN 'Vrijwillig + TS/TST → aantal 3'

                ELSE 'Geen specifieke voorwaarde voldaan → aantal 0'
            END AS reden

        FROM {SRC_TABLE} i
        LEFT JOIN {BEROEPS_TABLE} k 
               ON k.inzet_eenheid_id = i.inzet_eenheid_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM {DST_TABLE} d
            WHERE d.inzet_eenheid_id = i.inzet_eenheid_id
        );
    """)

    conn.commit()
    cur.close()
    conn.close()


# ============================================================================
# DAG-definitie
# ============================================================================
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # handmatig draaien
    catchup=False,
    tags=["vdb", "ag5", "sync"],
) as dag:

    insert_task = PythonOperator(
        task_id="insert_inzetten_ag5_kladblok",
        python_callable=insert_inzetten_ag5_kladblok,
    )

    insert_task

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

# ============================================================================
# Config
# ============================================================================
DAG_ID = "vdb_inzetten_voertuig_aankomstvolgorde"
PG_CONN_ID = "dwh"

SRC_TABLE = "vdb.inzetten"
DST_TABLE = "vdb.inzetten_voertuig_aankomstvolgorde"


# ============================================================================
# Task 1: insert volgorde (alleen nieuwe inzet_eenheid_id's)
# ============================================================================
def insert_inzetten_voertuig_aankomstvolgorde():
    print("---------- insert_inzetten_voertuig_aankomstvolgorde start -------------")

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.execute(f"""
        WITH src_new AS (
            SELECT i.*
            FROM {SRC_TABLE} i
            WHERE NOT EXISTS (
                SELECT 1
                FROM {DST_TABLE} d
                WHERE d.inzet_eenheid_id = i.inzet_eenheid_id
            )
        ),
        ranked AS (
            SELECT
                s.inzet_eenheid_id,
                s.incident_id,
                DENSE_RANK() OVER (
                    PARTITION BY s.incident_id
                    ORDER BY s.vr_dtg_tp ASC
                ) AS rk
            FROM src_new s
            WHERE s.code_voertuigsoort IN (
                    'HV', 'TS', 'TST',
                    'HVH-RTV', 'HV-K', 'HV-KR',
                    'SI', 'SI-2'
                )
              AND s.vr_tp_seconden > 0
        )
        INSERT INTO {DST_TABLE} (
            inzet_eenheid_id,
            volgorde
        )
        SELECT
            s.inzet_eenheid_id,
            COALESCE(r.rk, 0) AS volgorde
        FROM src_new s
        LEFT JOIN ranked r
          ON r.inzet_eenheid_id = s.inzet_eenheid_id;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("---------- insert_inzetten_voertuig_aankomstvolgorde klaar -------------")


# ============================================================================
# Task 2: opmerking vullen op basis van oorzaak
# ============================================================================
def update_opmerking_inzetten_voertuig_aankomstvolgorde():
    print("---------- update_opmerking_inzetten_voertuig_aankomstvolgorde start -------------")

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    # Opmerking invullen afhankelijk van oorzaak, maar bestaande waardes behouden
    cur.execute(f"""
        UPDATE {DST_TABLE} dst
        SET opmerking = CASE
            WHEN src.code_voertuigsoort NOT IN (
                    'HV', 'TS', 'TST',
                    'HVH-RTV', 'HV-K', 'HV-KR',
                    'SI', 'SI-2'
                )
            THEN 'geen relevant voertuig'
            WHEN src.code_voertuigsoort IN (
                    'HV', 'TS', 'TST',
                    'HVH-RTV', 'HV-K', 'HV-KR',
                    'SI', 'SI-2'
                )
             AND (src.vr_tp_seconden IS NULL OR src.vr_tp_seconden = 0)
            THEN 'geen tp tijd'
            ELSE dst.opmerking
        END
        FROM {SRC_TABLE} src
        WHERE dst.inzet_eenheid_id = src.inzet_eenheid_id
          AND dst.volgorde = 0
          AND dst.opmerking IS NULL;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("---------- update_opmerking_inzetten_voertuig_aankomstvolgorde klaar -------------")


# ============================================================================
# DAG-definitie
# ============================================================================
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # handmatig draaien
    catchup=False,
    tags=["vdb", "inzetten", "rangorde"],
) as dag:

    insert_task = PythonOperator(
        task_id="insert_inzetten_voertuig_aankomstvolgorde",
        python_callable=insert_inzetten_voertuig_aankomstvolgorde,
    )

    update_task = PythonOperator(
        task_id="update_opmerking_inzetten_voertuig_aankomstvolgorde",
        python_callable=update_opmerking_inzetten_voertuig_aankomstvolgorde,
    )

    insert_task >> update_task

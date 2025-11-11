from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

CONNECTION_ID = 'dwh'

SRC_INZETTEN_TABLE = "vdb.inzetten"
SRC_KAZERNE_TABLE = "vdb.inzetten_kazerne_beroeps_vrijwillig"
AG5_VEHICLE_TABLE = "public.ag5_involvedvehicle"
AG5_PERSONS_TABLE = "public.ag5_personassignments"
TARGET = "vdb.voertuigbezetting_ag5"

def sync_ag5_voertuigbezetting(**context):
    """
    Vul vdb.ag5_voertuigbezetting met nieuwe voertuigen uit AG5.
    Voor elk inzet_eenheid_id dat nog niet in de tabel staat:
      - Tel aantal personen
      - Bepaal of er een bevelvoerder (BV) is
      - Bepaal of er een chauffeur (CHF) is
    """

    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    # 1️⃣  SELECT alle nieuwe inzetten (nog niet in vdb.ag5_voertuigbezetting)
    #     inclusief berekening van aantal_personen_totaal, BV en CHF
    select_sql = f"""
        SELECT
            i.inzet_eenheid_id,
            COUNT(*) AS aantal_personen_totaal,
            BOOL_OR(LOWER(pa.role) LIKE '%bevelvoerder%') AS has_bv,
            BOOL_OR(LOWER(pa.role) LIKE '%chauffeur%')   AS has_chf
        FROM {SRC_INZETTEN_TABLE} i
        JOIN {SRC_KAZERNE_TABLE} k
            ON i.inzet_eenheid_id = k.inzet_eenheid_id
        JOIN {AG5_VEHICLE_TABLE} iv
          ON LEFT(REPLACE(iv.callsign, '-', ''), 6) = i.roepnaam_eenheid
        JOIN {AG5_PERSONS_TABLE} pa
          ON pa.vehicleid = iv.vehicleid
         AND pa.ag5_incident_id = iv.ag5_incident_id
        WHERE i.roepnaam_eenheid IS NOT NULL
          AND i.code_voertuigsoort IN ('TS', 'TST')
          AND k.vrijwillig = TRUE
          AND pa.vehicleid IS NOT NULL
          AND pa.vr_incidentid = i.incident_id
          AND NOT EXISTS (
              SELECT 1
              FROM {TARGET} vb
              WHERE vb.inzet_eenheid_id = i.inzet_eenheid_id
          )
        GROUP BY
            i.inzet_eenheid_id
    """

    cur.execute(select_sql)
    rows = cur.fetchall()

    if not rows:
        print("[ag5] Geen nieuwe voertuigen gevonden.")
        conn.commit()
        cur.close()
        conn.close()
        return

    # 2️⃣  Data voorbereiden voor bulk insert
    #     (inzet_eenheid_id, aantal_personen_totaal, has_bv, has_chf)
    insert_rows = [(r[0], r[1], r[2], r[3]) for r in rows]

    # 3️⃣  Bulk insert in vdb.ag5_voertuigbezetting
    insert_sql = f"""
        INSERT INTO {TARGET} (
            inzet_eenheid_id,
            aantal_personen_totaal,
            has_BV,
            has_CHF
        ) VALUES %s
    """
    execute_values(cur, insert_sql, insert_rows)

    conn.commit()
    cur.close()
    conn.close()
    print(f"[ag5] {len(insert_rows)} nieuwe voertuigen toegevoegd aan vdb.ag5_voertuigbezetting.")


# =====================================================================
# DAG-definitie
# =====================================================================
with DAG(
    dag_id="ag5_voertuigbezetting_bulk_insert",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # of bijvoorbeeld "0 * * * *"
    catchup=False,
    max_active_runs=1,
    tags=["vdb", "ag5", "voertuigbezetting", "sql", "bulk"],
) as dag:

    sync_ag5_voertuigbezetting_task = PythonOperator(
        task_id="sync_ag5_voertuigbezetting",
        python_callable=sync_ag5_voertuigbezetting,
        provide_context=True,
    )

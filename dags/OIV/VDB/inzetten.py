from __future__ import annotations
from datetime import datetime
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

# ============================================================================
# Config
# ============================================================================
DAG_ID = "load_inzetten_van_vdb"
SRC_CONN_ID = "vdb"     # bron: VDB (virtuele db connectie)
DST_CONN_ID = "dwh"     # doel: DWH
DST_TABLE = "vdb.inzetten"

MELDKAMER = "LB"
KAZ_PREFIX = "24 "      # kaz_naam moet hiermee beginnen (bv. '24 Heerlen')

BATCH_SIZE = 5000

# ============================================================================
# Kolommen in vaste volgorde (bron -> target)
# ============================================================================
INZET_COLS: List[str] = [
    "inzet_eenheid_id",
    "incident_id",
    "roepnaam_eenheid",
    "t_ind_disc_eenheid",
    "dtg_opdracht_inzet",
    "code_voertuigsoort",
    "kaz_naam",
    "stpl_vertrek",
    "naam_voertuig",
    "dtg_ontkoppel",
    "rol",
    "meldkamer",
    "rit_id",
    "koppel_regio_code",
    "externe_id",
    "prim_inzetrol",
    "kaz_afk",
    "ind_disc_incident",
    "pol_regio_nr",
    "brw_melding_cl2",
    "ind_melding",
]

def copy_inzetten_batched():
    """
    Batch-ETL van VDB -> DWH:
    - bepaal hoogste incident_id dat al in vdb.inzetten staat
    - haal daarna alleen records met hoger incident_id uit hst_inzet_eenheid
      gefilterd op meldkamer='LB' en kaz_naam LIKE '24 %'
    - pure INSERT (geen upsert), want inzet_eenheid_id is uniek (PK)
    """
    src_hook = PostgresHook(postgres_conn_id=SRC_CONN_ID, schema="gms-vdb")
    dst_hook = PostgresHook(postgres_conn_id=DST_CONN_ID)

    # 0) bepaal startpunt
    with dst_hook.get_conn() as dconn:
        with dconn.cursor() as dcur:
            dcur.execute(f"SELECT COALESCE(MAX(incident_id), 0) FROM {DST_TABLE};")
            last_incident_id = dcur.fetchone()[0]

    print(f"[loader-inzetten] start vanaf incident_id > {last_incident_id}")

    insert_cols_sql = ", ".join(INZET_COLS)
    insert_sql = f"INSERT INTO {DST_TABLE} ({insert_cols_sql}) VALUES %s"

    total_inserted = 0

    while True:
        select_list = ", ".join(f"e.{c}" for c in INZET_COLS)
        batch_sql = f"""
            SELECT {select_list}
            FROM v_24_0_gms_views.hst_inzet_eenheid e
            WHERE e.meldkamer   = %s
              AND e.kaz_naam LIKE %s
              AND e.incident_id > %s
            ORDER BY e.incident_id
            LIMIT {BATCH_SIZE}
        """

        # 1) lees batch uit de bron
        with src_hook.get_conn() as sconn:
            with sconn.cursor() as scur:
                scur.execute(batch_sql, (MELDKAMER, f"{KAZ_PREFIX}%", last_incident_id))
                rows = scur.fetchall()

        if not rows:
            print(f"[loader-inzetten] geen nieuwe rows meer na incident_id {last_incident_id}, stop.")
            break

        # 2) schrijf batch naar DWH
        with dst_hook.get_conn() as dconn:
            with dconn.cursor() as dcur:
                execute_values(dcur, insert_sql, rows)
            dconn.commit()

        batch_count = len(rows)
        total_inserted += batch_count

        # incident_id staat op index 1 in INZET_COLS
        incident_idx = INZET_COLS.index("incident_id")
        last_incident_id = rows[-1][incident_idx]

        print(f"[loader-inzetten] batch inserted {batch_count} rows, last_incident_id={last_incident_id}, total={total_inserted}")

    print(f"[loader-inzetten] DONE. totaal inserted naar {DST_TABLE}: {total_inserted} rijen")

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
    tags=["vdb", "inzetten", "LB", "batched"],
) as dag:

    load_inzetten_task = PythonOperator(
        task_id="copy_inzetten_lb_batched",
        python_callable=copy_inzetten_batched,
    )

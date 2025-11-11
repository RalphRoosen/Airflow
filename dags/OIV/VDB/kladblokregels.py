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
DAG_ID = "load_kladblokregels_van_vdb_2"  # pas aan als je een andere naam wilt
SRC_CONN_ID = "vdb"     # bron: VDB (gms-vdb / Teiid)
DST_CONN_ID = "dwh"     # doel: DWH (schema vdb)
DST_TABLE = "vdb.kladblokregels"

MELDKAMER = "LB"
KLADBLOK_INSERT_BATCH_SIZE = 5000      # max # rows per INSERT-chunk
INCIDENT_BATCH_SIZE = 500              # max # incident_id's per bron-query (bewust iets lager i.v.m. Teiid)

# ============================================================================
# Kolommen in vaste volgorde (bron -> target)
# Moet overeenkomen met de definitie van vdb.kladblokregels
# ============================================================================
KLADBLOK_COLS: List[str] = [
    "kladblok_regel_id",
    "incident_id",
    "dtg_kladblok_regel",
    "t_ind_disc_kladblok_regel",
    "type_kladblok_regel",
    "code_kladblok_regel",
    "volg_nr_kladblok_regel",
    "inhoud_kladblok_regel",
    "user_naam",
    "externe_systeem_type",
    "externe_systeem_code",
    "meldkamer",
    "wijziging_id",
]


def get_last_incident_id_in_kladblokregels() -> int:
    """
    Bepaal de hoogste incident_id die al in vdb.kladblokregels staat.
    Dit is ons increment-punt.
    """
    dst_hook = PostgresHook(postgres_conn_id=DST_CONN_ID)
    with dst_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(incident_id), 0) FROM vdb.kladblokregels;")
            last_id = cur.fetchone()[0] or 0

    print(f"[loader-kladblok] last_incident_id in vdb.kladblokregels = {last_id}")
    return int(last_id)


def get_incident_ids_to_sync(last_incident_id: int) -> List[int]:
    """
    Haal dynamisch de lijst incident_id op uit vdb.incidenten
    die nog NIET in vdb.kladblokregels zitten (incident_id > last_incident_id).
    vdb.incidenten is al gefilterd, extra filter op meldkamer = LB voor de zekerheid.
    """
    dst_hook = PostgresHook(postgres_conn_id=DST_CONN_ID)
    with dst_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT incident_id
                FROM vdb.incidenten
                WHERE incident_id > %s
                  AND meldkamer = %s
                ORDER BY incident_id
                """,
                (last_incident_id, MELDKAMER),
            )
            rows = cur.fetchall()

    incident_ids = [int(r[0]) for r in rows]
    print(
        f"[loader-kladblok] aantal incidenten in scope (incident_id > {last_incident_id}, meldkamer={MELDKAMER}): "
        f"{len(incident_ids)}"
    )
    return incident_ids


def copy_kladblokregels_batched():
    """
    Incremental batch-ETL van VDB -> DWH (vdb.kladblokregels):
    - bepaal hoogste incident_id die al in vdb.kladblokregels staat
    - haal uit vdb.incidenten alleen incidenten met hoger incident_id (en meldkamer=LB)
    - per batch van incident_id's:
        - lees alle kladblokregels uit v_24_0_gms_views.hst_kladblok
          voor die incidenten, met meldkamer=LB en t_ind_disc_kladblok_regel LIKE '%B%'
        - schrijf ze weg naar vdb.kladblokregels
    """
    src_hook = PostgresHook(postgres_conn_id=SRC_CONN_ID, schema="gms-vdb")
    dst_hook = PostgresHook(postgres_conn_id=DST_CONN_ID)

    # 0) Bepaal increment-start: last incident_id in doeltabel
    last_incident_id = get_last_incident_id_in_kladblokregels()

    # 1) Haal incidenten op die nog niet volledig zijn verwerkt
    incident_ids = get_incident_ids_to_sync(last_incident_id)
    if not incident_ids:
        print("[loader-kladblok] geen nieuwe incidenten gevonden om te laden; stop.")
        return

    insert_cols_sql = ", ".join(KLADBLOK_COLS)
    insert_sql = f"INSERT INTO {DST_TABLE} ({insert_cols_sql}) VALUES %s"

    total_inserted = 0

    # 2) Verwerk incident_ids in kleinere batches
    for i in range(0, len(incident_ids), INCIDENT_BATCH_SIZE):
        batch_incident_ids = incident_ids[i : i + INCIDENT_BATCH_SIZE]
        print(
            f"[loader-kladblok] incident-batch {i // INCIDENT_BATCH_SIZE + 1}: "
            f"{len(batch_incident_ids)} incidenten (range {batch_incident_ids[0]} - {batch_incident_ids[-1]})"
        )

        # 2a) Stel SELECT-statement op met dynamische IN-placeholders
        select_list = ", ".join(f"k.{c}" for c in KLADBLOK_COLS)
        placeholders = ", ".join(["%s"] * len(batch_incident_ids))

        batch_sql = f"""
            SELECT {select_list}
            FROM v_24_0_gms_views.hst_kladblok k
            WHERE k.meldkamer = %s
              AND CAST(k.incident_id AS bigint) IN ({placeholders})
              AND k.t_ind_disc_kladblok_regel LIKE %s
            ORDER BY k.kladblok_regel_id, k.volg_nr_kladblok_regel
        """

        # param-volgorde: meldkamer, alle incident_ids, pattern
        params = [MELDKAMER] + [int(x) for x in batch_incident_ids] + ["%B%"]

        with src_hook.get_conn() as sconn:
            with sconn.cursor() as scur:
                scur.execute(batch_sql, params)
                rows = scur.fetchall()

        if not rows:
            print("[loader-kladblok] geen kladblokregels voor deze incident-batch.")
            continue

        # 2b) Schrijf batch naar DWH in chunks
        offset = 0
        while offset < len(rows):
            chunk = rows[offset : offset + KLADBLOK_INSERT_BATCH_SIZE]
            offset += KLADBLOK_INSERT_BATCH_SIZE

            with dst_hook.get_conn() as dconn:
                with dconn.cursor() as dcur:
                    execute_values(dcur, insert_sql, chunk)
                dconn.commit()

            batch_count = len(chunk)
            total_inserted += batch_count
            print(
                f"[loader-kladblok] inserted {batch_count} rows "
                f"(totaal={total_inserted}) voor incident-batch "
                f"{i // INCIDENT_BATCH_SIZE + 1}"
            )

    print(f"[loader-kladblok] DONE. totaal inserted naar {DST_TABLE}: {total_inserted} rijen")


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
    tags=["vdb", "kladblok", "batched", "incremental", "incident-based"],
) as dag:

    load_kladblokregels_task = PythonOperator(
        task_id="copy_kladblokregels_batched_incremental_incident",
        python_callable=copy_kladblokregels_batched,
    )

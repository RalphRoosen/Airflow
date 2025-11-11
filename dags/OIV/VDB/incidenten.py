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
DAG_ID = "load_incidenten_van_vdb"
SRC_CONN_ID = "vdb"     # bron: VDB (virtuele db connectie, schema gms-vdb)
DST_CONN_ID = "dwh"     # doel: DWH
DST_TABLE = "vdb.incidenten"

MELDKAMER = "LB"

INCIDENT_BATCH_SIZE = 500   # max # incident_id's per bron-query (Teiid vriendelijk)
INSERT_BATCH_SIZE = 5000    # max # rows per INSERT-chunk

# ============================================================================
# Kolommen in vaste volgorde (bron -> target)
# ============================================================================
INCIDENT_COLS: List[str] = [
    "incident_id",
    "nr_incident",
    "ind_disc_incident",
    "dtg_start_incident",
    "dtg_einde_incident",
    "naam_gebeurtenis",
    "ind_bevestiging_regio",
    "volgend_nr_incident",
    "leidend_nr_incident",
    "vervolg_nr_incident",
    "oud_nr_incident",
    "brw_melding_cl",
    "brw_melding_cl1",
    "brw_melding_cl2",
    "type_locatie1",
    "naam_locatie1",
    "huis_paal_nr",
    "huis_nr_toev",
    "huisletter",
    "huis_nr_aanduiding",
    "postcode",
    "plaats_naam",
    "plaats_naam_afk",
    "gemeente_naam",
    "type_locatie2",
    "naam_locatie2",
    "t_gui_locatie",
    "t_x_coord_loc",
    "t_y_coord_loc",
    "object_functie_naam",
    "naam_instantie",
    "vak_nr",
    "aol_geweest",
    "ind_paaltype",
    "ind_ong_huis_nr",
    "afsluitstatus_brw",
    "straatnaam_nen",
    "ind_melding",
    "brw_dtg_start_incident",
    "brw_dtg_einde_incident",
    "brw_soort_afsluiting",
    "prioriteit_incident_brandweer",
    "ind_heruitgifte_brw",
    "t_x_coord_aanrij",
    "t_y_coord_aanrij",
    "bag_adresseerbaarobject_id",
    "bag_adres_woonplaats",
    "bag_nummeraanduiding_id",
    "bag_openbareruimte1_id",
    "bag_openbareruimte2_id",
    "bag_woonplaats_id",
    "aanrij_x_coordinaat_obj_brw",
    "aanrij_y_coordinaat_obj_brw",
    "meldkamer",
    "cpa_melding_cl",
    "cpa_melding_cl1",
    "cpa_melding_cl2",
    "cpa_dtg_start_incident",
    "cpa_dtg_einde_incident",
    "rit_classificatie_code",
    "rit_classificatie_omschrijving",
    "ind_coord_van_melder",
    "ind_heruitgifte_cpa",
    "pol_melding_cl",
    "pol_melding_cl1",
    "pol_melding_cl2",
    "pol_buurt_nr",
    "pol_wijk_nr",
    "bureau_nr",
    "pol_district_nr",
    "pol_regio_nr",
    "afsluitstatus_pol",
    "pol_dtg_start_incident",
    "pol_dtg_einde_incident",
    "pol_soort_afsluiting",
    "prioriteit_incident_politie",
    "ind_heruitgifte_pol",
    "aanrij_x_coordinaat_obj_pol",
    "aanrij_y_coordinaat_obj_pol",
    "dtg_alarm_incident",
    "type_locatie1_oms",
    "type_locatie2_oms",
    "cpa_regio_nr",
    "cpa_naam_regio",
    "verkorte_naam_geb",
    "brw_kaz_afk",
    "brw_korps_naam",
    "brw_regio_nr",
    "brw_regio_naam",
    "leidend_incident_id",
]


def get_last_incident_id_in_incidenten() -> int:
    """
    Bepaal de hoogste incident_id die al in vdb.incidenten staat.
    Dit is ons increment-punt.
    """
    dst_hook = PostgresHook(postgres_conn_id=DST_CONN_ID)
    with dst_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(incident_id), 0) FROM vdb.incidenten;")
            last_id = cur.fetchone()[0] or 0

    print(f"[loader-incidenten] last_incident_id in vdb.incidenten = {last_id}")
    return int(last_id)


def get_incident_ids_to_sync(last_incident_id: int) -> List[int]:
    """
    Haal dynamisch de lijst incident_id op uit vdb.inzetten
    die nog NIET in vdb.incidenten zitten (incident_id > last_incident_id).

    We filteren op meldkamer = LB, zodat we alleen de incidenten ophalen
    waarvoor ook inzetten voor LB bestaan.
    """
    dst_hook = PostgresHook(postgres_conn_id=DST_CONN_ID)
    with dst_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT incident_id
                FROM vdb.inzetten
                WHERE incident_id > %s
                  AND meldkamer = %s
                ORDER BY incident_id
                """,
                (last_incident_id, MELDKAMER),
            )
            rows = cur.fetchall()

    incident_ids = [int(r[0]) for r in rows]
    print(
        f"[loader-incidenten] aantal incidenten in scope (incident_id > {last_incident_id}, meldkamer={MELDKAMER}): "
        f"{len(incident_ids)}"
    )
    return incident_ids


def copy_incidenten_batched():
    """
    Incremental batch-ETL van VDB -> DWH (vdb.incidenten):

    - bepaal hoogste incident_id die al in vdb.incidenten staat
    - haal daarna via vdb.inzetten de lijst incident_id's op (incident_id > last, meldkamer=LB)
    - per batch van incident_id's:
        - lees alle incident-rijen uit v_24_0_gms_views.hst_incidenten
          voor die incidenten, met meldkamer=LB
        - schrijf ze weg naar vdb.incidenten
    """
    src_hook = PostgresHook(postgres_conn_id=SRC_CONN_ID, schema="gms-vdb")
    dst_hook = PostgresHook(postgres_conn_id=DST_CONN_ID)

    # 0) Bepaal increment-start: last incident_id in doeltabel
    last_incident_id = get_last_incident_id_in_incidenten()

    # 1) Haal incidenten op die nog niet volledig zijn verwerkt (via inzetten)
    incident_ids = get_incident_ids_to_sync(last_incident_id)
    if not incident_ids:
        print("[loader-incidenten] geen nieuwe incidenten gevonden om te laden; stop.")
        return

    insert_cols_sql = ", ".join(INCIDENT_COLS)
    insert_sql = f"INSERT INTO {DST_TABLE} ({insert_cols_sql}) VALUES %s"

    total_inserted = 0

    # 2) Verwerk incident_ids in kleinere batches
    for i in range(0, len(incident_ids), INCIDENT_BATCH_SIZE):
        batch_incident_ids = incident_ids[i : i + INCIDENT_BATCH_SIZE]
        print(
            f"[loader-incidenten] incident-batch {i // INCIDENT_BATCH_SIZE + 1}: "
            f"{len(batch_incident_ids)} incidenten (range {batch_incident_ids[0]} - {batch_incident_ids[-1]})"
        )

        # 2a) Stel SELECT-statement op met dynamische IN-placeholders
        select_list = ", ".join(f"i.{c}" for c in INCIDENT_COLS)
        placeholders = ", ".join(["%s"] * len(batch_incident_ids))

        batch_sql = f"""
            SELECT {select_list}
            FROM v_24_0_gms_views.hst_incident i
            WHERE i.meldkamer = %s
              AND CAST(i.incident_id AS bigint) IN ({placeholders})
            ORDER BY i.incident_id
        """

        params = [MELDKAMER] + [int(x) for x in batch_incident_ids]

        with src_hook.get_conn() as sconn:
            with sconn.cursor() as scur:
                scur.execute(batch_sql, params)
                rows = scur.fetchall()

        if not rows:
            print("[loader-incidenten] geen incident-rijen voor deze incident-batch.")
            continue

        # 2b) Schrijf batch naar DWH in chunks
        offset = 0
        while offset < len(rows):
            chunk = rows[offset : offset + INSERT_BATCH_SIZE]
            offset += INSERT_BATCH_SIZE

            with dst_hook.get_conn() as dconn:
                with dconn.cursor() as dcur:
                    execute_values(dcur, insert_sql, chunk)
                dconn.commit()

            batch_count = len(chunk)
            total_inserted += batch_count
            print(
                f"[loader-incidenten] inserted {batch_count} rows "
                f"(totaal={total_inserted}) voor incident-batch "
                f"{i // INCIDENT_BATCH_SIZE + 1}"
            )

    print(f"[loader-incidenten] DONE. totaal inserted naar {DST_TABLE}: {total_inserted} rijen")


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
    tags=["vdb", "incidenten", "LB", "batched", "incident-based"],
) as dag:

    load_incidenten_task = PythonOperator(
        task_id="copy_incidenten_lb_batched_incident_based",
        python_callable=copy_incidenten_batched,
    )

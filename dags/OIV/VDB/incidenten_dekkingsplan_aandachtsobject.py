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
SRC_CONN_ID = "vdb"     # bron: VDB (virtuele db connectie)
DST_CONN_ID = "dwh"     # doel: DWH
DST_TABLE = "vdb.incidenten"

MELDKAMER = "LB"

BATCH_SIZE = 5000

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


def copy_incidenten_batched():
    """
    Batch-ETL van VDB -> DWH:
    - bepaal hoogste incident_id dat al in vdb.incidenten staat
    - haal daarna alleen records met hoger incident_id uit v_24_0_gms_views.hst_incidenten
      gefilterd op meldkamer = 'LB'
    - pure INSERT (geen upsert), want incident_id is uniek binnen de scope die we laden
    """
    src_hook = PostgresHook(postgres_conn_id=SRC_CONN_ID, schema="gms-vdb")
    dst_hook = PostgresHook(postgres_conn_id=DST_CONN_ID)

    # 0) bepaal startpunt
    with dst_hook.get_conn() as dconn:
        with dconn.cursor() as dcur:
            dcur.execute(f"SELECT COALESCE(MAX(incident_id), 0) FROM {DST_TABLE};")
            last_incident_id = dcur.fetchone()[0]

    print(f"[loader-incidenten] start vanaf incident_id > {last_incident_id}")

    insert_cols_sql = ", ".join(INCIDENT_COLS)
    insert_sql = f"INSERT INTO {DST_TABLE} ({insert_cols_sql}) VALUES %s"

    total_inserted = 0

    while True:
        select_list = ", ".join(f"i.{c}" for c in INCIDENT_COLS)
        batch_sql = f"""
            SELECT {select_list}
            FROM v_24_0_gms_views.hst_incidenten i
            WHERE i.meldkamer = %s
              AND i.incident_id > %s
            ORDER BY i.incident_id
            LIMIT {BATCH_SIZE}
        """

        # 1) lees batch uit de bron
        with src_hook.get_conn() as sconn:
            with sconn.cursor() as scur:
                scur.execute(batch_sql, (MELDKAMER, last_incident_id))
                rows = scur.fetchall()

        if not rows:
            print(f"[loader-incidenten] geen nieuwe rows meer na incident_id {last_incident_id}, stop.")
            break

        # 2) schrijf batch naar DWH
        with dst_hook.get_conn() as dconn:
            with dconn.cursor() as dcur:
                execute_values(dcur, insert_sql, rows)
            dconn.commit()

        batch_count = len(rows)
        total_inserted += batch_count

        # incident_id staat op index 0 in INCIDENT_COLS
        incident_idx = INCIDENT_COLS.index("incident_id")
        last_incident_id = rows[-1][incident_idx]

        print(
            f"[loader-incidenten] batch inserted {batch_count} rows, "
            f"last_incident_id={last_incident_id}, total={total_inserted}"
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
    tags=["vdb", "incidenten", "LB", "batched"],
) as dag:

    load_incidenten_task = PythonOperator(
        task_id="copy_incidenten_lb_batched",
        python_callable=copy_incidenten_batched,
    )

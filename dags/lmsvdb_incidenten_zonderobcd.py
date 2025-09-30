# OLD (pyodbc)
# import psycopg2
# import pyodbc
import time
from datetime import datetime
from datetime import timedelta

from tabulate import tabulate
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
doc_md = """
### Synchroniseren met LMS
#### getDekkingsplan

1. Loop door alle incidenten in inci_vr_incident vanaf incident id sync_from_getDekkingsplan_incident_id. Deze airflow var wordt naar iedere run een maand terug gezet. Hier wordt de volgende keer gestart.
2. Voor ieder incident haal uit dekkingsplan_objectselectie_anw_ts4 op basis van postcode huisnummer omschrijving, buurtcode en opkomsttijd_ts1
	- Indien buurtcode gevonden dekkingsplan_buurtenrapport_anw_ts4 bevragen voor dekPlanBuurtRappCat.

3. Voor ieder incident dekkingsplan_objectselectie_aandachtsobjecten bevragen op opkomsttijd_ts1_ts6, opkomsttijd_ts2_ts6, opkomsttijd_ts1_ts4.

Update bij incident de kolommen:
- AandachtsObject
- omschrijving
- dekPlanBuurtRappCat
- dekPlanBuurtRappBuNaam
- dekPlanBuurtRappWKCode
- dekPlanBuurtRappWaardering
- opkomsttijd_ts1_anwflex
- opkomsttijd_ts1_ts6
- opkomsttijd_ts2_ts6
- opkomsttijd_ts1_ts4
- lms_incident_id
"""

vdb_schema      =   "v_24_0_gms_views"

meldkamerCode   =   "LB"

def safe_ts_str(dt_like):
    """
    Maak een Teiid-veilige timestamp string (YYYY-MM-DD HH:MM:SS).
    Accepteert datetime, date of string (met of zonder 'T').
    """
    if dt_like is None:
        return None
    if isinstance(dt_like, datetime):
        return dt_like.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(dt_like, "strftime"):  # date
        return datetime(dt_like.year, dt_like.month, dt_like.day).strftime("%Y-%m-%d %H:%M:%S")
    # string:
    s = str(dt_like).replace("T", " ").split(".")[0].strip()
    # fallback: als er alleen een datum staat
    if len(s) == 10 and s.count("-") == 2:
        s += " 00:00:00"
    return s

def vdb_fetch_batch(sql, params=None, attempts=2, backoff_first=1.0):
    """
    Haal rijen uit VDB met verse Hook/Conn/Cursor per poging.
    - attempts: aantal pogingen
    - backoff_first: wachttijd (s) bij 1e retry; verdubbelt elke keer
    Retourneert list(tuple).
    Gooi de laatste exception door na mislukte pogingen.
    """
    last_err = None
    for i in range(1, attempts + 1):
        print(f"[vdb_fetch_batch] attempt {i}/{attempts}")
        try:
            vdb_hook = PostgresHook(postgres_conn_id='vdb', schema='gms-vdb')
            with vdb_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # NB: param-binding gebruikt psycopg2-placeholder %s
                    if params:
                        cur.execute(sql, params)
                    else:
                        cur.execute(sql)
                    rows = cur.fetchall()
                    print(f"[vdb_fetch_batch] fetched {len(rows)} rows")
                    return rows
        except Exception as e:
            last_err = e
            print(f"[vdb_fetch_batch][WARN] fetch failed on attempt {i}: {e}")
            if i < attempts:
                sleep_s = backoff_first * (2 ** (i - 1))
                time.sleep(sleep_s)
    # alle pogingen gefaald
    raise last_err

daysBackInTime = 2


X0      = 155000
Y0      = 463000
PHI0    = 52.15517440
LAM0    = 5.38720621
def rd_to_wgs(x, y):
    """
    Convert rijksdriehoekcoordinates into WGS84 cooridnates. Input parameters: x (float), y (float). 
    """

    if isinstance(x, (list, tuple)):
        x, y = x

    pqk = [(0, 1, 3235.65389),
        (2, 0, -32.58297),
        (0, 2, -0.24750),
        (2, 1, -0.84978),
        (0, 3, -0.06550),
        (2, 2, -0.01709),
        (1, 0, -0.00738),
        (4, 0, 0.00530),
        (2, 3, -0.00039),
        (4, 1, 0.00033),
        (1, 1, -0.00012)]

    pql = [(1, 0, 5260.52916), 
        (1, 1, 105.94684), 
        (1, 2, 2.45656), 
        (3, 0, -0.81885), 
        (1, 3, 0.05594), 
        (3, 1, -0.05607), 
        (0, 1, 0.01199), 
        (3, 2, -0.00256), 
        (1, 4, 0.00128), 
        (0, 2, 0.00022), 
        (2, 0, -0.00022), 
        (5, 0, 0.00026)]

    dx = 1E-5 * ( x - X0 )
    dy = 1E-5 * ( y - Y0 )
    
    phi = PHI0
    lam = LAM0

    for p, q, k in pqk:
        phi += k * dx**p * dy**q / 3600

    for p, q, l in pql:
        lam += l * dx**p * dy**q / 3600

    return [phi,lam]

def _testDBConnection():
    try:
        # cnxn = pyodbc.connect(connection_string)
        # cursor = cnxn.cursor()
        # print('')
        # print('Connection successful')
        # print('')
        # cnxn.close()
        #
        # except pyodbc.Error as ex:
        #     print('')
        #     print('Connection failed: ' + str(ex))
        #     print('')

        vdb_hook = PostgresHook(postgres_conn_id='vdb', schema='gms-vdb')
        cnxn = vdb_hook.get_conn()
        cursor = cnxn.cursor()
        print('')
        print('Connection successful')
        print('')
        cnxn.close()
    except Exception as ex:
        print('')
        print('Connection failed: ' + str(ex))
        print('')

def _syncIncidents(tbl_voorvoegsel):
    print("=== [syncIncidents] START ===")
    print(f"[syncIncidents] tbl_voorvoegsel = {tbl_voorvoegsel}")

    syncFromVarName = f"sync_from_{tbl_voorvoegsel}_syncIncidents_incident_id"
    lms_sync_from   = int(Variable.get(syncFromVarName, 0))
    print(f"[syncIncidents] init lms_sync_from (Variable {syncFromVarName}) = {lms_sync_from}")

    tbl_pk = "incident_id"
    limit  = 1000

    dwh_hook = PostgresHook(postgres_conn_id="dwh", schema="dwh")
    dwh_conn = dwh_hook.get_conn()
    dwh_cur  = dwh_conn.cursor()
    print("[syncIncidents] Connected to DWH via PostgresHook")

    total_upserts = 0
    while True:
        # ------- VDB batch ophalen (parametrized + retry) -------
        sql_vdb = f"""
            SELECT 
                INCIDENT_ID, 
                NR_INCIDENT,
                DTG_START_INCIDENT,
                DTG_EINDE_INCIDENT,
                NAAM_GEBEURTENIS,
                BRW_MELDING_CL,
                BRW_MELDING_CL1,
                BRW_MELDING_CL2,
                TYPE_LOCATIE1,
                NAAM_LOCATIE1,
                HUIS_PAAL_NR,
                HUIS_NR_TOEV,
                HUISLETTER,
                HUIS_NR_AANDUIDING,
                POSTCODE,
                PLAATS_NAAM,
                PLAATS_NAAM_AFK,
                GEMEENTE_NAAM,
                TYPE_LOCATIE2,
                NAAM_LOCATIE2,
                T_X_COORD_LOC,
                T_Y_COORD_LOC,
                OBJECT_FUNCTIE_NAAM,
                VAK_NR,
                AFSLUITSTATUS_BRW,
                STRAATNAAM_NEN,
                BRW_DTG_START_INCIDENT,
                BRW_DTG_EINDE_INCIDENT,
                BRW_SOORT_AFSLUITING,
                PRIORITEIT_INCIDENT_BRANDWEER,
                BRW_KAZ_AFK,
                BRW_KORPS_NAAM,
                BRW_REGIO_NR,
                BRW_REGIO_NAAM,
                MELDKAMER
            FROM {vdb_schema}.{tbl_voorvoegsel}_incident
            WHERE meldkamer = %s
              AND {tbl_pk} > %s
              AND BRW_MELDING_CL IS NOT NULL
            ORDER BY {tbl_pk}
            LIMIT {limit}
        """
        print(f"[syncIncidents] VDB query (start_from={lms_sync_from}, limit={limit})")
        rows = vdb_fetch_batch(sql_vdb, params=(meldkamerCode, lms_sync_from), attempts=3, backoff_first=1.0)

        if not rows:
            print(f"[syncIncidents] No rows; done (last_sync_from={lms_sync_from})")
            break

        print(f"[syncIncidents] Batch size: {len(rows)} | first_id={rows[0][0]} last_id={rows[-1][0]}")

        # ------- Schrijven naar DWH -------
        for idx, row in enumerate(rows, start=1):
            incident_id = row[0]
            # RD -> WGS
            lat = None
            lon = None
            try:
                if row[20] is not None and row[21] is not None:
                    wgs = rd_to_wgs(float(row[20]), float(row[21]))
                    lat, lon = wgs[0], wgs[1]
            except Exception as e:
                print(f"[syncIncidents][WARN] RD->WGS failed for x={row[20]} y={row[21]}: {e}")

            # bestaat al?
            dwh_cur.execute("SELECT COUNT(*) FROM public.inci_vr_incident WHERE lms_incident_id = %s", (incident_id,))
            exists = dwh_cur.fetchone()[0] > 0

            if exists:
                # UPDATE met parameters (volg exact de kolomvolgorde)
                sql_upd = """
                    UPDATE public.inci_vr_incident SET
                        LMS_NR_INCIDENT = %s,
                        LMS_DTG_START_INCIDENT = %s,
                        LMS_DTG_EINDE_INCIDENT = %s,
                        LMS_NAAM_GEBEURTENIS = %s,
                        LMS_BRW_MELDING_CL = %s,
                        LMS_BRW_MELDING_CL1 = %s,
                        LMS_BRW_MELDING_CL2 = %s,
                        LMS_TYPE_LOCATIE1 = %s,
                        LMS_NAAM_LOCATIE1 = %s,
                        LMS_HUIS_PAAL_NR = %s,
                        LMS_HUIS_NR_TOEV = %s,
                        LMS_HUISLETTER = %s,
                        LMS_HUIS_NR_AANDUIDING = %s,
                        LMS_POSTCODE = %s,
                        LMS_PLAATS_NAAM = %s,
                        LMS_PLAATS_NAAM_AFK = %s,
                        LMS_GEMEENTE_NAAM = %s,
                        LMS_TYPE_LOCATIE2 = %s,
                        LMS_NAAM_LOCATIE2 = %s,
                        LMS_T_X_COORD_LOC = %s,
                        LMS_T_Y_COORD_LOC = %s,
                        LMS_OBJECT_FUNCTIE_NAAM = %s,
                        LMS_VAK_NR = %s,
                        LMS_AFSLUITSTATUS_BRW = %s,
                        LMS_STRAATNAAM_NEN = %s,
                        LMS_BRW_DTG_START_INCIDENT = %s,
                        LMS_BRW_DTG_EINDE_INCIDENT = %s,
                        LMS_BRW_SOORT_AFSLUITING = %s,
                        LMS_PRIORITEIT_INCIDENT_BRANDWEER = %s,
                        LMS_BRW_KAZ_AFK = %s,
                        LMS_BRW_KORPS_NAAM = %s,
                        LMS_BRW_REGIO_NR = %s,
                        LMS_BRW_REGIO_NAAM = %s,
                        LMS_MELDKAMER = %s,
                        vr_lat = %s,
                        vr_lon = %s,
                        vr_viewvoorvoegsel = %s
                    WHERE LMS_INCIDENT_ID = %s
                """
                params_upd = (
                    row[1],  row[2],  row[3],  row[4],  row[5],
                    row[6],  row[7],  row[8],  row[9],  row[10],
                    row[11], row[12], row[13], row[14], row[15],
                    row[16], row[17], row[18], row[19], row[20],
                    row[21], row[22], row[23], row[24], row[25],
                    row[26], row[27], row[28], row[29], row[30],
                    row[31], row[32], row[33], row[34],
                    lat, lon, tbl_voorvoegsel,
                    incident_id
                )
                dwh_cur.execute(sql_upd, params_upd)
            else:
                # INSERT met parameters
                sql_ins = """
                    INSERT INTO public.inci_vr_incident (
                        LMS_INCIDENT_ID, 
                        LMS_NR_INCIDENT,
                        LMS_DTG_START_INCIDENT,
                        LMS_DTG_EINDE_INCIDENT,
                        LMS_NAAM_GEBEURTENIS,
                        LMS_BRW_MELDING_CL,
                        LMS_BRW_MELDING_CL1,
                        LMS_BRW_MELDING_CL2,
                        LMS_TYPE_LOCATIE1,
                        LMS_NAAM_LOCATIE1,
                        LMS_HUIS_PAAL_NR,
                        LMS_HUIS_NR_TOEV, 
                        LMS_HUISLETTER,
                        LMS_HUIS_NR_AANDUIDING,
                        LMS_POSTCODE,
                        LMS_PLAATS_NAAM,
                        LMS_PLAATS_NAAM_AFK,
                        LMS_GEMEENTE_NAAM,
                        LMS_TYPE_LOCATIE2,
                        LMS_NAAM_LOCATIE2,
                        LMS_T_X_COORD_LOC,
                        LMS_T_Y_COORD_LOC,
                        LMS_OBJECT_FUNCTIE_NAAM,
                        LMS_VAK_NR,
                        LMS_AFSLUITSTATUS_BRW,
                        LMS_STRAATNAAM_NEN,
                        LMS_BRW_DTG_START_INCIDENT,
                        LMS_BRW_DTG_EINDE_INCIDENT,
                        LMS_BRW_SOORT_AFSLUITING,
                        LMS_PRIORITEIT_INCIDENT_BRANDWEER,
                        LMS_BRW_KAZ_AFK,
                        LMS_BRW_KORPS_NAAM,
                        LMS_BRW_REGIO_NR,
                        LMS_BRW_REGIO_NAAM,
                        LMS_MELDKAMER,
                        vr_lat,
                        vr_lon,
                        vr_viewvoorvoegsel
                    ) VALUES (
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,%s
                    )
                """
                params_ins = (
                    row[0],  row[1],  row[2],  row[3],  row[4],  row[5],
                    row[6],  row[7],  row[8],  row[9],  row[10], row[11],
                    row[12], row[13], row[14], row[15], row[16], row[17],
                    row[18], row[19], row[20], row[21], row[22], row[23],
                    row[24], row[25], row[26], row[27], row[28], row[29],
                    row[30], row[31], row[32], row[33], row[34],
                    lat, lon, tbl_voorvoegsel
                )
                dwh_cur.execute(sql_ins, params_ins)

            total_upserts += 1
            # we verschuiven de in-memory pointer alvast voor de volgende batchselect
            lms_sync_from = incident_id

        # Commit de hele batch en zet dan pas de Variable
        dwh_conn.commit()
        Variable.set(syncFromVarName, lms_sync_from)
        print(f"[syncIncidents] COMMIT batch OK. last_incident_id={lms_sync_from} | total_upserts={total_upserts}")

    # ------- Pointer “terugzetten” (rolling window) -------
    # Gebruik eerst BRW_DTG_START_INCIDENT; als die None is, val terug op DTG_START_INCIDENT.
    try:
        # timestamp van laatst verwerkte incident
        sql_dt = f"""
            SELECT BRW_DTG_START_INCIDENT, DTG_START_INCIDENT
            FROM {vdb_schema}.{tbl_voorvoegsel}_incident
            WHERE {tbl_pk} = %s
        """
        dt_row = vdb_fetch_batch(sql_dt, params=(lms_sync_from,), attempts=2)
        ts_last = None
        if dt_row:
            ts_brw = dt_row[0][0]
            ts_std = dt_row[0][1]
            ts_last = ts_brw if ts_brw is not None else ts_std

        if ts_last:
            dt_last  = safe_ts_str(ts_last)
            dt_minus = safe_ts_str(datetime.strptime(dt_last, "%Y-%m-%d %H:%M:%S") - timedelta(days=daysBackInTime))

            # 1) probeer op BRW_DTG_START_INCIDENT
            sql_from_brw = f"""
                SELECT {tbl_pk}
                FROM {vdb_schema}.{tbl_voorvoegsel}_incident
                WHERE meldkamer = %s
                  AND BRW_MELDING_CL IS NOT NULL
                  AND BRW_DTG_START_INCIDENT <= %s
                ORDER BY {tbl_pk} DESC
                LIMIT 1
            """
            try:
                back_row = vdb_fetch_batch(sql_from_brw, params=(meldkamerCode, dt_minus), attempts=2)
            except Exception:
                back_row = []

            # 2) val terug op DTG_START_INCIDENT
            if not back_row:
                sql_from_std = f"""
                    SELECT {tbl_pk}
                    FROM {vdb_schema}.{tbl_voorvoegsel}_incident
                    WHERE meldkamer = %s
                      AND BRW_MELDING_CL IS NOT NULL
                      AND DTG_START_INCIDENT <= %s
                    ORDER BY {tbl_pk} DESC
                    LIMIT 1
                """
                back_row = vdb_fetch_batch(sql_from_std, params=(meldkamerCode, dt_minus), attempts=2)

            if back_row:
                Variable.set(syncFromVarName, int(back_row[0][0]))
                print(f"[syncIncidents] Pointer teruggezet naar {back_row[0][0]} (op/bij {dt_minus})")
            else:
                print("[syncIncidents][WARN] Geen rollback-pointer gevonden; pointer blijft op latest.")
        else:
            print("[syncIncidents][WARN] Geen timestamp bij laatste incident; rollback-pointer overgeslagen.")
    except Exception as e:
        print(f"[syncIncidents][WARN] pointer-rollback overgeslagen: {e}")

    print("=== [syncIncidents] END ===")

def _collectMelding(viewvoorvoegsel):
    print("---------- _collectMelding voor viewvoorvoegsel:", viewvoorvoegsel, "-------------")

    dwh_hook = PostgresHook(postgres_conn_id='dwh', schema='dwh')
    dwh_conn = dwh_hook.get_conn()
    dwh_cur  = dwh_conn.cursor()

    # batchlimiet
    limit = 1000 if viewvoorvoegsel == 'ARC' else 2000

    var_name = f"sync_from_{viewvoorvoegsel}_collectMelding_melding_id"
    last_id  = int(Variable.get(var_name, 0))

    total_written = 0
    while True:
        sql = f"""
            SELECT 
                melding_id, incident_id, melding_nr, huis_nr, huisletter, naam_melder, 
                straatnaam_nen, huis_nr_toev, huis_nr_aanduiding, postcode, plaats_naam, 
                gemeente_naam, cli_abonnee_nr, lijn_melding, meldkamer, dtg_melding, 
                t_bron_melding, persnr_aannamecentralist, naam_aannamecentralist, mld_soort_afsluiting
            FROM {vdb_schema}.{viewvoorvoegsel}_melding
            WHERE melding_id > %s AND meldkamer = %s
            ORDER BY melding_id
            LIMIT {limit}
        """
        print(f"[collectMelding] VDB query (start_from={last_id}, limit={limit})")
        rows = vdb_fetch_batch(sql, params=(last_id, meldkamerCode))

        if not rows:
            print(f"[collectMelding] No rows; done (last_id={last_id})")
            break

        print(f"[collectMelding] Batch size: {len(rows)}")
        # schrijf batch
        for r in rows:
            (melding_id, incident_id, melding_nr, huis_nr, huisletter, naam_melder,
             straatnaam_nen, huis_nr_toev, huis_nr_aanduiding, postcode, plaats_naam,
             gemeente_naam, cli_abonnee_nr, lijn_melding, meldkamer, dtg_melding,
             t_bron_melding, persnr_aannamecentralist, naam_aannamecentralist, mld_soort_afsluiting) = r

            # upsert-achtig (delete+insert) zoals je al deed
            dwh_cur.execute("DELETE FROM public.inci_vr_melding WHERE melding_id = %s", (melding_id,))
            dwh_cur.execute("""
                INSERT INTO public.inci_vr_melding(
                    melding_id, incident_id, melding_nr, huis_nr, huisletter, naam_melder,
                    straatnaam_nen, huis_nr_toev, huis_nr_aanduiding, postcode, plaats_naam,
                    gemeente_naam, cli_abonnee_nr, lijn_melding, meldkamer, dtg_melding,
                    t_bron_melding, persnr_aannamecentralist, naam_aannamecentralist, mld_soort_afsluiting
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s
                )
            """, r)
            total_written += 1

        # commit batch & pointer pas na succes
        dwh_conn.commit()
        last_id = rows[-1][0]
        Variable.set(var_name, last_id)
        print(f"[collectMelding] COMMIT batch ok. last_melding_id={last_id} (total_written={total_written})")

    # —— Pointer “terugzetten” (veilig) —— #
    # Haal dtg van laatst verwerkte id
    try:
        sql_dt = f"SELECT dtg_melding FROM {vdb_schema}.{viewvoorvoegsel}_melding WHERE melding_id = %s"
        dt_row = vdb_fetch_batch(sql_dt, params=(last_id,), attempts=2)
        if dt_row and dt_row[0][0]:
            dt_last   = safe_ts_str(dt_row[0][0])
            dt_minus  = safe_ts_str((datetime.strptime(dt_last, "%Y-%m-%d %H:%M:%S")) - timedelta(days=daysBackInTime))
            # Zoek de laatste id op/bij dat tijdstip
            sql_from = f"""
                SELECT melding_id 
                FROM {vdb_schema}.{viewvoorvoegsel}_melding
                WHERE meldkamer = %s AND dtg_melding <= %s
                ORDER BY melding_id DESC
                LIMIT 1
            """
            from_row = vdb_fetch_batch(sql_from, params=(meldkamerCode, dt_minus), attempts=2)
            if from_row:
                Variable.set(var_name, int(from_row[0][0]))
                print(f"[collectMelding] Pointer teruggezet naar {from_row[0][0]} (op/bij {dt_minus})")
            else:
                print("[collectMelding][WARN] Geen pointer-rollback gevonden; pointer blijft op latest.")
        else:
            print("[collectMelding][WARN] kon dtg_melding niet ophalen voor last_id.")
    except Exception as e:
        print(f"[collectMelding][WARN] pointer-rollback overgeslagen: {e}")

def _collectKladblokRegels(viewvoorvoegsel):
    print("---------- collectKladblokRegels viewvoorvoegsel:", viewvoorvoegsel, "-------------")

    dwh_hook = PostgresHook(postgres_conn_id='dwh', schema='dwh')
    dwh_conn = dwh_hook.get_conn()
    dwh_cur  = dwh_conn.cursor()

    var_name = f"sync_from_{viewvoorvoegsel}_collectKladblokRegels_kladblok_regel_id"
    last_id  = int(Variable.get(var_name, 0)) - 1
    if last_id < 0:
        last_id = 0

    limit = 5000  # beide varianten gelijk bij jou

    total_written = 0
    while True:
        sql = f"""
            SELECT 
                incident_id, kladblok_regel_id, volg_nr_kladblok_regel, wijziging_id, 
                type_kladblok_regel, code_kladblok_regel, inhoud_kladblok_regel, user_naam, 
                externe_systeem_type, externe_systeem_code, meldkamer, dtg_kladblok_regel
            FROM {vdb_schema}.{viewvoorvoegsel}_kladblok
            WHERE meldkamer = %s
              AND kladblok_regel_id > %s
              AND t_ind_disc_kladblok_regel LIKE '%%B%%'
            ORDER BY kladblok_regel_id, volg_nr_kladblok_regel
            LIMIT {limit}
        """
        print(f"[collectKladblokRegels] VDB query (start_from={last_id}, limit={limit})")
        rows = vdb_fetch_batch(sql, params=(meldkamerCode, last_id))

        if not rows:
            print(f"[collectKladblokRegels] No rows; done (last_id={last_id})")
            break

        # Schrijf batch
        # NB: kladblok_regel_id is de “record”-id; om duplicates te mijden doen we delete+insert per (incident_id, kladblok_regel_id, volg_nr_kladblok_regel) of enkel op kladblok_regel_id (zoals je deed)
        for r in rows:
            (incident_id, kladblok_regel_id, volg_nr, wijziging_id, type_kr, code_kr,
             inhoud, user_naam, ex_type, ex_code, meldkamer, dtg) = r

            dwh_cur.execute("DELETE FROM public.inci_vr_kladblok WHERE incident_id=%s AND kladblok_regel_id=%s AND volg_nr_kladblok_regel=%s",
                            (incident_id, kladblok_regel_id, volg_nr))

            dwh_cur.execute("""
                INSERT INTO public.inci_vr_kladblok(
                    incident_id, kladblok_regel_id, volg_nr_kladblok_regel, wijziging_id,
                    type_kladblok_regel, code_kladblok_regel, inhoud_kladblok_regel, user_naam,
                    externe_systeem_type, externe_systeem_code, meldkamer, dtg_kladblok_regel
                ) VALUES (
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,%s
                )
            """, r)
            total_written += 1

        dwh_conn.commit()
        last_id = rows[-1][1]  # kladblok_regel_id
        Variable.set(var_name, last_id)
        print(f"[collectKladblokRegels] COMMIT batch ok. last_kladblok_regel_id={last_id} (total_written={total_written})")

    # —— Pointer “terugzetten” —— #
    try:
        sql_dt = f"SELECT dtg_kladblok_regel FROM {vdb_schema}.{viewvoorvoegsel}_kladblok WHERE kladblok_regel_id = %s"
        dt_row = vdb_fetch_batch(sql_dt, params=(last_id,), attempts=2)
        if dt_row and dt_row[0][0]:
            dt_last  = safe_ts_str(dt_row[0][0])
            dt_minus = safe_ts_str((datetime.strptime(dt_last, "%Y-%m-%d %H:%M:%S")) - timedelta(days=daysBackInTime))
            sql_from = f"""
                SELECT kladblok_regel_id
                FROM {vdb_schema}.{viewvoorvoegsel}_kladblok
                WHERE meldkamer=%s AND dtg_kladblok_regel <= %s
                ORDER BY kladblok_regel_id DESC
                LIMIT 1
            """
            from_row = vdb_fetch_batch(sql_from, params=(meldkamerCode, dt_minus), attempts=2)
            if from_row:
                Variable.set(var_name, int(from_row[0][0]))
                print(f"[collectKladblokRegels] Pointer teruggezet naar {from_row[0][0]} (op/bij {dt_minus})")
            else:
                print("[collectKladblokRegels][WARN] Geen pointer-rollback gevonden; pointer blijft op latest.")
        else:
            print("[collectKladblokRegels][WARN] kon dtg_kladblok_regel niet ophalen voor last_id.")
    except Exception as e:
        print(f"[collectKladblokRegels][WARN] pointer-rollback overgeslagen: {e}")

    print(f"{total_written} kladblokregels gevonden/geschreven voor {viewvoorvoegsel}")

def _collectInzetEenheid(viewvoorvoegsel):
    print("---------- _collectInzetEenheid voor viewvoorvoegsel:", viewvoorvoegsel, "-------------")

    dwh_hook = PostgresHook(postgres_conn_id='dwh', schema='dwh')
    dwh_conn = dwh_hook.get_conn()
    dwh_cur  = dwh_conn.cursor()

    limit = 50000 if viewvoorvoegsel == 'ARC' else 1000
    var_name = f"sync_from_{viewvoorvoegsel}_collectInzetEenheid_inzet_eenheid_id"
    last_id  = int(Variable.get(var_name, 0))

    total_written = 0
    while True:
        sql = f"""
            SELECT 
                inzet_eenheid_id, incident_id, roepnaam_eenheid, code_voertuigsoort, 
                kaz_naam, naam_voertuig, rol, meldkamer, kaz_afk, 
                dtg_opdracht_inzet, dtg_ontkoppel
            FROM {vdb_schema}.{viewvoorvoegsel}_inzet_eenheid
            WHERE inzet_eenheid_id > %s
              AND meldkamer = %s
            ORDER BY inzet_eenheid_id
            LIMIT {limit}
        """
        print(f"[collectInzetEenheid] VDB query (start_from={last_id}, limit={limit})")
        rows = vdb_fetch_batch(sql, params=(last_id, meldkamerCode))

        if not rows:
            print(f"[collectInzetEenheid] No rows; done (last_id={last_id})")
            break

        for r in rows:
            (inzet_eenheid_id, incident_id, roepnaam_eenheid, code_voertuigsoort,
             kaz_naam, naam_voertuig, rol, meldkamer, kaz_afk, dtg_opdracht_inzet, dtg_ontkoppel) = r

            dwh_cur.execute("DELETE FROM public.inci_vr_inzet_eenheid WHERE inzet_eenheid_id = %s",
                            (inzet_eenheid_id,))
            dwh_cur.execute("""
                INSERT INTO public.inci_vr_inzet_eenheid(
                    inzet_eenheid_id, incident_id, roepnaam_eenheid, code_voertuigsoort, kaz_naam, 
                    naam_voertuig, rol, meldkamer, kaz_afk, dtg_opdracht_inzet, dtg_ontkoppel
                ) VALUES (
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s
                )
            """, r)
            total_written += 1

        dwh_conn.commit()
        last_id = rows[-1][0]
        Variable.set(var_name, last_id)
        print(f"[collectInzetEenheid] COMMIT batch ok. last_inzet_eenheid_id={last_id} (total_written={total_written})")

    # —— Pointer “terugzetten” —— #
    try:
        sql_dt = f"SELECT dtg_opdracht_inzet FROM {vdb_schema}.{viewvoorvoegsel}_inzet_eenheid WHERE inzet_eenheid_id=%s"
        dt_row = vdb_fetch_batch(sql_dt, params=(last_id,), attempts=2)
        if dt_row and dt_row[0][0]:
            dt_last  = safe_ts_str(dt_row[0][0])
            dt_minus = safe_ts_str((datetime.strptime(dt_last, "%Y-%m-%d %H:%M:%S")) - timedelta(days=daysBackInTime))
            sql_from = f"""
                SELECT inzet_eenheid_id
                FROM {vdb_schema}.{viewvoorvoegsel}_inzet_eenheid
                WHERE meldkamer=%s AND dtg_opdracht_inzet <= %s
                ORDER BY inzet_eenheid_id DESC
                LIMIT 1
            """
            from_row = vdb_fetch_batch(sql_from, params=(meldkamerCode, dt_minus), attempts=2)
            if from_row:
                Variable.set(var_name, int(from_row[0][0]))
                print(f"[collectInzetEenheid] Pointer teruggezet naar {from_row[0][0]} (op/bij {dt_minus})")
            else:
                print("[collectInzetEenheid][WARN] Geen pointer-rollback gevonden; pointer blijft op latest.")
        else:
            print("[collectInzetEenheid][WARN] kon dtg_opdracht_inzet niet ophalen voor last_id.")
    except Exception as e:
        print(f"[collectInzetEenheid][WARN] pointer-rollback overgeslagen: {e}")

def _calcBrandGrootte_Gaslek_dcu_fromKB():
    print("_calcBrandGrootte_Gaslek_dcu_fromKB")

    dwh_hook = PostgresHook(postgres_conn_id='dwh', schema='dwh')
    conn = dwh_hook.get_conn()
    cur  = conn.cursor()

    today = datetime.now().date()
    datumVan = (today - timedelta(days=90)).strftime("%Y-%m-%d")
    print(f"Sync vanaf {datumVan}")

    def exec_and_log(sql, params, label):
        cur.execute(sql, params)
        print(f"[{label}] rows updated: {cur.rowcount}")

    try:
        # ===== BRAND =====
        print("---------- Bereken brandgrootte uit kladblok -------------")

        # reset
        exec_and_log("""
            UPDATE public.inci_vr_incident
               SET brandgrootte = '-'
             WHERE lms_dtg_start_incident > %s
        """, (datumVan,), "brand: reset '-'")

        # kleine brand
        exec_and_log("""
            UPDATE public.inci_vr_incident tgt
               SET brandgrootte = 'kleine brand'
              FROM public.inci_vr_kladblok kb
             WHERE tgt.lms_incident_id = kb.incident_id
               AND kb.inhoud_kladblok_regel LIKE %s
               AND tgt.lms_dtg_start_incident > %s
        """, ("%-kleine brand%", datumVan), "brand: kleine brand")

        # middelbrand
        exec_and_log("""
            UPDATE public.inci_vr_incident tgt
               SET brandgrootte = 'middelbrand'
              FROM public.inci_vr_kladblok kb
             WHERE tgt.lms_incident_id = kb.incident_id
               AND kb.inhoud_kladblok_regel LIKE %s
               AND tgt.lms_dtg_start_incident > %s
        """, ("%-middelbrand%", datumVan), "brand: middelbrand")

        # grote brand
        exec_and_log("""
            UPDATE public.inci_vr_incident tgt
               SET brandgrootte = 'grote brand'
              FROM public.inci_vr_kladblok kb
             WHERE tgt.lms_incident_id = kb.incident_id
               AND kb.inhoud_kladblok_regel LIKE %s
               AND tgt.lms_dtg_start_incident > %s
        """, ("%-grote brand%", datumVan), "brand: grote brand")

        # zeer grote brand
        exec_and_log("""
            UPDATE public.inci_vr_incident tgt
               SET brandgrootte = 'zeer grote brand'
              FROM public.inci_vr_kladblok kb
             WHERE tgt.lms_incident_id = kb.incident_id
               AND kb.inhoud_kladblok_regel LIKE %s
               AND tgt.lms_dtg_start_incident > %s
        """, ("%-zeer grote brand%", datumVan), "brand: zeer grote brand")

        # ===== GASLEK =====
        print("---------- Bereken gasleksoort uit kladblok -------------")

        # reset
        exec_and_log("""
            UPDATE public.inci_vr_incident
               SET gaslek = '-'
             WHERE lms_dtg_start_incident > %s
        """, (datumVan,), "gaslek: reset '-'")

        # gaslekkage (algemeen)
        exec_and_log("""
            UPDATE public.inci_vr_incident tgt
               SET gaslek = 'gaslekkage'
              FROM public.inci_vr_kladblok kb
             WHERE tgt.lms_incident_id = kb.incident_id
               AND kb.inhoud_kladblok_regel LIKE %s
               AND tgt.lms_dtg_start_incident > %s
        """, ("%-gaslekkage%", datumVan), "gaslek: gaslekkage")

        # gaslekkage binnen
        exec_and_log("""
            UPDATE public.inci_vr_incident tgt
               SET gaslek = 'gaslekkage binnen'
              FROM public.inci_vr_kladblok kb
             WHERE tgt.lms_incident_id = kb.incident_id
               AND kb.inhoud_kladblok_regel LIKE %s
               AND tgt.lms_dtg_start_incident > %s
        """, ("%-gaslekkage binnen%", datumVan), "gaslek: binnen")

        # gaslekkage buiten
        exec_and_log("""
            UPDATE public.inci_vr_incident tgt
               SET gaslek = 'gaslekkage buiten'
              FROM public.inci_vr_kladblok kb
             WHERE tgt.lms_incident_id = kb.incident_id
               AND kb.inhoud_kladblok_regel LIKE %s
               AND tgt.lms_dtg_start_incident > %s
        """, ("%-gaslekkage buiten%", datumVan), "gaslek: buiten")

        # ===== DCU =====
        print("---------- Bereken DCU uit kladblok -------------")

        # reset
        exec_and_log("""
            UPDATE public.inci_vr_incident
               SET vr_dcu = FALSE
             WHERE lms_dtg_start_incident > %s
        """, (datumVan,), "dcu: reset false")

        # markeer DCU
        exec_and_log("""
            UPDATE public.inci_vr_incident tgt
               SET vr_dcu = TRUE
              FROM public.inci_vr_kladblok kb
             WHERE tgt.lms_incident_id = kb.incident_id
               AND kb.inhoud_kladblok_regel ILIKE %s
               AND tgt.lms_dtg_start_incident > %s
        """, ("%-dcu%", datumVan), "dcu: set true")

        conn.commit()
        print("[calc] COMMIT OK")

    except Exception as e:
        conn.rollback()
        print(f"[calc][ERROR] rollback uitgevoerd: {e}")
        raise
    finally:
        try:
            cur.close()
        finally:
            conn.close()

def _doAg5Sync():
    print("---------- doAg5Sync -------------")

    dwh_hook = PostgresHook(postgres_conn_id='dwh', schema='dwh')
    conn = dwh_hook.get_conn()
    cur_inc = conn.cursor()   # voor lijst incidenten
    cur = conn.cursor()       # voor AG5 lookups & updates

    # Startpunt: vanaf ~200 dagen terug (beperkt volume)
    today = datetime.today()
    back_in_time = (today - timedelta(days=200)).strftime('%Y-%m-%d')

    cur_inc.execute("""
        SELECT lms_incident_id
          FROM public.inci_vr_incident
         WHERE lms_dtg_start_incident >= %s
         ORDER BY lms_dtg_start_incident ASC
         LIMIT 1
    """, (back_in_time,))
    row0 = cur_inc.fetchone()
    if not row0:
        print(f"[ag5] Geen incidenten sinds {back_in_time}; niets te doen.")
        cur_inc.close(); cur.close(); conn.close()
        return

    prev_lms_incident_id = row0[0]
    print(f"[ag5] Start vanaf incident_id {prev_lms_incident_id} (>= {back_in_time})")

    LIMIT = 1000
    total_done = 0
    batch_no = 0

    try:
        while True:
            batch_no += 1
            # Pak volgende batch nog-niet-gesyncte incidenten
            cur_inc.execute("""
                SELECT lms_incident_id, vr_viewvoorvoegsel, lms_brw_melding_cl2, lms_plaats_naam,
                       EXTRACT(YEAR FROM lms_dtg_start_incident AT TIME ZONE current_setting('TIMEZONE')) AS year
                  FROM public.inci_vr_incident
                 WHERE vr_ag5_synced = 0
                   AND lms_incident_id > %s
                 ORDER BY lms_incident_id
                 LIMIT %s
            """, (prev_lms_incident_id, LIMIT))
            inci_rows = cur_inc.fetchall()

            if not inci_rows:
                print(f"[ag5] Batch {batch_no}: 0 rijen — klaar.")
                break

            first_id = inci_rows[0][0]
            last_id  = inci_rows[-1][0]
            print(f"[ag5] Batch {batch_no}: size={len(inci_rows)}  first={first_id}  last={last_id}")

            batch_updates = 0
            for (lms_incident_id, viewvoorvoegsel, cl2, plaats, jaar) in inci_rows:
                prev_lms_incident_id = lms_incident_id  # pointer alleen vooruit

                # 1) Vind bijbehorend AG5-incident
                cur.execute("""
                    SELECT ag5_id, numberofpeoplekilled, numberofpeopleinjured, werepeoplerescued
                      FROM public.ag5_incidenten
                     WHERE vr_incidentid = %s
                     LIMIT 1
                """, (str(lms_incident_id),))
                ag5 = cur.fetchone()

                if not ag5:
                    print(f"[ag5] Not Found: {lms_incident_id} : {viewvoorvoegsel} : {cl2} : {plaats}")
                    continue

                ag5_id, killed_raw, injured_raw, rescued_raw = ag5

                # 2) Parse velden veilig
                got_error = False
                try:
                    ag5_killed = int(killed_raw) if killed_raw is not None else None
                except (ValueError, TypeError):
                    got_error = True

                try:
                    ag5_injured = int(injured_raw) if injured_raw is not None else None
                except (ValueError, TypeError):
                    got_error = True

                if isinstance(rescued_raw, str):
                    if rescued_raw.lower() == "true":
                        ag5_rescued = 1
                    elif rescued_raw.lower() == "false":
                        ag5_rescued = 0
                    else:
                        got_error = True
                elif isinstance(rescued_raw, (int, bool)):
                    ag5_rescued = 1 if bool(rescued_raw) else 0
                else:
                    got_error = True

                if got_error:
                    print(f"[ag5] Found but data not okay: {lms_incident_id} : {viewvoorvoegsel} : {cl2} : {plaats}")
                    continue  # niet syncen

                # 3) Haal custom fields in één query
                field_ids = (
                    '5f6958c968d41b4875f8a47d64a93a18',  # aangetroffen situatie
                    'eac7b6c682b6cf10ccaf4baef73e48fb',  # bijzonderheden
                    '4539c60ee730ad89f9b722bfba4fba1f',  # #pers TS
                )
                cur.execute(f"""
                    SELECT fieldid, value
                      FROM public.ag5_incidenten_customfieldvalues
                     WHERE ag5_incident_id = %s
                       AND fieldid IN %s
                """, (ag5_id, tuple(field_ids)))
                cf_map = {r[0]: r[1] for r in cur.fetchall()}

                aangetroffen = cf_map.get(field_ids[0]) or ""
                bijzonder   = cf_map.get(field_ids[1]) or ""
                pers_ts_raw = cf_map.get(field_ids[2])

                try:
                    pers_ts = int(pers_ts_raw) if pers_ts_raw not in (None, "") else None
                except (ValueError, TypeError):
                    pers_ts = None

                # 4) Update incident + markeer als gesynct
                cur.execute("""
                    UPDATE public.inci_vr_incident
                       SET ag5_numberofpeoplekilled = %s,
                           ag5_numberofpeopleinjured = %s,
                           ag5_werepeoplerescued     = %s,
                           ag5_aangetroffen_situatie = %s,
                           ag5_bijzonderheden        = %s,
                           ag5_aantalpers_ts         = %s,
                           vr_ag5_synced             = 1
                     WHERE lms_incident_id = %s
                """, (
                    ag5_killed,
                    ag5_injured,
                    ag5_rescued,
                    aangetroffen,
                    bijzonder,
                    pers_ts,
                    lms_incident_id,
                ))

                batch_updates += 1
                total_done += 1
                print(f"[ag5] Updated: {lms_incident_id} : {viewvoorvoegsel} : {cl2} : {plaats} "
                      f":> killed={ag5_killed} rescued={ag5_rescued} pers_ts={pers_ts}")

            # commit per batch
            conn.commit()
            print(f"[ag5] Batch {batch_no} COMMIT OK (updates={batch_updates})")

        print(f"[ag5] DONE. Totaal geüpdatet: {total_done}")

    except Exception as e:
        conn.rollback()
        print(f"[ag5][ERROR] rollback uitgevoerd: {e}")
        raise
    finally:
        try:
            cur_inc.close()
            cur.close()
        finally:
            conn.close()


def syncSimpleTable(tbl_name, tbl_pk, targetTableName):
    print(f"[syncSimpleTable] Start {tbl_name}")

    syncFromVarName = f"syncSimpleTable_lms_syncFromVar_{tbl_name}"
    lms_sync_from = int(Variable.get(syncFromVarName, 0))
    print(f"[syncSimpleTable] Begin sync vanaf {lms_sync_from}")

    dwh_hook = PostgresHook(postgres_conn_id="dwh", schema="dwh")
    conn = dwh_hook.get_conn()
    cur_dwh = conn.cursor()

    LIMIT = 1500
    doLoop = True
    batch_no = 0
    total_updates = 0
    total_inserts = 0

    start_time = time.time()

    try:
        while doLoop:
            batch_no += 1

            # --- batch ophalen met retry wrapper ---
            primQuery = f"""
                SELECT * 
                  FROM {vdb_schema}.{tbl_name}
                 WHERE meldkamer = %s
                   AND {tbl_pk} > %s
                 ORDER BY {tbl_pk}
                 LIMIT {LIMIT}
            """
            rows = vdb_fetch_batch(primQuery, (meldkamerCode, lms_sync_from))
            if not rows:
                print(f"[syncSimpleTable] Batch {batch_no}: 0 records, klaar.")
                break

            first_id, last_id = rows[0][0], rows[-1][0]
            print(f"[syncSimpleTable] Batch {batch_no}: size={len(rows)} first={first_id} last={last_id}")

            # Bepaal PK-index op basis van cursor.description
            vdb_hook = PostgresHook(postgres_conn_id="vdb", schema="gms-vdb")
            with vdb_hook.get_conn() as vdb_conn:
                vcur = vdb_conn.cursor()
                vcur.execute(f"SELECT * FROM {vdb_schema}.{tbl_name} LIMIT 1")
                desc = vcur.description
                pk_index = [i for i, d in enumerate(desc) if d[0].lower() == tbl_pk.lower()][0]
                colnames = [d[0] for d in desc]

            for row in rows:
                pk_value = row[pk_index]

                # Bestaat record?
                cur_dwh.execute(f"SELECT 1 FROM {targetTableName} WHERE {tbl_pk} = %s", (pk_value,))
                exists = cur_dwh.fetchone()

                if exists:
                    # UPDATE
                    set_clauses = []
                    params = []
                    for col, val in zip(colnames, row):
                        if col.lower() == tbl_pk.lower():
                            continue
                        set_clauses.append(f"{col} = %s")
                        params.append(val)
                    params.append(pk_value)
                    update_sql = f"UPDATE {targetTableName} SET {', '.join(set_clauses)} WHERE {tbl_pk} = %s"
                    cur_dwh.execute(update_sql, tuple(params))
                    total_updates += 1
                else:
                    # INSERT
                    placeholders = ", ".join(["%s"] * len(colnames))
                    insert_sql = f"INSERT INTO {targetTableName} ({', '.join(colnames)}) VALUES ({placeholders})"
                    cur_dwh.execute(insert_sql, tuple(row))
                    total_inserts += 1

                lms_sync_from = pk_value  # pointer vooruit

            # commit batch + update pointer
            conn.commit()
            Variable.set(syncFromVarName, lms_sync_from)
            print(f"[syncSimpleTable] Batch {batch_no} COMMIT OK. last_id={lms_sync_from}")

            doLoop = len(rows) == LIMIT

        elapsed = time.time() - start_time
        print(f"[syncSimpleTable] DONE. Updates={total_updates}, Inserts={total_inserts}, Elapsed={elapsed:.1f}s")

    except Exception as e:
        conn.rollback()
        print(f"[syncSimpleTable][ERROR] rollback: {e}")
        raise
    finally:
        cur_dwh.close()
        conn.close()

def truncateTable(tbl: str):
    """
    Leeg een tabel in de DWH via TRUNCATE.
    Let op: TRUNCATE reset ook de sequences (indien aanwezig).
    """

    pg_hook = PostgresHook(
        postgres_conn_id="dwh",
        schema="dwh"
    )

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            try:
                sql = f"TRUNCATE TABLE {tbl}"  # identifiers (tabelnaam) kun je niet parameterizen
                print(f"[truncateTable] Uitvoeren: {sql}")
                cursor.execute(sql)
                conn.commit()
                print(f"[truncateTable] Tabel {tbl} succesvol geleegd.")
            except Exception as e:
                conn.rollback()
                print(f"[truncateTable][ERROR] Fout bij TRUNCATE {tbl}: {e}")
                raise


def calcAantalPersonenVoertuig(incident_id: int, voertuigNr: str):
    """
    Haal per incident/voertuig het betrokken voertuig en de personeelsbezetting op uit AG5-tabellen.
    Retourneert dict:
    {
        "incident_id": int,
        "voertuig": { ... } | None,
        "personeel": [ {personname, role, vehicleid}, ... ]
    }
    """
    print(f"[calcAantalPersonenVoertuig] incident_id={incident_id}, voertuigNr={voertuigNr}")

    # Normaliseer roepnaam (bv. 'TS-1234' -> 'TS1234') voor betrouwbare vergelijking
    normalized_callsign = voertuigNr.replace("-", "")
    print(f"[calcAantalPersonenVoertuig] normalized_callsign={normalized_callsign}")

    pg_hook = PostgresHook(postgres_conn_id="dwh", schema="dwh")
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            # 1) Vind AG5-incident-id(s) bij dit LMS-incident
            sql_ag5_ids = """
                SELECT ag5_id
                FROM ag5_incidenten
                WHERE vr_incidentid = %s
            """
            cur.execute(sql_ag5_ids, (incident_id,))
            ag5_ids = [r[0] for r in cur.fetchall() if r and r[0]]
            print(f"[calcAantalPersonenVoertuig] gevonden ag5_ids={ag5_ids}")

            if not ag5_ids:
                print("[calcAantalPersonenVoertuig] Geen AG5 incident gevonden.")
                return {"incident_id": incident_id, "voertuig": None, "personeel": []}

            result = {"incident_id": incident_id, "voertuig": None, "personeel": []}

            # 2) Zoek het voertuig binnen elk AG5-incident op callsign
            sql_vehicle = """
                SELECT vehicleid, ag5_incident_id, callsign, vehicletype, station
                FROM ag5_involvedvehicle
                WHERE ag5_incident_id = %s
                  AND REPLACE(callsign, '-', '') = %s
                LIMIT 1
            """
            for ag5_incident_id in ag5_ids:
                cur.execute(sql_vehicle, (ag5_incident_id, normalized_callsign))
                veh = cur.fetchone()
                if veh:
                    vehicle = {
                        "vehicleid": veh[0],
                        "ag5_incident_id": veh[1],
                        "callsign": veh[2],
                        "vehicletype": veh[3],
                        "station": veh[4],
                    }
                    result["voertuig"] = vehicle
                    print(f"[calcAantalPersonenVoertuig] voertuig gevonden: {vehicle}")

                    # 3) Haal personeelsassignments voor dit voertuig op
                    sql_persons = """
                        SELECT pa.vehicleid, pa.personname, pa.role
                        FROM ag5_personassignments pa
                        WHERE pa.ag5_incident_id = %s
                          AND pa.vehicleid = %s
                          AND pa.vehicleid IS NOT NULL
                    """
                    cur.execute(sql_persons, (ag5_incident_id, veh[0]))
                    rows = cur.fetchall() or []
                    personeel = [
                        {"vehicleid": r[0], "personname": r[1], "role": r[2]}
                        for r in rows
                    ]
                    result["personeel"] = personeel
                    print(f"[calcAantalPersonenVoertuig] aantal personen: {len(personeel)}")
                    break  # eerste match is genoeg

            if result["voertuig"] is None:
                print("[calcAantalPersonenVoertuig] Geen voertuig gevonden voor dit incident/callsign.")

            return result


def _calcOpkomsttijden():
    print("---------- calcOpkomsttijden  -------------")

    pg_hook = PostgresHook(postgres_conn_id="dwh", schema="dwh")
    with pg_hook.get_conn() as dwh:
        with dwh.cursor() as dwh_cursor:
            syncFromVarName = "sync_from_calcOpkomsttijden_incident_id"
            last_incident_id = int(Variable.get(syncFromVarName, 0))
            if last_incident_id < 0:
                last_incident_id = 0

            print(f"[calcOpkomsttijden] start from incident_id={last_incident_id}")

            # Kijk een maand terug vanaf laatste incident
            dwh_cursor.execute(
                "SELECT lms_dtg_start_incident FROM inci_vr_incident WHERE lms_incident_id = %s",
                (last_incident_id,),
            )
            inciRow = dwh_cursor.fetchone()
            if inciRow:
                lms_dtg_start_incident = inciRow[0]
                one_month_ago = lms_dtg_start_incident - timedelta(days=30)
                print(
                    f"[calcOpkomsttijden] laatste incident @ {lms_dtg_start_incident}, terug naar >= {one_month_ago.date()}"
                )
                dwh_cursor.execute(
                    """
                    SELECT lms_incident_id, lms_dtg_start_incident
                    FROM inci_vr_incident
                    WHERE lms_dtg_start_incident >= %s
                    ORDER BY lms_dtg_start_incident ASC
                    LIMIT 1
                    """,
                    (one_month_ago,),
                )
                row = dwh_cursor.fetchone()
                if row:
                    last_incident_id = row[0]
                    print(
                        f"[calcOpkomsttijden] reset pointer -> {last_incident_id} ({row[1]})"
                    )

            limit = 500
            doLoop = True

            while doLoop:
                dwh_cursor.execute(
                    f"""
                    SELECT lms_incident_id, lms_brw_dtg_start_incident,
                           lms_brw_melding_cl, lms_brw_melding_cl1
                    FROM public.inci_vr_incident
                    WHERE lms_incident_id >= %s
                      AND lms_incident_id IN (
                          SELECT incident_id
                          FROM inci_vr_inzet_eenheid
                          WHERE incident_id >= %s
                      )
                    ORDER BY lms_incident_id
                    LIMIT {limit}
                    """,
                    (last_incident_id, last_incident_id),
                )
                inciRows = dwh_cursor.fetchall()
                print(
                    f"[calcOpkomsttijden] batch: {len(inciRows)} rows vanaf {last_incident_id}"
                )

                doLoop = len(inciRows) > 1

                for inci in inciRows:
                    incident_id = inci[0]
                    last_incident_id = incident_id
                    start_dt = inci[1]
                    print(f"[calcOpkomsttijden] incident_id={incident_id}")

                    # Reset vr_dtg_x kolommen
                    dwh_cursor.execute(
                        "UPDATE inci_vr_inzet_eenheid "
                        "SET vr_dtg_ut=NULL, vr_dtg_tp=NULL "
                        "WHERE incident_id=%s",
                        (incident_id,),
                    )

                    # Statushistorie ophalen
                    dwh_cursor.execute(
                        """
                        SELECT dtg_statuswijziging, roepnaam_eenheid,
                               inzet_eenheid_id, status_afk
                        FROM public.inci_vr_status_hist
                        WHERE incident_id = %s
                          AND status_afk IN ('ut','tp','bs','kz')
                        """,
                        (incident_id,),
                    )
                    for dtg, roep, inzet_id, afk in dwh_cursor.fetchall():
                        duur = (dtg - start_dt).total_seconds()
                        mapping = {
                            "ut": ("vr_dtg_ut", "vr_ut_seconden"),
                            "tp": ("vr_dtg_tp", "vr_tp_seconden"),
                            "bs": ("vr_dtg_bs", "vr_bs_seconden"),
                            "kz": ("vr_dtg_kz", "vr_kz_seconden"),
                        }
                        dt_col, sec_col = mapping[afk]
                        dwh_cursor.execute(
                            f"""
                            UPDATE inci_vr_inzet_eenheid
                            SET {dt_col}=%s, {sec_col}=%s
                            WHERE inzet_eenheid_id=%s
                              AND incident_id=%s
                            """,
                            (dtg, int(duur), inzet_id, incident_id),
                        )

                    # Per eenheid bereken BV/CHF/aantal
                    dwh_cursor.execute(
                        """
                        SELECT inzet_eenheid_id, roepnaam_eenheid,
                               code_voertuigsoort, kaz_naam,
                               vr_ut_seconden, vr_tp_seconden
                        FROM public.inci_vr_inzet_eenheid
                        WHERE incident_id = %s
                        """,
                        (incident_id,),
                    )
                    for inzet_eenheid_id, roepnaam, _, _, ut_sec, tp_sec in dwh_cursor.fetchall():
                        aanrijtijd = tp_sec - ut_sec if ut_sec and tp_sec else None

                        dwh_cursor.execute(
                            """
                            SELECT pa.vehicleid, pa.personname, pa.role
                            FROM ag5_personassignments pa
                            JOIN ag5_involvedvehicle iv
                              ON pa.vehicleid = iv.vehicleid
                             AND pa.ag5_incident_id = iv.ag5_incident_id
                            WHERE LEFT(REPLACE(iv.callsign, '-', ''), 6) = %s
                              AND pa.vehicleid IS NOT NULL
                              AND pa.vr_incidentid = %s
                            """,
                            (roepnaam, incident_id),
                        )
                        BV = False
                        CHF = False
                        count = 0
                        for _, _, role in dwh_cursor.fetchall():
                            count += 1
                            role_lower = (role or "").lower()
                            if "bevelvoerder" in role_lower:
                                BV = True
                            if "chauffeur" in role_lower:
                                CHF = True

                        dwh_cursor.execute(
                            """
                            UPDATE inci_vr_inzet_eenheid
                            SET has_BV=%s, has_CHF=%s,
                                vr_aanrijtijd_seconden=%s,
                                aantal_personen_totaal=%s
                            WHERE inzet_eenheid_id=%s
                              AND incident_id=%s
                            """,
                            (BV, CHF, aanrijtijd, count, inzet_eenheid_id, incident_id),
                        )
                        print(
                            f"[calcOpkomsttijden] eenheid={roepnaam}, BV={BV}, CHF={CHF}, count={count}, aanrijtijd={aanrijtijd}"
                        )

                    dwh.commit()

                # Pointer update pas NA batch commit
                Variable.set(syncFromVarName, last_incident_id)
                print(f"[calcOpkomsttijden] sync pointer -> {last_incident_id}")

                dwh.commit()
                time.sleep(0.05)

    print("[calcOpkomsttijden] Klaar")

def _collectStatusHist(viewvoorvoegsel):
    print("---------- collectStatusHist (viewvoorvoegsel=", viewvoorvoegsel, ") -------------")

    # Batchgrootte per bron
    limit = 1000

    # Pointer-variabele
    syncFromVarName = f"sync_from_{viewvoorvoegsel}_collectStatusHist_status_hist_id"
    last_status_hist_id = int(Variable.get(syncFromVarName, 0))
    print(f"[collectStatusHist] start pointer: {last_status_hist_id} (Variable={syncFromVarName})")

    # DWH-connectie (blijft hergebruikt binnen de functie)
    dwh_hook = PostgresHook(postgres_conn_id="dwh", schema="dwh")
    dwh_conn = dwh_hook.get_conn()
    dwh_cur  = dwh_conn.cursor()
    print("[collectStatusHist] Connected to DWH via PostgresHook")

    total_written = 0

    while True:
        # ------- Batch ophalen uit VDB (met retries/backoff + parameters) -------
        sql_vdb = f"""
            SELECT
              status_hist_id, inzet_eenheid_id, incident_id, status_code, ind_handm_aut,
              status_afk, reden_interne_status, meldkamer, bron, roepnaam_eenheid,
              prim_inzetrol, kaz_afk, dtg_aanmaak, dtg_statuswijziging
            FROM {vdb_schema}.{viewvoorvoegsel}_status_hist
            WHERE meldkamer = %s
              AND status_hist_id > %s
            ORDER BY status_hist_id
            LIMIT {limit}
        """
        print(f"[collectStatusHist] VDB query (start_from={last_status_hist_id}, limit={limit})")
        rows = vdb_fetch_batch(sql_vdb, params=(meldkamerCode, last_status_hist_id), attempts=3, backoff_first=1.0)

        if not rows:
            print(f"[collectStatusHist] Geen verdere rows; klaar. (last_id={last_status_hist_id})")
            break

        print(f"[collectStatusHist] Batch size: {len(rows)} | first_id={rows[0][0]} last_id={rows[-1][0]}")

        # ------- Schrijven naar DWH -------
        batch_first_id = rows[0][0]
        batch_last_id  = rows[-1][0]
        batch_written  = 0

        for r in rows:
            (
                status_hist_id, inzet_eenheid_id, incident_id, status_code, ind_handm_aut,
                status_afk, reden_interne_status, meldkamer, bron, roepnaam_eenheid,
                prim_inzetrol, kaz_afk, dtg_aanmaak, dtg_statuswijziging
            ) = r

            # Delete+insert (zelfde semantiek als je originele code)
            dwh_cur.execute(
                "DELETE FROM public.inci_vr_status_hist WHERE status_hist_id = %s",
                (status_hist_id,)
            )
            dwh_cur.execute(
                """
                INSERT INTO public.inci_vr_status_hist (
                    status_hist_id, inzet_eenheid_id, incident_id, status_code, ind_handm_aut,
                    status_afk, reden_interne_status, meldkamer, bron, roepnaam_eenheid,
                    prim_inzetrol, kaz_afk, dtg_aanmaak, dtg_statuswijziging
                ) VALUES (
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s
                )
                """,
                (
                    status_hist_id, inzet_eenheid_id, incident_id, status_code, ind_handm_aut,
                    status_afk, reden_interne_status, meldkamer, bron, roepnaam_eenheid,
                    prim_inzetrol, kaz_afk, dtg_aanmaak, dtg_statuswijziging
                )
            )
            batch_written += 1
            total_written += 1

            # “in-memory” pointer alvast vooruit voor de volgende select
            last_status_hist_id = status_hist_id

            # Optioneel compacte voortgang
            if batch_written % 500 == 0:
                print(f"[collectStatusHist] .. wrote {batch_written}/{len(rows)} in batch; last_id={last_status_hist_id}")

        # Commit de batch en zet dan pas de Variable (striktere pointer-semantiek)
        dwh_conn.commit()
        Variable.set(syncFromVarName, last_status_hist_id)
        print(f"[collectStatusHist] COMMIT batch OK. first_id={batch_first_id} last_id={batch_last_id} (written={batch_written}) | total_written={total_written}")
        # kleine adempauze tegen rate-limits
        time.sleep(0.5)

    # ------- Pointer “terugzetten” (rolling window op dtg_aanmaak, Teiid-safe timestamps) -------
    try:
        # Haal timestamp van de laatst verwerkte status uit VDB
        sql_dt = f"""
            SELECT dtg_aanmaak
            FROM {vdb_schema}.{viewvoorvoegsel}_status_hist
            WHERE status_hist_id = %s
        """
        dt_row = vdb_fetch_batch(sql_dt, params=(last_status_hist_id,), attempts=2, backoff_first=0.8)

        if dt_row and dt_row[0][0]:
            dt_last_str = safe_ts_str(dt_row[0][0])  # 'YYYY-MM-DD HH:MM:SS'
            dt_last     = datetime.strptime(dt_last_str, "%Y-%m-%d %H:%M:%S")
            dt_minus    = dt_last - timedelta(days=daysBackInTime)
            dt_minus_str= dt_minus.strftime("%Y-%m-%d %H:%M:%S")

            # Zoek de dichtstbijzijnde oudere id (<= dt_minus) op dtg_aanmaak
            sql_back = f"""
                SELECT status_hist_id
                FROM {vdb_schema}.{viewvoorvoegsel}_status_hist
                WHERE meldkamer = %s
                  AND dtg_aanmaak <= %s
                ORDER BY status_hist_id DESC
                LIMIT 1
            """
            back_row = vdb_fetch_batch(sql_back, params=(meldkamerCode, dt_minus_str), attempts=2, backoff_first=0.8)

            if back_row:
                rollback_id = int(back_row[0][0])
                Variable.set(syncFromVarName, rollback_id)
                print(f"[collectStatusHist] Pointer teruggezet naar {rollback_id} (op/bij {dt_minus_str})")
            else:
                print("[collectStatusHist][WARN] Geen rollback-pointer gevonden; pointer blijft op latest.")
        else:
            print("[collectStatusHist][WARN] Geen dtg_aanmaak bij laatste status; rollback-pointer overgeslagen.")
    except Exception as e:
        print(f"[collectStatusHist][WARN] pointer-rollback overgeslagen: {e}")

    print(f"{total_written} statusregels gevonden/geschreven voor {viewvoorvoegsel}")

def _syncObjectAlarm():
    syncSimpleTable('beh_object', 'object_id', 'public.inci_vr_beh_object')
    syncSimpleTable('beh_object_alarmhistorie', 'object_alarmhistorie_id', 'public.inci_vr_beh_object_alarmhistorie')
    syncSimpleTable('beh_object_adr', 'object_adres_id', 'public.inci_vr_beh_object_adr')

def _getDekkingsplan():
    showDebugInfo = False
    print("---------- getDekkingsplan  -------------")

    search_descriptions = {
        "celfunctie",
        "gebouwen voor niet-zelfredzame personen",
        "woonfunctie portiekflats",
        "woonfunctie portiekwoningen",
        "woongebouw hoger dan 20 meter",
    }

    syncFromVarName = "sync_from_getDekkingsplan_incident_id"
    last_incident_id = int(Variable.get(syncFromVarName, 0))
    if last_incident_id < 0:
        last_incident_id = 0

    limit = 500
    print(f"[getDekkingsplan] start from incident_id={last_incident_id}, limit={limit}")

    pg_hook = PostgresHook(postgres_conn_id="dwh", schema="dwh")
    with pg_hook.get_conn() as dwh:
        with dwh.cursor() as cur, dwh.cursor() as cur2:
            doLoop = True
            while doLoop:
                # Batch ophalen van incidenten met adresinfo
                query_inci = f"""
                    SELECT lms_incident_id,
                           lms_brw_dtg_start_incident,
                           lms_postcode,
                           lms_huis_paal_nr,
                           lms_huis_nr_toev
                    FROM public.inci_vr_incident
                    WHERE lms_incident_id >= %s
                      AND lms_incident_id IN (
                          SELECT incident_id
                          FROM inci_vr_inzet_eenheid
                          WHERE incident_id >= %s
                      )
                      AND lms_postcode IS NOT NULL AND lms_postcode <> ''
                      AND lms_huis_paal_nr IS NOT NULL
                    ORDER BY lms_incident_id
                    LIMIT {limit}
                """
                cur.execute(query_inci, (last_incident_id, last_incident_id))
                inciRows = cur.fetchall()

                print(f"[getDekkingsplan] batch size={len(inciRows)} vanaf id={last_incident_id}")
                if len(inciRows) <= 1:
                    doLoop = False

                batch_first = inciRows[0][0] if inciRows else None
                batch_last = inciRows[-1][0] if inciRows else None
                if batch_first is not None:
                    print(f"[getDekkingsplan] batch ids: first={batch_first}, last={batch_last}")

                for (lms_incident_id,
                     lms_brw_dtg_start_incident,
                     lms_postcode,
                     lms_huis_paal_nr,
                     lms_huis_nr_toev) in inciRows:

                    last_incident_id = lms_incident_id  # pointer binnen de batch

                    # Defaults
                    omschrijving = None
                    buurtcode = None
                    opkomsttijd_ts1_anwflex = None
                    AandachtsObject = True  # default: behandel als AO
                    dekPlanBuurtRappCat = None
                    dekPlanBuurtRappBuNaam = None
                    dekPlanBuurtRappWKCode = None
                    dekPlanBuurtRappWaardering = None
                    opkomsttijd_ts1_ts6 = None
                    opkomsttijd_ts2_ts6 = None
                    opkomsttijd_ts1_ts4 = None

                    # --- dekkingsplan_objectselectie_anw_ts4 ---
                    if lms_huis_nr_toev is not None and str(lms_huis_nr_toev).strip():
                        cur2.execute(
                            """
                            SELECT omschrijving, buurtcode, opkomsttijd_ts1
                            FROM dekkingsplan_objectselectie_anw_ts4
                            WHERE postcode = %s AND huisnummer = %s AND toevoeging = %s
                            """,
                            (lms_postcode, lms_huis_paal_nr, lms_huis_nr_toev),
                        )
                    else:
                        cur2.execute(
                            """
                            SELECT omschrijving, buurtcode, opkomsttijd_ts1
                            FROM dekkingsplan_objectselectie_anw_ts4
                            WHERE postcode = %s AND huisnummer = %s
                            """,
                            (lms_postcode, lms_huis_paal_nr),
                        )
                    row = cur2.fetchone()
                    if row:
                        omschrijving, buurtcode, opkomsttijd_ts1_anwflex = row
                        if showDebugInfo and buurtcode:
                            print(f"   [anw_ts4] buurtcode: {buurtcode}")

                        if omschrijving and omschrijving.lower() in search_descriptions:
                            AandachtsObject = True
                            if showDebugInfo:
                                print("   Object type: Aandachtsobject")
                        else:
                            AandachtsObject = False
                            if showDebugInfo:
                                print("   Object type: Geen Aandachtsobject")

                        # Buurtrapport ophalen
                        if buurtcode:
                            cur2.execute(
                                """
                                SELECT categorie, bu_naam, wk_code, waardering
                                FROM dekkingsplan_buurtenrapport_anw_ts4
                                WHERE bu_code = %s
                                """,
                                (buurtcode,),
                            )
                            b = cur2.fetchone()
                            if b:
                                (dekPlanBuurtRappCat,
                                 dekPlanBuurtRappBuNaam,
                                 dekPlanBuurtRappWKCode,
                                 dekPlanBuurtRappWaardering) = b
                                if showDebugInfo:
                                    print(f"   [bu_rap] {dekPlanBuurtRappCat} : {dekPlanBuurtRappBuNaam}")

                    # --- dekkingsplan_objectselectie_aandachtsobjecten ---
                    if lms_huis_nr_toev is not None and str(lms_huis_nr_toev).strip():
                        cur2.execute(
                            """
                            SELECT opkomsttijd_ts1_ts6, opkomsttijd_ts2_ts6, opkomsttijd_ts1_ts4
                            FROM dekkingsplan_objectselectie_aandachtsobjecten
                            WHERE postcode = %s AND huisnummer = %s AND toevoeging = %s
                            """,
                            (lms_postcode, lms_huis_paal_nr, lms_huis_nr_toev),
                        )
                    else:
                        cur2.execute(
                            """
                            SELECT opkomsttijd_ts1_ts6, opkomsttijd_ts2_ts6, opkomsttijd_ts1_ts4
                            FROM dekkingsplan_objectselectie_aandachtsobjecten
                            WHERE postcode = %s AND huisnummer = %s
                            """,
                            (lms_postcode, lms_huis_paal_nr),
                        )
                    row = cur2.fetchone()
                    if row:
                        opkomsttijd_ts1_ts6, opkomsttijd_ts2_ts6, opkomsttijd_ts1_ts4 = row
                        if showDebugInfo:
                            print(f"   [ao] ts1_ts6={opkomsttijd_ts1_ts6}, ts2_ts6={opkomsttijd_ts2_ts6}, ts1_ts4={opkomsttijd_ts1_ts4}")

                    # Updaten van incidentrecord
                    cur2.execute(
                        """
                        UPDATE inci_vr_incident
                           SET AandachtsObject = %s,
                               objDescription = %s,
                               dekPlanBuurtRappCat = %s,
                               dekPlanBuurtRappBuNaam = %s,
                               dekPlanBuurtRappWKCode = %s,
                               dekPlanBuurtRappWaardering = %s,
                               dekPlanBuurtRappBUCode = %s,
                               opkomsttijd_ts1_anwflex = %s,
                               opkomsttijd_ts1_ts6 = %s,
                               opkomsttijd_ts2_ts6 = %s,
                               opkomsttijd_ts1_ts4 = %s
                         WHERE lms_incident_id = %s
                        """,
                        (
                            AandachtsObject,
                            omschrijving,
                            dekPlanBuurtRappCat,
                            dekPlanBuurtRappBuNaam,
                            dekPlanBuurtRappWKCode,
                            dekPlanBuurtRappWaardering,
                            buurtcode,
                            opkomsttijd_ts1_anwflex,
                            opkomsttijd_ts1_ts6,
                            opkomsttijd_ts2_ts6,
                            opkomsttijd_ts1_ts4,
                            lms_incident_id,
                        ),
                    )

                # Batch commit
                dwh.commit()

                # Pointer: kies incident ~N dagen vóór het laatst verwerkte incident
                try:
                    cur2.execute(
                        "SELECT lms_dtg_start_incident FROM public.inci_vr_incident WHERE lms_incident_id = %s",
                        (last_incident_id,),
                    )
                    row = cur2.fetchone()
                    if row and row[0]:
                        datumTijd = row[0]
                        days_before = datumTijd - timedelta(days=daysBackInTime)
                        cur2.execute(
                            """
                            SELECT lms_incident_id
                            FROM public.inci_vr_incident
                            WHERE lms_dtg_start_incident <= %s
                            ORDER BY lms_incident_id DESC
                            LIMIT 1
                            """,
                            (days_before,),
                        )
                        row2 = cur2.fetchone()
                        if row2 and row2[0]:
                            new_syncFrom_incident_id = int(row2[0])
                            Variable.set(syncFromVarName, new_syncFrom_incident_id)
                            print(f"[getDekkingsplan] sync pointer -> {new_syncFrom_incident_id} (<= {days_before})")
                        else:
                            print("[getDekkingsplan][WARN] geen oudere incident_id gevonden voor pointer-reset; pointer ongewijzigd.")
                    else:
                        print("[getDekkingsplan][WARN] geen lms_dtg_start_incident gevonden voor laatst id; pointer ongewijzigd.")
                except Exception as e:
                    print(f"[getDekkingsplan][WARN] pointer-reset mislukt: {e}")

                time.sleep(0.05)

    print("[getDekkingsplan] Klaar")

def _collectKarakteristieken(viewvoorvoegsel: str):
    """
    Haalt batches uit VDB ({vdb_schema}.{viewvoorvoegsel}_inzet_kar) en schrijft
    ze naar DWH (public.inci_vr_inzet_kar).

    - Verse VDB-conn/cursor via vdb_fetch_batch(sql, params, attempts, backoff_first)
    - 2–3 retries + kleine backoff binnen vdb_fetch_batch
    - Pointer (Airflow Variable) pas updaten na succesvolle batch-commit
    - Uitvoerige logging: batch size, first/last id
    - Eind: pointer terugzetten op basis van dtg_reg_waarde (rolling window), met safe_ts_str
    """
    print(f"---------- collectKarakteristieken viewvoorvoegsel: {viewvoorvoegsel} -------------")

    # Batchgrootte
    limit = 50000 if viewvoorvoegsel == "ARC" else 1000

    # Sync-pointer ophalen
    syncFromVarName = f"sync_from_{viewvoorvoegsel}_collectKarakteristieken_karakteristiek_id"
    last_karakteristiek_id = int(Variable.get(syncFromVarName, 0))
    if last_karakteristiek_id < 0:
        last_karakteristiek_id = 0

    print(f"[collectKarakteristieken] start_from id={last_karakteristiek_id}, limit={limit}")

    # DWH-verbinding
    dwh_hook = PostgresHook(postgres_conn_id="dwh", schema="dwh")
    with dwh_hook.get_conn() as dwh:
        with dwh.cursor() as cur:
            total_written = 0

            while True:
                # ---------- Batch uit VDB ophalen (parametrized + retries) ----------
                sql_batch = (
                    f"SELECT karakteristiek_id, incident_id, naam_karakteristiek, actuele_kar_waarde, "
                    f"       bron, meldkamer, ind_disc_incident, pol_regio_nr, dtg_reg_waarde "
                    f"FROM {vdb_schema}.{viewvoorvoegsel}_inzet_kar "
                    f"WHERE karakteristiek_id > %s AND meldkamer = %s "
                    f"ORDER BY karakteristiek_id "
                    f"LIMIT {limit}"
                )
                print(
                    f"[collectKarakteristieken] VDB query: "
                    f"{vdb_schema}.{viewvoorvoegsel}_inzet_kar > {last_karakteristiek_id} "
                    f"mk={meldkamerCode} LIMIT={limit}"
                )

                try:
                    statRows = vdb_fetch_batch(
                        sql_batch,
                        params=(last_karakteristiek_id, meldkamerCode),
                        attempts=3,
                        backoff_first=1.0,
                    )
                except Exception as e:
                    print(f"[collectKarakteristieken][ERROR] batch fetch faalde na retries: {e}")
                    raise

                batch_size = len(statRows)
                print(f"[collectKarakteristieken] batch size={batch_size} (from={last_karakteristiek_id})")

                if batch_size == 0:
                    print("[collectKarakteristieken] geen rijen meer; klaar.")
                    break

                batch_first = statRows[0][0]
                batch_last = statRows[-1][0]
                print(f"[collectKarakteristieken] batch ids: first={batch_first}, last={batch_last}")

                # ---------- Schrijven naar DWH ----------
                for idx, kar in enumerate(statRows, start=1):
                    karakteristiek_id = int(kar[0])
                    incident_id = kar[1]

                    # DELETE (geparameteriseerd) – gedrag gelijk aan origineel
                    cur.execute(
                        "DELETE FROM public.inci_vr_inzet_kar WHERE karakteristiek_id = %s",
                        (karakteristiek_id,),
                    )

                    # INSERT (geparameteriseerd)
                    cur.execute(
                        """
                        INSERT INTO public.inci_vr_inzet_kar
                          (karakteristiek_id, incident_id, naam_karakteristiek, actuele_kar_waarde,
                           bron, meldkamer, ind_disc_incident, pol_regio_nr, dtg_reg_waarde)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            kar[0],  # karakteristiek_id
                            kar[1],  # incident_id
                            kar[2],  # naam_karakteristiek
                            kar[3],  # actuele_kar_waarde
                            kar[4],  # bron
                            kar[5],  # meldkamer
                            kar[6],  # ind_disc_incident
                            kar[7],  # pol_regio_nr
                            kar[8],  # dtg_reg_waarde
                        ),
                    )

                    if idx % 5000 == 0 or idx == batch_size:
                        ts_dbg = kar[8] if kar[8] is not None else "-"
                        print(
                            f"[collectKarakteristieken] .. wrote #{idx}/{batch_size} "
                            f"id={karakteristiek_id} inc={incident_id} dtg={ts_dbg}"
                        )

                    # Pointer in-memory bijwerken voor volgende select
                    last_karakteristiek_id = karakteristiek_id
                    total_written += 1

                # Batch commit + daarna pas Variable.set (striktere pointer)
                dwh.commit()
                Variable.set(syncFromVarName, last_karakteristiek_id)
                print(
                    f"[collectKarakteristieken] COMMIT batch OK. "
                    f"last_id={last_karakteristiek_id} (batch={batch_size}, total={total_written})"
                )

                # Als batch kleiner dan limit → klaar; anders volgende batch
                if batch_size < limit:
                    print("[collectKarakteristieken] laatste batch was kleiner dan limit; klaar.")
                    break

    # ---------- Pointer “terugzetten” (rolling window) ----------
    try:
        # 1) Laatste timestamp ophalen
        sql_last_dtg = (
            f"SELECT dtg_reg_waarde "
            f"FROM {vdb_schema}.{viewvoorvoegsel}_inzet_kar "
            f"WHERE karakteristiek_id = %s"
        )
        rows = vdb_fetch_batch(sql_last_dtg, params=(last_karakteristiek_id,), attempts=2)
        if not rows or not rows[0] or rows[0][0] is None:
            print("[collectKarakteristieken][WARN] geen dtg_reg_waarde gevonden voor pointer-reset.")
            return

        datumTijd = rows[0][0]
        days_before = datumTijd - timedelta(days=daysBackInTime)
        days_before_str = safe_ts_str(days_before)  # 'YYYY-MM-DD HH:MM:SS' i.p.v. ISO 'T'

        # 2) Oudere id zoeken t/m die datum
        sql_prev_id = (
            f"SELECT karakteristiek_id "
            f"FROM {vdb_schema}.{viewvoorvoegsel}_inzet_kar "
            f"WHERE meldkamer = %s AND dtg_reg_waarde <= %s "
            f"ORDER BY karakteristiek_id DESC "
            f"LIMIT 1"
        )
        prev_rows = vdb_fetch_batch(sql_prev_id, params=(meldkamerCode, days_before_str), attempts=2)
        if not prev_rows or not prev_rows[0] or prev_rows[0][0] is None:
            print("[collectKarakteristieken][WARN] geen oudere id gevonden voor pointer-reset.")
            return

        new_sync_from = int(prev_rows[0][0])
        Variable.set(syncFromVarName, new_sync_from)
        print(
            f"[collectKarakteristieken] pointer teruggezet naar {new_sync_from} (<= {days_before_str})"
        )
    except Exception as e:
        print(f"[collectKarakteristieken][WARN] pointer-rollback overgeslagen: {e}")


def _calcGripFromKladblok():
    """
    Bereken max GRIP-niveau per incident op basis van kladblokregels
    en update public.inci_vr_incident.vr_grip_max.

    - Start vanaf incident_id bepaald via datum (14 dagen terug vanaf laatste sync)
    - Leest kladblokregels, zoekt naar '-grip X' en bepaalt maxGrip
    - Schrijft maxGrip terug in DWH
    - Update syncFromVarName pointer (datum) pas na commit
    """
    print("---------- calcGripFromKladblok -------------")

    pg_hook = PostgresHook(postgres_conn_id="dwh", schema="dwh")
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur_inci, conn.cursor() as cur_kb:
            syncFromVarName = "calcGripFromKladblokLastSyncDate"
            huidige_datum = datetime.now().date()

            # Startdatum ophalen
            last_sync_str = Variable.get(syncFromVarName, huidige_datum.strftime("%Y-%m-%d"))
            last_sync_date = datetime.strptime(last_sync_str, "%Y-%m-%d").date()
            datumVan = (last_sync_date - timedelta(days=14)).strftime("%Y-%m-%d")

            # Haal eerste incident-id sinds datumVan
            cur_inci.execute(
                """
                SELECT lms_incident_id
                FROM public.inci_vr_incident
                WHERE lms_dtg_start_incident >= %s
                ORDER BY lms_incident_id
                LIMIT 1
                """,
                (datumVan,),
            )
            row = cur_inci.fetchone()
            prev_lms_incident_id = row[0] if row else 0

            doLoop = True
            while doLoop:
                print(f"[calcGrip] Start van prev_lms_incident_id: {prev_lms_incident_id}")

                # Pak max 1000 incidents met kladblokregels die "grip" bevatten
                cur_inci.execute(
                    """
                    SELECT DISTINCT ON (i.lms_incident_id)
                           i.lms_incident_id, i.vr_viewvoorvoegsel, i.lms_brw_melding_cl2,
                           i.lms_plaats_naam, i.lms_dtg_start_incident
                    FROM public.inci_vr_incident i
                    INNER JOIN public.inci_vr_kladblok kb
                        ON i.lms_incident_id = kb.incident_id
                    WHERE i.lms_incident_id > %s
                      AND kb.inhoud_kladblok_regel ILIKE '%%-grip%%'
                    ORDER BY i.lms_incident_id
                    LIMIT 1000
                    """,
                    (prev_lms_incident_id,),
                )
                inciRows = cur_inci.fetchall()

                if not inciRows:
                    print("[calcGrip] STOP LOOPING: geen nieuwe incidents gevonden.")
                    doLoop = False
                    break

                print(f"[calcGrip] Found {len(inciRows)} incidents with GRIP in kladblok")

                for inci in inciRows:
                    incident_id, _, _, _, dtg_start = inci
                    prev_lms_incident_id = incident_id
                    maxGrip = 0

                    # Lees alle kladblokregels voor dit incident
                    cur_kb.execute(
                        """
                        SELECT inhoud_kladblok_regel
                        FROM public.inci_vr_kladblok
                        WHERE incident_id = %s
                        ORDER BY kladblok_regel_id, volg_nr_kladblok_regel
                        """,
                        (incident_id,),
                    )
                    kbRows = cur_kb.fetchall()

                    for (txt,) in kbRows:
                        if not txt:
                            continue
                        kbTxt = txt.strip().lower()

                        # Check patronen
                        if "-grip1" in kbTxt or "-grip 1" in kbTxt:
                            maxGrip = max(maxGrip, 1)
                        elif "-grip2" in kbTxt or "-grip 2" in kbTxt:
                            maxGrip = max(maxGrip, 2)
                        elif "-grip3" in kbTxt or "-grip 3" in kbTxt:
                            maxGrip = max(maxGrip, 3)
                        elif "-grip4" in kbTxt or "-grip 4" in kbTxt:
                            maxGrip = max(maxGrip, 4)

                    # Update incident-record
                    cur_kb.execute(
                        "UPDATE public.inci_vr_incident SET vr_grip_max = %s WHERE lms_incident_id = %s",
                        (maxGrip, incident_id),
                    )
                    conn.commit()

                    print(f"[calcGrip] incident {incident_id} @ {dtg_start} -> maxGrip={maxGrip}")

                    # Pointer updaten naar laatst verwerkte datum
                    Variable.set(syncFromVarName, dtg_start.strftime("%Y-%m-%d"))

            # Zet pointer op huidige datum als alles klaar is
            Variable.set(syncFromVarName, huidige_datum.strftime("%Y-%m-%d"))
            print(f"[calcGrip] sync pointer gezet op {huidige_datum}")



with DAG(
    dag_id='lmsvdb_incidenten_ARC_zonderobcd',
    doc_md=doc_md,
    schedule_interval='0 */2 * * *',
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    concurrency=1,
    catchup=False
) as dag:

    #tmp = PythonOperator(hhh
    #    task_id = 'tmp',
    #    python_callable = _tmp
    #)
    
    #showTableCount = PythonOperator(
    #    task_id = 'showTableCount',
    #    python_callable = _showTableCount
    #)

    #doTestQuery = PythonOperator(
    #    task_id = 'doTestQuery',
    #    python_callable = _doTestQuery
    #)


    testDBConnection = PythonOperator(
        task_id = 'testDBConnection',
        python_callable = _testDBConnection
    )

    syncIncidentData_ARC = PythonOperator(
        task_id = 'syncIncidentData',
        python_callable = _syncIncidents,
        op_kwargs={'tbl_voorvoegsel': 'ARC'}
    )


    collectMelding_ARC = PythonOperator(
        task_id = 'collectMelding',
        python_callable = _collectMelding,
        op_kwargs={'viewvoorvoegsel': 'ARC'}
    )

    collectKladblokRegels_ARC = PythonOperator(
        task_id = 'collectKladblokRegels',
        python_callable = _collectKladblokRegels,
        op_kwargs={'viewvoorvoegsel': 'ARC'}
    )

    collectKarakteristieken_ARC = PythonOperator(
        task_id = 'collectKarakteristieken',
        python_callable = _collectKarakteristieken,
        op_kwargs={'viewvoorvoegsel': 'ARC'}
    )

    collectInzetEenheid_ARC = PythonOperator(
        task_id = 'collectInzetEenheid',
        python_callable = _collectInzetEenheid,
        op_kwargs={'viewvoorvoegsel': 'ARC'}
    )

    collectStatusHist_ARC = PythonOperator(
        task_id = 'collectStatusHist',
        retries = 5,
        retry_delay = timedelta(minutes=2),
        python_callable = _collectStatusHist,
        op_kwargs={'viewvoorvoegsel': 'ARC'}
    )

    calcGripFromKladblok = PythonOperator(
        task_id = 'calcGripFromKladblok',
        python_callable = _calcGripFromKladblok
    )

    #calcDCUFromKladblok = PythonOperator(
    #    task_id = 'calcDCUFromKladblok',
    #    python_callable = _calcDCUFromKladblok
    #)

    calcBrandGrootte_Gaslek_dcu_fromKB = PythonOperator(
        task_id = 'calcBrandGrootte_Gaslek_dcu_fromKB',
        python_callable = _calcBrandGrootte_Gaslek_dcu_fromKB
    )

    doAg5Sync = PythonOperator(
        task_id = 'doAg5Sync',
        python_callable = _doAg5Sync
    )

    calcOpkomsttijden = PythonOperator(
        task_id = 'calcOpkomsttijden',
        python_callable = _calcOpkomsttijden
    )

    getDekkingsplan = PythonOperator(
        task_id = 'getDekkingsplan',
        python_callable = _getDekkingsplan
    )




    begin = EmptyOperator(task_id="begin", trigger_rule="all_done")
    middle = EmptyOperator(task_id="middle", trigger_rule="all_done")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    
    begin >> testDBConnection >> syncIncidentData_ARC >> [collectMelding_ARC, collectKladblokRegels_ARC, collectKarakteristieken_ARC, collectInzetEenheid_ARC, collectStatusHist_ARC, getDekkingsplan] >> middle >> doAg5Sync >> calcOpkomsttijden >> end
    collectKladblokRegels_ARC >> calcGripFromKladblok >> calcBrandGrootte_Gaslek_dcu_fromKB



with DAG(
    dag_id='lmsvdb_incidenten_HST_zonderobcd',
    doc_md=doc_md,
    schedule_interval='@weekly',
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    concurrency=1,
    catchup=False
) as dag:

    testDBConnection = PythonOperator(
        task_id = 'testDBConnection',
        python_callable = _testDBConnection
    )

    syncIncidentData_HST = PythonOperator(
        task_id = 'syncIncidentData',
        python_callable = _syncIncidents,
        op_kwargs={'tbl_voorvoegsel': 'HST'}
    )

    collectMelding_HST = PythonOperator(
        task_id = 'collectMelding',
        python_callable = _collectMelding,
        op_kwargs={'viewvoorvoegsel': 'HST'}
    )

    collectKladblokRegels_HST = PythonOperator(
        task_id = 'collectKladblokRegels',
        python_callable = _collectKladblokRegels,
        op_kwargs={'viewvoorvoegsel': 'HST'}
    )

    collectKarakteristieken_HST = PythonOperator(
        task_id = 'collectKarakteristieken',
        python_callable = _collectKarakteristieken,
        op_kwargs={'viewvoorvoegsel': 'HST'}
    )

    collectInzetEenheid_HST = PythonOperator(
        task_id = 'collectInzetEenheid',
        python_callable = _collectInzetEenheid,
        op_kwargs={'viewvoorvoegsel': 'HST'}
    )

    collectStatusHist_HST = PythonOperator(
        task_id = 'collectStatusHist',
        retries = 5,
        retry_delay = timedelta(minutes=2),
        python_callable = _collectStatusHist,
        op_kwargs={'viewvoorvoegsel': 'HST'}
    )

    calcGripFromKladblok = PythonOperator(
        task_id = 'calcGripFromKladblok',
        python_callable = _calcGripFromKladblok
    )

    #calcDCUFromKladblok = PythonOperator(
    #    task_id = 'calcDCUFromKladblok',
    #    python_callable = _calcDCUFromKladblok
    #)

    calcBrandGrootte_Gaslek_dcu_fromKB = PythonOperator(
        task_id = 'calcBrandGrootte_Gaslek_dcu_fromKB',
        python_callable = _calcBrandGrootte_Gaslek_dcu_fromKB
    )

    doAg5Sync = PythonOperator(
        task_id = 'doAg5Sync',
        python_callable = _doAg5Sync
    )

    begin = EmptyOperator(task_id="begin", trigger_rule="all_done")
    middle = EmptyOperator(task_id="middle", trigger_rule="all_done")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    
    begin >> testDBConnection >> syncIncidentData_HST >> [collectMelding_HST, collectKladblokRegels_HST, collectKarakteristieken_HST, collectInzetEenheid_HST, collectStatusHist_HST] >> middle >> doAg5Sync >> end
    collectKladblokRegels_HST >> calcGripFromKladblok >> calcBrandGrootte_Gaslek_dcu_fromKB


with DAG(
    dag_id='lmsvdb_syncObjectAlarm_zonderobcd',
    doc_md=doc_md,
    schedule_interval='0 */2 * * *',
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    concurrency=1,
    catchup=False
) as dag:

    testDBConnection = PythonOperator(
        task_id = 'testDBConnection',
        python_callable = _testDBConnection
    )

    #syncBehData = PythonOperator(
    #    task_id = 'syncBehData',
    #    python_callable = _syncBehData,
    #)

    syncObjectAlarm = PythonOperator(
        task_id = 'syncObjectAlarm',
        python_callable = _syncObjectAlarm
    )

    begin = EmptyOperator(task_id="begin", trigger_rule="all_done")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    
    begin >> testDBConnection >> syncObjectAlarm >> end

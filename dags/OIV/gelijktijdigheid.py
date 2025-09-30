from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from datetime import datetime, date, timedelta

default_args = {
    'owner': 'OIV',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(0),
}

@dag(
    dag_id='OIV_gelijktijdigheid',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2019, 1, 1),
    catchup=False
)
def incident_pipeline():

    @task()
    def get_not_processed_incident() -> datetime:
        """
        Bepaalt sinds welk tijdstip we nieuwe incidenten moeten ophalen op basis
        van de meest recente tijdsperiode in public.gelijktijdigheid:
        
        1. SELECT MAX(lower(tijdsperiode)) uit gelijktijdigheid.
        2. Als er geen resultaat is: fallback naar 2019-01-01 00:00.
        3. Skip de run als die timestamp vandaag is.
        """
        hook = PostgresHook(postgres_conn_id='dwh')

        # 1) Haal de meest recente start van alle tsranges
        row = hook.get_first(
            """
            SELECT MAX(lower(tijdsperiode))::timestamp
            FROM public.inci_vr_gelijktijdigheid
            """
        )

        if row and row[0]:
            last_ts = row[0]
        else:
            # 2) Eerste run: start op 2019-01-01 00:00
            last_ts = datetime(2019, 1, 1, 0, 0, 0)

        # 3) Skip als die laatste timestamp vandaag is
        if last_ts.date() == date.today():
            raise AirflowSkipException("Latest processed incident is from today.")

        return last_ts.isoformat()

    @task()
    def process_and_store(since: datetime):
        """
        Pusht alle grouping-, JSON-agg- en upsert-logica naar de database:
        1) raw: nieuwe incidenten + hun gelijktijdigen.
        2) enriched: bouwt voor elke rij een gesorteerde group_ids-array.
        3) aggregated: groeperen op group_ids, tsrange bepalen, JSON payload maken (incl. voertuigen).
        4) Upsert in één INSERT … ON CONFLICT.
        5) Variable-update: sla de meest recente tijdsperiode-start op.
        """
        hook = PostgresHook(postgres_conn_id='dwh')

        # 1–4: CTE’s + upsert in één call
        hook.run(
            sql="""
            WITH raw AS (
              SELECT
                g.lms_incident_id,
                g.lms_brw_dtg_start_incident,
                g.lms_brw_dtg_einde_incident_operationeel,
                ia.incidentadres,
                g.lms_brw_melding_cl,
                g.lms_brw_melding_cl1,
                g.lms_brw_melding_cl2,
                g.lms_prioriteit_incident_brandweer,
                g.gelijktijdige_incidenten
              FROM public.view_gelijktijdige_incidenten_operationeel AS g
              JOIN public.view_incidentadres AS ia
                ON g.lms_incident_id = ia.lms_incident_id
              WHERE g.lms_brw_dtg_start_incident > %s
              AND g.lms_brw_dtg_einde_incident_operationeel > g.lms_brw_dtg_start_incident
              ORDER BY g.lms_brw_dtg_start_incident ASC
            ),
            enriched AS (
              SELECT
                r.*,
                gi.group_ids
              FROM raw AS r
              -- 1) via LATERAL de group_ids berekenen
              CROSS JOIN LATERAL (
                SELECT
                  array_agg(id ORDER BY id) AS group_ids
                FROM unnest(
                  array[r.lms_incident_id]
                  || COALESCE(r.gelijktijdige_incidenten, ARRAY[]::int[])
                ) AS id
              ) AS gi
              -- 2) nu kun je filteren op de lengte van die array
              WHERE cardinality(gi.group_ids) > 2
            ),
            aggregated AS (
              SELECT
                tsrange(
                  min(lms_brw_dtg_start_incident),
                  max(lms_brw_dtg_einde_incident_operationeel),
                  '[)'
                ) AS tijdsperiode,
                jsonb_agg(
                  jsonb_build_object(
                    'lms_incident_id',                    r.lms_incident_id,
                    'lms_brw_dtg_start_incident',         to_char(r.lms_brw_dtg_start_incident, 'YYYY-MM-DD"T"HH24:MI:SS'),
                    'lms_brw_dtg_einde_incident_operationeel', to_char(r.lms_brw_dtg_einde_incident_operationeel, 'YYYY-MM-DD"T"HH24:MI:SS'),
                    'incidentadres',                      r.incidentadres,
                    'lms_brw_melding_cl',                 r.lms_brw_melding_cl,
                    'lms_brw_melding_cl1',                r.lms_brw_melding_cl1,
                    'lms_brw_melding_cl2',                r.lms_brw_melding_cl2,
                    'lms_prioriteit_incident_brandweer',  r.lms_prioriteit_incident_brandweer,
                    'voertuigen',
                      COALESCE(
                        (
                          SELECT jsonb_agg(
                                   jsonb_build_object(
                                     'code', ie.code_voertuigsoort,
                                     'rol', ie.rol,
                                     'voertuig', ie.roepnaam_eenheid,
                                     'kazerne', ie.kaz_naam
                                   )
                                 )
                          FROM public.inci_vr_inzet_eenheid AS ie
                          WHERE ie.incident_id = r.lms_incident_id
                        ),
                        '[]'::jsonb
                      )
                  )
                ) AS data
              FROM enriched AS r
              GROUP BY r.group_ids
            )
            INSERT INTO public.inci_vr_gelijktijdigheid (tijdsperiode, data)
            SELECT tijdsperiode, data
            FROM aggregated
            ON CONFLICT (tijdsperiode) DO UPDATE
              SET data = EXCLUDED.data;
            """,
            parameters=(datetime.fromisoformat(since),)
        )
        

        # 5. Variabele bijwerken: zet op de meest recente tijdsperiode-start
        last_period_start = hook.get_first(
            """
            SELECT MAX(lower(tijdsperiode)) AS latest_start
            FROM public.inci_vr_gelijktijdigheid
            WHERE lower(tijdsperiode) > %s
            """,
            parameters=(datetime.fromisoformat(since),)
        )
        if last_period_start and last_period_start[0]:
            latest_start_ts = last_period_start[0]
            Variable.set(
                "starttijd_laatst_behandelde_incidenten",
                latest_start_ts.strftime("%Y-%m-%d %H:%M:%S"),
            )

    # Task-volgorde: bepaal begin-timestamp, verwerk alles in één SQL-statement
    start_ts = get_not_processed_incident()
    process_and_store(start_ts)

incident_pipeline = incident_pipeline()



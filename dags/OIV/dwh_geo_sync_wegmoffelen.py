from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from datetime import datetime, timezone
import re
from typing import List

SRC_CONN_ID = 'dwh'
DST_CONN_ID = 'geoserver'

DEFAULT_START = datetime(1970, 1, 1, tzinfo=timezone.utc)

WHERE_CLAUSE = """
WHERE (
      lms_gemeente_naam IN (
          'Voerendaal', 'Maastricht', 'Sittard-Geleen', 'Simpelveld', 'Eijsden-Margraten',
          'Heerlen', 'Brunssum', 'Gulpen-Wittem', 'Stein', 'Landgraaf', 'Vaals',
          'Beek', 'Valkenburg aan de Geul', 'Meerssen', 'Beekdaelen', 'Kerkrade'
      )
      OR lms_gemeente_naam IS NULL
    )
  AND (
      lms_brw_soort_afsluiting IS NULL
      OR lms_brw_soort_afsluiting NOT IN ('Test', 'Test/oefening')
    )
  AND (
      lms_naam_gebeurtenis IS NULL
      OR lms_naam_gebeurtenis <> 'Oefening'
    )
  AND (
      lms_brw_melding_cl2 IS NULL
      OR lms_brw_melding_cl2 <> 'Oefening'
    )
"""

def load_batches(select_sql: str, insert_sql: str, datum_parameter: str = None, batch_size: int = 2000, create_geom_puntcoordinaat: bool = False):
    contains_placeholder = '%s' in select_sql

    if datum_parameter is not None:
        if not contains_placeholder:
            raise ValueError(
                "Er is een 'datum' meegegeven, maar de SELECT-query bevat geen '%s'-placeholder.")
    else:
        if contains_placeholder:
            raise ValueError(
                "De SELECT-query bevat een '%s'-placeholder, maar er is geen 'datum' doorgegeven.")

    src = PostgresHook(postgres_conn_id=SRC_CONN_ID)
    dst = PostgresHook(postgres_conn_id=DST_CONN_ID)

    with src.get_conn() as src_conn, src_conn.cursor(name='inc_cursor') as src_cur:
        if datum_parameter is not None:
            src_cur.execute(select_sql, (datum_parameter,))
        else:
            src_cur.execute(select_sql)

        with dst.get_conn() as dst_conn, dst_conn.cursor() as dst_cur:
            while True:
                rows = src_cur.fetchmany(batch_size)
                if not rows:
                    break

                if create_geom_puntcoordinaat:
                    kolommen = extract_column_names(select_sql)
                    validate_geom_parameters(kolommen, insert_sql)

                    execute_values(
                        dst_cur,
                        insert_sql,
                        rows,
                        template=generate_insert_template(kolommen),
                        page_size=batch_size
                    )

                else:
                    execute_values(
                        dst_cur,
                        insert_sql,
                        rows,
                        page_size=batch_size
                    )

                dst_conn.commit()

def validate_geom_parameters(kolommen: List[str], insert_sql: str):
    # 1) Controleer vr_lon / vr_lat in gegeven kolommenlijst
    try:
        idx_lon = kolommen.index("vr_lon")
        idx_lat = kolommen.index("vr_lat")
    except ValueError:
        raise ValueError(
            "create_geom_puntcoordinaat=True, maar vr_lon of vr_lat ontbreekt in de SELECT-kolommen."
        )
    if idx_lat != idx_lon + 1:
        raise ValueError(
            "create_geom_puntcoordinaat=True, maar vr_lon en vr_lat staan niet direct achter elkaar in de SELECT-kolommen."
        )

    # 2) Controleer 'geom' in INSERT
    match_ins = re.search(
        r"insert\s+into\s+\S+\s*\(\s*(.*?)\s*\)\s*values",
        insert_sql,
        re.IGNORECASE | re.DOTALL
    )
    if not match_ins:
        raise ValueError(
            "Kan de kolommen in de INSERT-query niet bepalen."
        )
    insert_cols = [col.strip() for col in match_ins.group(1).split(',')]
    if "geom" not in insert_cols:
        raise ValueError(
            "create_geom_puntcoordinaat=True, maar 'geom' ontbreekt in de INSERT-kolommen."
        )

def generate_insert_template(kolommen: List[str]) -> str:
    parts = []
    i = 0
    while i < len(kolommen):
        if kolommen[i] == "vr_lon" and (i + 1) < len(kolommen) and kolommen[i + 1] == "vr_lat":
            parts.append(
                "ST_Transform(\n"
                "  ST_SetSRID(\n"
                "    ST_MakePoint(%s, %s),\n"
                "    4326\n"
                "  ),\n"
                "  28992\n"
                ")"
            )
            i += 2
        else:
            parts.append("%s")
            i += 1

    row_template = "(" + ", ".join(parts) + ")"
    return row_template

def extract_column_names(sql_query: str) -> List[str]:
    # Find SELECT ... FROM part
    match = re.search(r"select\s+(.*?)\s+from", sql_query, re.IGNORECASE | re.DOTALL)
    if not match:
        return []

    select_clause = match.group(1)

    # Split by commas to get individual column expressions
    columns = [col.strip() for col in select_clause.split(',')]

    column_names = []
    for col in columns:
        # Remove any surrounding backticks or brackets
        col_clean = re.sub(r"[`\[\]]", "", col)

        # Check for alias using AS
        alias_match = re.search(r"\s+as\s+(.+)$", col_clean, re.IGNORECASE)
        if alias_match:
            column_names.append(alias_match.group(1).strip())
        else:
            # If no alias, take the part after the last dot if exists
            if '.' in col_clean:
                column_names.append(col_clean.split('.')[-1].strip())
            else:
                column_names.append(col_clean.strip())

    return column_names
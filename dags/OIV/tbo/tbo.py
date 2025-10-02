# airflow dags test tbo
import os
import ssl
from OpenSSL import crypto
from datetime import datetime
from dateutil import parser

from airflow import AirflowException
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

import pyodbc
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError

doc_md = """
### Synchronize Remote TBO MSSQL Tables to Local PostgreSQL Datawarehouse
#### Werking
    Daily synchronization of MSSQL tables to PostgreSQL tables with prefix staging_DM_
"""

# ---- Credentials via environment -----------------------
def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise AirflowException(f"Missing required environment variable: {name}")
    return val

ACCOUNTS = [
    {
        "db_server":   _require_env("DM_GMS_EXPORT_SERVER"),
        "db_database": _require_env("DM_GMS_EXPORT_DATABASE"),
        "db_login":    _require_env("DM_GMS_EXPORT_LOGIN"),
        "db_password": _require_env("DM_GMS_EXPORT_PASSWORD"),
    },
    {
        "db_server":   _require_env("DM_Brandonderzoek_Export_SERVER"),
        "db_database": _require_env("DM_Brandonderzoek_Export_DATABASE"),
        "db_login":    _require_env("DM_Brandonderzoek_Export_LOGIN"),
        "db_password": _require_env("DM_Brandonderzoek_Export_PASSWORD"),
    },
]

def showAllTablesOfSingleConnection(db_login, db_password, db_server, db_database):
    # Masked logging (no secrets in logs)
    print(f"showAllTablesOfSingleConnection: Connect naar {db_login}:***@{db_server}/{db_database}")

    remote_connection_str = (
        f"mssql+pyodbc://{db_login}:{db_password}@{db_server}/{db_database}"
        f"?driver=ODBC+Driver+18+for+SQL+Server"
        f"&TrustServerCertificate=yes"
    )
    remote_engine = create_engine(remote_connection_str)

    try:
        table_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
        tables = pd.read_sql(table_query, remote_engine)
        for table_name in tables['TABLE_NAME']:
            print(f"Table {table_name} found in remote {db_server}/{db_database}.")
    except Exception as e:
        print(f"Failed: {e}")
        raise
    finally:
        remote_engine.dispose()

def sync_tablesOfSingleConnection(db_login, db_password, db_server, db_database):
    remote_connection_str = (
        f"mssql+pyodbc://{db_login}:{db_password}@{db_server}/{db_database}"
        f"?driver=ODBC+Driver+18+for+SQL+Server"
        f"&TrustServerCertificate=yes"
    )
    remote_engine = create_engine(remote_connection_str)

    pg_hook = PostgresHook(postgres_conn_id='dwh', schema='dwh')
    local_engine = pg_hook.get_sqlalchemy_engine()
    inspector = inspect(local_engine)

    unsupported_data_types = {'geometry', 'geography', 'hierarchyid', 'xml', 'sql_variant'}

    try:
        table_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        tables = pd.read_sql(table_query, remote_engine)

        for table_name in tables['TABLE_NAME']:
            local_table_name = f"staging_dm_{table_name.lower()}"

            try:
                # Truncate existing
                if inspector.has_table(local_table_name):
                    with local_engine.connect() as conn:
                        conn.execute(f"TRUNCATE TABLE {local_table_name}")
                        print(f"Truncated table: {local_table_name}")
                else:
                    print(f"Table {local_table_name} does not exist. It will be created.")

                # Build safe SELECT with casts for unsupported types
                metadata_query = f"""
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table_name}'
                """
                columns_df = pd.read_sql(metadata_query, remote_engine)

                column_casts = []
                for _, row in columns_df.iterrows():
                    column_name = row['COLUMN_NAME']
                    data_type = row['DATA_TYPE']
                    if data_type.lower() in unsupported_data_types:
                        column_casts.append(f"CAST([{column_name}] AS VARCHAR(MAX)) AS [{column_name}]")
                    else:
                        column_casts.append(f"[{column_name}]")

                columns_sql = ', '.join(column_casts)
                query = f"SELECT {columns_sql} FROM {table_name}"

                df = pd.read_sql(query, remote_engine)

                print(f"Synchronizing table: {table_name}")
                print(f"Number of rows fetched: {len(df)}")

                if df.empty:
                    print(f"Table {table_name} is empty, skipping...")
                    continue

                with local_engine.begin() as conn:
                    df.to_sql(local_table_name, con=conn, if_exists='append', index=False)

                with local_engine.connect() as conn:
                    result = conn.execute(f"SELECT COUNT(*) FROM {local_table_name}")
                    count = result.scalar()
                    print(f"Table {local_table_name} successfully synchronized with {count} rows.")

            except SQLAlchemyError as e:
                print(f"SQLAlchemy Error during sync of table {table_name}: {e}")
                raise
            except Exception as e:
                print(f"Unexpected error during sync of table {table_name}: {e}")
                raise

    except Exception as e:
        print(f"Failed during the table synchronization process: {e}")
        raise
    finally:
        remote_engine.dispose()
        local_engine.dispose()

def _showAllTables():
    for account in ACCOUNTS:
        showAllTablesOfSingleConnection(
            account['db_login'],
            account['db_password'],
            account['db_server'],
            account['db_database'],
        )

def _sync_tables():
    for account in ACCOUNTS:
        sync_tablesOfSingleConnection(
            account['db_login'],
            account['db_password'],
            account['db_server'],
            account['db_database'],
        )

with DAG(
    dag_id='tbo',
    doc_md=doc_md,
    schedule_interval='0 3 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    showAllTables = PythonOperator(
        task_id='showAllTables',
        python_callable=_showAllTables,
    )

    sync_tables = PythonOperator(
        task_id='sync_tables',
        python_callable=_sync_tables,
    )

    showAllTables, sync_tables

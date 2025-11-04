# """
# ETL DAG: MSSQL → PostgreSQL Table Upsert
# ----------------------------------------
# ✅ Dynamically lists tables from MSSQL
# ✅ Creates PostgreSQL tables if missing
# ✅ UPSERT (merge) new + updated data
# ✅ Converts table + column names to lowercase
# ✅ Runs every 30 min, Mon–Sat, 8 AM–2 AM PKT
# """

# from __future__ import annotations
# import sys, os, logging
# from datetime import datetime, timedelta
# import pandas as pd
# import pendulum, pyodbc
# from airflow.decorators import dag, task
# from airflow.hooks.base import BaseHook
# from airflow.operators.empty import EmptyOperator
# from airflow.utils.trigger_rule import TriggerRule
# from sqlalchemy import create_engine, text
# from sqlalchemy.dialects.postgresql import insert

# # ---------------------------------------------------------------- #
# # CONFIG
# # ---------------------------------------------------------------- #
# PKT = pendulum.timezone("Asia/Karachi")
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2025, 10, 31, 21, 40, tzinfo=PKT),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
#     "execution_timeout": timedelta(hours=6),
#     "catchup": False,
# }

# MSSQL_CONN_ID = "SilverStr"
# POSTGRES_CONN_ID = "pg-ssg"
# INCLUDED_TABLES = ["Coa31", "Employees", "DefDepartments", "OperationBreakDown", "OperationBreakDown_Det"]

# # ---------------------------------------------------------------- #
# # UTILITIES
# # ---------------------------------------------------------------- #
# def build_mssql_conn_str(conn):
#     return (
#         f"DRIVER={{FreeTDS}};SERVER={conn.host};PORT=1433;"
#         f"DATABASE={conn.schema};UID={conn.login};PWD={conn.password};"
#         "TDS_Version=7.0;Connect Timeout=30;Login Timeout=30;"
#     )


# def get_postgres_engine():
#     from urllib.parse import quote_plus
#     try:
#         c = BaseHook.get_connection(POSTGRES_CONN_ID)
#         uri = f"postgresql://{c.login}:{quote_plus(c.password or '')}@{c.host}:{c.port}/{c.schema}"
#         logger.info(f"[PG] Connected via Airflow: {c.host}/{c.schema}")
#     except Exception as e:
#         logger.warning(f"[PG] Airflow conn failed ({e}), using fallback")
#         uri = f"postgresql://postgres:{quote_plus('P@kistan12')}@172.16.7.6:5432/ssg"
#     return create_engine(uri, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600, echo=False)


# def get_mssql_dataframe(table_name: str) -> pd.DataFrame:
#     """Fetch MSSQL table as pandas DataFrame"""
#     conn = BaseHook.get_connection(MSSQL_CONN_ID)
#     conn_str = build_mssql_conn_str(conn)
#     with pyodbc.connect(conn_str, timeout=60) as c:
#         df = pd.read_sql(f"SELECT * FROM {table_name}", c)
#     df.columns = [col.lower() for col in df.columns]
#     return df


# def upsert_dataframe(engine, df: pd.DataFrame, table_name: str):
#     """UPSERT DataFrame to Postgres (on all columns as conflict target)"""
#     if df.empty:
#         logger.warning(f"⚠️ Skipped {table_name} (no data)")
#         return "skipped"

#     table_name = table_name.lower()
#     with engine.begin() as conn:
#         # Create table if missing
#         df.head(0).to_sql(table_name, conn, if_exists="append", index=False)

#         # Load existing structure
#         temp_table = f"{table_name}_temp"
#         df.to_sql(temp_table, conn, if_exists="replace", index=False)

#         # Get columns for merge
#         cols = list(df.columns)
#         updates = ", ".join([f"{col}=EXCLUDED.{col}" for col in cols])

#         # Build and execute UPSERT query
#         insert_sql = f"""
#             INSERT INTO {table_name} ({','.join(cols)})
#             SELECT {','.join(cols)} FROM {temp_table}
#             ON CONFLICT ({','.join(cols)}) DO UPDATE SET {updates};
#             DROP TABLE {temp_table};
#         """
#         try:
#             conn.execute(text(insert_sql))
#             logger.info(f"✅ Upserted {len(df)} rows into {table_name}")
#         except Exception as e:
#             logger.error(f"❌ Upsert failed for {table_name}: {e}")
#             raise


# # ---------------------------------------------------------------- #
# # DAG DEFINITION
# # ---------------------------------------------------------------- #
# @dag(
#     dag_id="data_sync_mssql_to_postgres_upsert",
#     default_args=default_args,
#     schedule="*/30 8-23,0-1 * * 1-6",  # every 30 min, 8AM–2AM, Mon–Sat
#     tags=["mssql", "postgres", "sync", "upsert"],
#     max_active_runs=1,
# )
# def data_sync_mssql_to_postgres_upsert():
#     start = EmptyOperator(task_id="start")
#     end = EmptyOperator(task_id="end")

#     # ---------------- TASK 1: List Tables ---------------- #
#     @task
#     def list_tables() -> list:
#         conn = BaseHook.get_connection(MSSQL_CONN_ID)
#         conn_str = build_mssql_conn_str(conn)
#         with pyodbc.connect(conn_str, timeout=30) as c:
#             cur = c.cursor()
#             q = f"""
#                 SELECT t.name
#                 FROM sys.tables t
#                 WHERE t.name IN ({','.join([f"'{t}'" for t in INCLUDED_TABLES])})
#             """
#             cur.execute(q)
#             tables = [r[0] for r in cur.fetchall()]
#         logger.info(f"✅ Tables to sync: {tables}")
#         return tables

#     # ---------------- TASK 2: Upsert Each Table ---------------- #
#     @task
#     def sync_table(table_name: str) -> str:
#         try:
#             df = get_mssql_dataframe(table_name)
#             engine = get_postgres_engine()
#             upsert_dataframe(engine, df, table_name)
#             return f"✅ {table_name}: {len(df)} rows upserted"
#         except Exception as e:
#             logger.error(f"❌ {table_name} failed: {e}")
#             return f"❌ {table_name}: Failed - {e}"

#     # ---------------- TASK 3: Summary ---------------- #
#     @task(trigger_rule=TriggerRule.ALL_DONE)
#     def summarize(results: list):
#         success = sum("✅" in r for r in results)
#         failed = sum("❌" in r for r in results)
#         logger.info("📊 SYNC SUMMARY")
#         logger.info(f"✅ Success: {success} | ❌ Failed: {failed}")
#         return {"success": success, "failed": failed}

#     # ---------------- PIPELINE ---------------- #
#     tables = list_tables()
#     results = sync_table.expand(table_name=tables)
#     summarize(results)
#     start >> tables >> results >> end


# dag = data_sync_mssql_to_postgres_upsert()

#!/usr/bin/env python3
"""
etl_operator_daily_performance.py

Optimized PySpark ETL for operator_daily_performance aggregations.

Key points:
- No toPandas() (keeps pipeline distributed)
- Spark JDBC write -> staging table
- Postgres INSERT..ON CONFLICT upsert from staging -> target
- Accurate per-target row counts via staging COUNT(*)
- Rich metrics JSON output for Airflow XCom
"""

from __future__ import annotations

import os
import json
import time
import argparse
import logging
from datetime import datetime
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import psycopg2

# Airflow is available in your Airflow image; Spark jobs run inside it (Option A)
try:
    from airflow.hooks.base import BaseHook
except Exception:
    BaseHook = None  # fallback to env vars

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
from pyspark import StorageLevel


# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger("etl_odp")
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.handlers[:] = [_handler]


def log_event(event: str, **fields):
    payload = {"event": event, **fields}
    logger.info(json.dumps(payload, default=str))


@contextmanager
def timed(durations: dict, key: str):
    t0 = time.time()
    try:
        yield
    finally:
        durations[key] = round(time.time() - t0, 3)


# -----------------------------
# Targets
# -----------------------------
TARGETS = [
    {
        "table": "odp_date_oc",
        "group": ["odp_date", "oc_description", "source_connection"],
        "pk": ["odp_date", "oc_description", "source_connection"],
        "non_null_pk": ["oc_description"],
    },
    {
        "table": "odp_date_shift",
        "group": ["odp_date", "shift", "source_connection"],
        "pk": ["odp_date", "shift", "source_connection"],
        "non_null_pk": [],
    },
    {
        "table": "odp_date_employee",
        "group": ["odp_date", "odp_em_key", "em_firstname", "source_connection"],
        "pk": ["odp_date", "odp_em_key", "em_firstname", "source_connection"],
        "non_null_pk": ["em_firstname"],
    },
]


# -----------------------------
# Postgres helpers
# -----------------------------
def get_postgres_connection_params(conn_id: str = "pg-ssg") -> Dict[str, Any]:
    """
    Prefer Airflow Connection; fallback to environment variables.

    Env fallback vars:
      POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    """
    # 1) Airflow connection
    if BaseHook is not None:
        try:
            c = BaseHook.get_connection(conn_id)
            host = c.host
            port = c.port or 5432
            database = c.schema
            user = c.login
            password = c.password
            if not all([host, database, user]):
                raise ValueError("Airflow connection missing required fields")
            logger.info("Using Airflow connection '%s' (%s:%s/%s)", conn_id, host, port, database)
            return {
                "host": host,
                "port": int(port),
                "database": database,
                "user": user,
                "password": password,
                "jdbc_url": f"jdbc:postgresql://{host}:{port}/{database}",
            }
        except Exception as e:
            logger.warning("Airflow connection '%s' not usable, fallback to env: %s", conn_id, e)

    # 2) Env fallback
    host = os.getenv("POSTGRES_HOST", "172.16.7.6")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    database = os.getenv("POSTGRES_DB", "ssg")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")

    logger.info("Using env connection params %s:%s/%s user=%s", host, port, database, user)
    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "jdbc_url": f"jdbc:postgresql://{host}:{port}/{database}",
    }


def exec_sql(pg: Dict[str, Any], sql: str) -> None:
    with psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        dbname=pg["database"],
        user=pg["user"],
        password=pg["password"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def fetch_one_int(pg: Dict[str, Any], sql: str) -> int:
    with psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        dbname=pg["database"],
        user=pg["user"],
        password=pg["password"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return int(cur.fetchone()[0])


# -----------------------------
# Spark helpers
# -----------------------------
def create_spark_session(app_name: str) -> SparkSession:
    """
    SparkSession for standalone cluster. Master provided via env SPARK_MASTER_URL or defaults local.
    In Airflow Option A, SparkSubmitOperator already passes master; spark-submit sets it.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "48"))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
    )

    spark = builder.getOrCreate()
    logger.info("Spark started: master=%s version=%s", spark.sparkContext.master, spark.version)
    return spark


def read_source_df(spark: SparkSession, pg: Dict[str, Any], lookback_days: int, debug_rowcounts: bool):
    query = f"""
    (
        SELECT
            odp_date,
            oc_description,
            shift,
            odp_em_key,
            em_firstname,
            odpd_quantity,
            source_connection
        FROM operator_daily_performance
        WHERE odp_date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
    ) t
    """

    df = (
        spark.read.format("jdbc")
        .option("url", pg["jdbc_url"])
        .option("dbtable", query)
        .option("user", pg["user"])
        .option("password", pg["password"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", os.getenv("JDBC_FETCHSIZE", "10000"))
        .option("queryTimeout", os.getenv("JDBC_QUERY_TIMEOUT", "600"))
        .load()
    )

    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    if debug_rowcounts:
        n = df.count()
        logger.info("Loaded source rows: %d", n)
    else:
        logger.info("Loaded source data (Spark count skipped; enable --debug-rowcounts to count source rows).")

    return df


def upsert_from_staging(pg: Dict[str, Any], staging_table: str, target_table: str, key_columns: List[str], columns: List[str]) -> None:
    cols_str = ", ".join(columns)
    keys_str = ", ".join(key_columns)
    non_key_cols = [c for c in columns if c not in key_columns]
    if not non_key_cols:
        raise ValueError("No non-key columns to update in upsert.")

    set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in non_key_cols])

    sql = f"""
    INSERT INTO {target_table} ({cols_str})
    SELECT {cols_str} FROM {staging_table}
    ON CONFLICT ({keys_str})
    DO UPDATE SET {set_clause};
    """
    exec_sql(pg, sql)


def upsert_via_staging(
    agg_df,
    pg: Dict[str, Any],
    target_table: str,
    key_columns: List[str],
    staging_table: str,
) -> Dict[str, Any]:
    """
    1) Drop staging
    2) Spark JDBC write staging (distributed)
    3) Count staging rows in Postgres (cheap)
    4) Upsert staging -> target
    5) Drop staging
    """
    metrics: Dict[str, Any] = {"success": False, "staging_rows": None, "durations": {}}

    cols = agg_df.columns
    non_key_cols = [c for c in cols if c not in key_columns]
    if not non_key_cols:
        raise ValueError("No non-key columns found for update set clause")

    exec_sql(pg, f"DROP TABLE IF EXISTS {staging_table};")

    with timed(metrics["durations"], "write_staging_sec"):
        (
            agg_df.write.format("jdbc")
            .option("url", pg["jdbc_url"])
            .option("dbtable", staging_table)
            .option("user", pg["user"])
            .option("password", pg["password"])
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", os.getenv("JDBC_BATCHSIZE", "5000"))
            .mode("overwrite")
            .save()
        )

    with timed(metrics["durations"], "staging_count_sec"):
        metrics["staging_rows"] = fetch_one_int(pg, f"SELECT COUNT(*) FROM {staging_table};")

    with timed(metrics["durations"], "upsert_sec"):
        upsert_from_staging(pg, staging_table, target_table, key_columns, cols)

    with timed(metrics["durations"], "drop_staging_sec"):
        exec_sql(pg, f"DROP TABLE IF EXISTS {staging_table};")

    metrics["success"] = True
    return metrics


# -----------------------------
# Main ETL
# -----------------------------
def run_etl(
    conn_id: str,
    lookback_days: int,
    metrics_path: str,
    app_name: str,
    debug_rowcounts: bool,
) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "success": False,
        "started_at": datetime.utcnow().isoformat(),
        "ended_at": None,
        "conn_id": conn_id,
        "lookback_days": lookback_days,
        "debug_rowcounts": debug_rowcounts,
        "spark": {},
        "source": {
            "table": "operator_daily_performance",
            "filter": f"odp_date >= CURRENT_DATE - INTERVAL '{lookback_days} days'",
            "rows": None,
        },
        "targets": [],
        "warnings": [],
        "durations": {},
        "message": "",
        "metrics_path": metrics_path,
    }

    spark: Optional[SparkSession] = None
    df = None

    try:
        spark = create_spark_session(app_name)
        metrics["spark"] = {
            "master": spark.sparkContext.master,
            "app_name": spark.sparkContext.appName,
            "version": spark.version,
        }

        pg = get_postgres_connection_params(conn_id)

        log_event("etl_start", lookback_days=lookback_days, conn_id=conn_id)

        with timed(metrics["durations"], "read_source_sec"):
            df = read_source_df(spark, pg, lookback_days, debug_rowcounts)

        if debug_rowcounts:
            with timed(metrics["durations"], "source_count_sec"):
                metrics["source"]["rows"] = df.count()
            if metrics["source"]["rows"] == 0:
                metrics["success"] = True
                metrics["ended_at"] = datetime.utcnow().isoformat()
                metrics["message"] = "No recent data"
                return metrics

        sink_partitions = int(os.getenv("JDBC_SINK_PARTITIONS", "16"))

        for cfg in TARGETS:
            t: Dict[str, Any] = {
                "table": cfg["table"],
                "group": cfg["group"],
                "pk": cfg["pk"],
                "null_filters": cfg.get("non_null_pk", []),
                "sink_partitions": sink_partitions,
                "staging_rows": None,
                "durations": {},
                "success": False,
            }

            log_event("target_start", table=cfg["table"])

            df_to_process = df
            for c in cfg.get("non_null_pk", []):
                df_to_process = df_to_process.filter(df_to_process[c].isNotNull())

            with timed(t["durations"], "aggregate_sec"):
                agg_df = (
                    df_to_process
                    .groupBy(*cfg["group"])
                    .agg(spark_sum("odpd_quantity").alias("odpd_quantity"))
                )

            # Prevent too many JDBC writers / connections:
            agg_df = agg_df.coalesce(sink_partitions)

            staging = f"{cfg['table']}__staging"

            with timed(t["durations"], "upsert_total_sec"):
                up = upsert_via_staging(
                    agg_df=agg_df,
                    pg=pg,
                    target_table=cfg["table"],
                    key_columns=cfg["pk"],
                    staging_table=staging,
                )

            t["staging_rows"] = up["staging_rows"]
            t["durations"].update(up["durations"])
            t["success"] = True
            metrics["targets"].append(t)

            log_event(
                "target_done",
                table=t["table"],
                staging_rows=t["staging_rows"],
                durations=t["durations"],
            )

        metrics["success"] = True
        metrics["ended_at"] = datetime.utcnow().isoformat()
        metrics["message"] = "ETL completed successfully"
        log_event("etl_done", success=True, targets=len(metrics["targets"]))
        return metrics

    except Exception as e:
        metrics["success"] = False
        metrics["ended_at"] = datetime.utcnow().isoformat()
        metrics["message"] = f"ETL failed: {e}"
        metrics["warnings"].append(str(e))
        log_event("etl_failed", error=str(e))
        return metrics

    finally:
        # Unpersist + stop
        if df is not None:
            try:
                df.unpersist(blocking=False)
            except Exception:
                pass
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass


def write_metrics(metrics: Dict[str, Any], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, default=str)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--conn-id", default=os.getenv("PG_CONN_ID", "pg-ssg"))
    p.add_argument("--lookback-days", type=int, default=int(os.getenv("LOOKBACK_DAYS", "7")))
    p.add_argument("--metrics-path", default=os.getenv("METRICS_PATH", "/opt/airflow/logs/etl_metrics/etl_metrics.json"))
    p.add_argument("--app-name", default=os.getenv("SPARK_APP_NAME", "HangerLaneDataTransformation"))
    p.add_argument("--debug-rowcounts", action="store_true", default=os.getenv("DEBUG_ROWCOUNTS", "0") == "1")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    m = run_etl(
        conn_id=args.conn_id,
        lookback_days=args.lookback_days,
        metrics_path=args.metrics_path,
        app_name=args.app_name,
        debug_rowcounts=args.debug_rowcounts,
    )
    write_metrics(m, args.metrics_path)
    # Helpful for grepping logs:
    print(json.dumps({"metrics_path": args.metrics_path, "success": m.get("success")}, default=str))
    raise SystemExit(0 if m.get("success") else 1)

#!/usr/bin/env python3
"""
Optimized PySpark ETL for operator_daily_performance aggregations.

Features:
- Avoid toPandas() (no driver OOM risk)
- Staging table + single SQL upsert per target
- Accurate per-target row counts via staging COUNT(*)
- Rich metrics JSON output for Airflow XCom
- Minimal Spark config; stable in containers

Usage:
  spark-submit hangerline_transform_spark.py --conn-id pg-ssg --lookback-days 7 --metrics-path /path/metrics.json
"""

import os
import json
import time
import argparse
import logging
from datetime import datetime
from contextlib import contextmanager
from typing import Dict, List, Any

import psycopg2
from airflow.hooks.base import BaseHook

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
from pyspark import StorageLevel


# -------------------------
# Config / constants
# -------------------------
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


# -------------------------
# Logging
# -------------------------
logger = logging.getLogger("etl_odp")
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.handlers[:] = [handler]


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


# -------------------------
# Postgres helpers
# -------------------------
def get_postgres_connection_params(conn_id: str) -> Dict[str, Any]:
    """
    Prefer Airflow connection; fallback to env vars.
    Returns: host, port, database, user, password, jdbc_url
    """
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
    except Exception as e:
        logger.warning("Airflow connection '%s' not usable, falling back to env vars: %s", conn_id, e)
        host = os.getenv("POSTGRES_HOST", "172.16.7.6")
        port = int(os.getenv("POSTGRES_PORT", "5432"))
        database = os.getenv("POSTGRES_DB", "ssg")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "")
        logger.info("Using env connection params %s:%s/%s", host, port, database)

    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "jdbc_url": f"jdbc:postgresql://{host}:{port}/{database}",
    }


def get_postgres_jdbc_properties(connection_params: Dict[str, str]) -> Dict[str, str]:
    """
    Get JDBC properties for connecting to PostgreSQL.
    """
    return {
        "user": connection_params["user"],
        "password": connection_params["password"]
    }


def exec_sql(pg: Dict[str, Any], sql: str) -> None:
    """Execute SQL statement with connection from params."""
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
    """Fetch a single integer value from query."""
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


# -------------------------
# Spark
# -------------------------
def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and configure Spark session with optimized settings.
    Keep config minimal and stable. Let Spark decide most things.
    """
    spark_master = os.getenv("SPARK_MASTER_URL", "local[4]")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "48"))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
    )

    # JDBC driver: allow override or use common paths
    jdbc_jar = os.getenv("POSTGRES_JDBC_JAR")
    if not jdbc_jar:
        for p in (
            "/opt/airflow/sparkFiles/jdbc-drivers/postgresql-42.7.3.jar",
            "/opt/spark/work/jdbc-drivers/postgresql-42.7.3.jar",
        ):
            if os.path.exists(p):
                jdbc_jar = p
                break

    if jdbc_jar and os.path.exists(jdbc_jar):
        builder = builder.config("spark.jars", jdbc_jar)
        logger.info("Using PostgreSQL JDBC driver: %s", jdbc_jar)
    else:
        logger.warning("PostgreSQL JDBC jar not found (set POSTGRES_JDBC_JAR if needed).")

    spark = builder.getOrCreate()
    logger.info("Spark started: master=%s version=%s", spark.sparkContext.master, spark.version)
    return spark


def read_source_df(spark: SparkSession, pg: Dict[str, Any], lookback_days: int, debug_rowcounts: bool):
    """
    Reads only needed columns and last N days.
    Pushdown filter in SQL.
    """
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

    # Cache once since we reuse it across targets
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    if debug_rowcounts:
        n = df.count()
        logger.info("Loaded source rows: %d", n)
    else:
        logger.info("Loaded source data (Spark count skipped; enable with --debug-rowcounts).")

    return df


# -------------------------
# Upsert via staging
# -------------------------
def upsert_via_staging(
    agg_df,
    pg: Dict[str, Any],
    target_table: str,
    key_columns: List[str],
    staging_table: str,
) -> Dict[str, Any]:
    """
    1) Write agg_df to staging table using Spark JDBC (fast, distributed)
    2) Run a single INSERT..ON CONFLICT..DO UPDATE from staging into target
    3) Drop staging
    
    Returns metrics including staging_rows and durations.
    """
    metrics = {"staging_rows": None, "durations": {}, "success": False}

    cols = agg_df.columns
    non_key_cols = [c for c in cols if c not in key_columns]
    if not non_key_cols:
        raise ValueError("No non-key columns to update.")

    # Ensure staging is clean
    logger.info("Creating staging table: %s", staging_table)
    exec_sql(pg, f"DROP TABLE IF EXISTS {staging_table};")

    # Write staging (distributed) - this is much faster than toPandas()
    logger.info("Writing data to staging table via Spark JDBC...")
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

    # Get accurate row count from staging table
    with timed(metrics["durations"], "staging_count_sec"):
        metrics["staging_rows"] = fetch_one_int(pg, f"SELECT COUNT(*) FROM {staging_table};")
    
    logger.info("Staging table '%s' has %d rows", staging_table, metrics["staging_rows"])

    # Upsert from staging - single SQL operation
    logger.info("Performing upsert from staging to %s...", target_table)
    cols_str = ", ".join(cols)
    keys_str = ", ".join(key_columns)
    set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in non_key_cols])

    upsert_sql = f"""
    INSERT INTO {target_table} ({cols_str})
    SELECT {cols_str} FROM {staging_table}
    ON CONFLICT ({keys_str})
    DO UPDATE SET {set_clause};
    """

    with timed(metrics["durations"], "upsert_sec"):
        exec_sql(pg, upsert_sql)

    # Cleanup
    logger.info("Dropping staging table: %s", staging_table)
    with timed(metrics["durations"], "drop_staging_sec"):
        exec_sql(pg, f"DROP TABLE IF EXISTS {staging_table};")

    metrics["success"] = True
    return metrics


# -------------------------
# Main ETL function
# -------------------------
def run_etl(args) -> Dict[str, Any]:
    """Main ETL pipeline with rich metrics output."""
    metrics: Dict[str, Any] = {
        "success": False,
        "started_at": datetime.utcnow().isoformat(),
        "ended_at": None,
        "lookback_days": args.lookback_days,
        "debug_rowcounts": args.debug_rowcounts,
        "spark": {},
        "source": {
            "table": "operator_daily_performance",
            "filter": f"odp_date >= CURRENT_DATE - INTERVAL '{args.lookback_days} days'",
            "rows": None,
        },
        "targets": [],
        "warnings": [],
        "durations": {},
        "message": "",
    }

    spark = None
    try:
        spark = create_spark_session(args.app_name)
        metrics["spark"] = {
            "master": spark.sparkContext.master,
            "app_name": spark.sparkContext.appName,
            "version": spark.version,
        }

        pg = get_postgres_connection_params(args.conn_id)

        log_event("etl_start", lookback_days=args.lookback_days, conn_id=args.conn_id)

        with timed(metrics["durations"], "read_source_sec"):
            df = read_source_df(spark, pg, args.lookback_days, args.debug_rowcounts)

        if args.debug_rowcounts:
            with timed(metrics["durations"], "source_count_sec"):
                metrics["source"]["rows"] = df.count()
            if metrics["source"]["rows"] == 0:
                metrics["success"] = True
                metrics["ended_at"] = datetime.utcnow().isoformat()
                metrics["message"] = "No recent data"
                logger.warning("No recent data found")
                return metrics

        sink_partitions = int(os.getenv("JDBC_SINK_PARTITIONS", "16"))

        for cfg in TARGETS:
            t = {
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
            
            # Filter out NULL values in primary key columns
            for c in cfg.get("non_null_pk", []):
                logger.info("Filtering NULL values in column: %s", c)
                df_to_process = df_to_process.filter(df_to_process[c].isNotNull())

            # Aggregate by group columns
            logger.info("Aggregating data for %s by columns: %s", cfg["table"], cfg["group"])
            with timed(t["durations"], "aggregate_sec"):
                agg_df = (
                    df_to_process
                    .groupBy(*cfg["group"])
                    .agg(spark_sum("odpd_quantity").alias("odpd_quantity"))
                )

            # Small optimization: reduce partitions before JDBC write
            # (prevents too many DB connections)
            agg_df = agg_df.coalesce(sink_partitions)
            logger.info("Coalesced to %d partitions for JDBC sink", sink_partitions)

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

            log_event("target_done", table=cfg["table"], staging_rows=t["staging_rows"], durations=t["durations"])
            logger.info("✓ Upsert complete: %s (%d rows)", cfg["table"], t["staging_rows"])

        # Cleanup cached dataframe
        try:
            df.unpersist(blocking=False)
            logger.info("Unpersisted cached source DataFrame")
        except Exception as e:
            metrics["warnings"].append(f"Unpersist failed: {e}")

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
        logger.exception("ETL failed")
        return metrics

    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("Spark session stopped")
            except Exception:
                pass


def write_metrics(metrics: Dict[str, Any], path: str) -> None:
    """Write metrics to JSON file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, default=str)
    logger.info("Metrics written to: %s", path)


def parse_args():
    """Parse command-line arguments."""
    p = argparse.ArgumentParser(description="Hanger Line Data Transformation ETL")
    p.add_argument("--conn-id", default=os.getenv("PG_CONN_ID", "pg-ssg"), 
                   help="Airflow connection ID for PostgreSQL")
    p.add_argument("--lookback-days", type=int, default=int(os.getenv("LOOKBACK_DAYS", "7")),
                   help="Number of days to look back for data")
    p.add_argument("--metrics-path", default=os.getenv("METRICS_PATH", "/opt/airflow/logs/etl_metrics.json"),
                   help="Path to write metrics JSON file")
    p.add_argument("--app-name", default=os.getenv("SPARK_APP_NAME", "HangerLaneDataTransformation"),
                   help="Spark application name")
    p.add_argument("--debug-rowcounts", action="store_true", default=os.getenv("DEBUG_ROWCOUNTS", "0") == "1",
                   help="Enable row counting (triggers full scans)")
    return p.parse_args()


# -------------------------
# Backward compatibility functions
# -------------------------
def check_for_recent_data(spark: SparkSession = None, days: int = 30) -> int:
    """
    Check if there's recent data in operator_daily_performance table to process.
    This function is kept for backward compatibility with old DAG code.
    """
    spark_created = False
    try:
        if spark is None:
            logger.info("Creating Spark session for data check...")
            spark = create_spark_session("DataCheck")
            spark_created = True
        
        logger.info("Getting database connection parameters...")
        pg = get_postgres_connection_params("pg-ssg")
        
        query = f"""
        (
            SELECT COUNT(*) as record_count 
            FROM operator_daily_performance 
            WHERE odp_date >= CURRENT_DATE - INTERVAL '{days} days'
        ) t
        """
        
        logger.info("Checking for data in last %d days...", days)
        
        count_df = spark.read \
            .format("jdbc") \
            .option("url", pg["jdbc_url"]) \
            .option("dbtable", query) \
            .option("user", pg["user"]) \
            .option("password", pg["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        count = count_df.first()["record_count"]
        logger.info("✓ Found %d recent records", count)
        return count
        
    except Exception as e:
        logger.error("✗ Error checking for recent data: %s", e)
        return 0
    finally:
        if spark_created and spark is not None:
            try:
                spark.stop()
                logger.info("Spark session stopped after data check")
            except:
                pass


def transform_data(spark: SparkSession) -> Dict:
    """
    Legacy function for backward compatibility.
    New code should use run_etl() with command-line args instead.
    """
    logger.warning("Using legacy transform_data() function. Consider migrating to run_etl().")
    
    class Args:
        conn_id = "pg-ssg"
        lookback_days = int(os.getenv("LOOKBACK_DAYS", "7"))
        metrics_path = "/tmp/legacy_metrics.json"
        app_name = "HangerLaneDataTransformation"
        debug_rowcounts = os.getenv("DEBUG_ROWCOUNTS", "0") == "1"
    
    return run_etl(Args())


# -------------------------
# Main entry point
# -------------------------
if __name__ == "__main__":
    args = parse_args()
    m = run_etl(args)
    write_metrics(m, args.metrics_path)
    
    # Print summary for easy grepping in logs
    print(json.dumps({
        "metrics_path": args.metrics_path,
        "success": m.get("success"),
        "message": m.get("message"),
        "targets": len(m.get("targets", []))
    }, default=str))
    
    raise SystemExit(0 if m.get("success") else 1)

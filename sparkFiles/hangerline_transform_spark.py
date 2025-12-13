"""
Final Clean & Correct PySpark ETL with Real PostgreSQL UPSERT (ON CONFLICT)
Works on PostgreSQL 11–17 | No MERGE required
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
from airflow.hooks.base import BaseHook

try:
    from dags.db_utils import get_postgres_connection_params, get_postgres_jdbc_properties
except ImportError as e:
    raise ImportError("db_utils.py not found in DAGs folder") from e

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HangerLaneETL")

JDBC_JAR = "/opt/airflow/sparkFiles/jdbc-drivers/postgresql-42.7.3.jar"

TARGETS = [
    {"table": "odp_date_oc",        "group": ["odp_date", "oc_description", "source_connection"], "pk": ["odp_date", "oc_description", "source_connection"]},
    {"table": "odp_date_shift",     "group": ["odp_date", "shift", "source_connection"],         "pk": ["odp_date", "shift", "source_connection"]},
    {"table": "odp_date_employee",  "group": ["odp_date", "odp_em_key", "em_firstname", "source_connection"], "pk": ["odp_date", "odp_em_key", "source_connection"]},
]

def create_spark() -> SparkSession:
    builder = SparkSession.builder.appName("HangerLane_Aggregated_ETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", "1") \
        .config("spark.sql.shuffle.partitions", "50")

    if os.path.exists(JDBC_JAR):
        builder = builder.config("spark.jars", JDBC_JAR)
    return builder.getOrCreate()


def get_conn():
    try:
        c = BaseHook.get_connection("pg-ssg")
        return {
            "host": c.host, "port": c.port or 5432, "database": c.schema,
            "user": c.login, "password": c.password or "",
            "jdbc_url": f"jdbc:postgresql://{c.host}:{c.port or 5432}/{c.schema}"
        }
    except:
        return get_postgres_connection_params("pg-ssg")


def load_data(spark, url, props):
    query = """
    (SELECT odp_date, oc_description, shift, odp_em_key, em_firstname,
            odpd_quantity, source_connection
     FROM operator_daily_performance
     WHERE odp_date >= CURRENT_DATE - INTERVAL '3 days') t
    """
    return (spark.read.format("jdbc")
            .option("url", url)
            .option("dbtable", query)
            .option("user", props["user"])
            .option("password", props["password"])
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", 10000)
            .load())


def upsert_data_via_spark(
    spark: SparkSession,
    data_df,
    table_name: str,
    key_columns: list,
    connection_params: dict = None
) -> bool:
    """
    Perform upsert operation on PostgreSQL table using Spark with staging table approach.

    Args:
        spark: SparkSession instance
        data_df: DataFrame containing the data to upsert
        table_name: Name of the target table
        key_columns: List of column names that form the primary key
        connection_params: Database connection parameters (optional)

    Returns:
        bool: True if successful, False otherwise
    """
    import psycopg2
    import uuid

    try:
        if data_df is None or data_df.rdd.isEmpty():
            print("No data to upsert")
            return True

        # Get connection parameters if not provided
        if not connection_params:
            try:
                connection_params = get_postgres_connection_params("pg-ssg")
            except Exception as e:
                print(f"Error getting connection params: {e}")
                return False

        # Get JDBC properties
        props = get_postgres_jdbc_properties(connection_params)
        jdbc_url = connection_params["jdbc_url"]

        # Generate a unique staging table name
        staging_table = f"{table_name}_staging_{str(uuid.uuid4()).replace('-', '_')}"

        try:
            # Create staging table with same structure as target table
            conn = psycopg2.connect(
                host=connection_params.get("host"),
                port=connection_params.get("port", "5432"),
                database=connection_params.get("database"),
                user=connection_params.get("user"),
                password=connection_params.get("password")
            )
            cursor = conn.cursor()

            # Drop staging table if exists
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")

            # Create staging table with same structure as target table
            cursor.execute(f"CREATE TABLE {staging_table} (LIKE {table_name} INCLUDING ALL);")
            conn.commit()
            cursor.close()
            conn.close()

            # Write data to staging table using Spark
            data_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", staging_table) \
                .option("user", props["user"]) \
                .option("password", props["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

            # Now perform upsert using ON CONFLICT
            conn = psycopg2.connect(
                host=connection_params.get("host"),
                port=connection_params.get("port", "5432"),
                database=connection_params.get("database"),
                user=connection_params.get("user"),
                password=connection_params.get("password")
            )
            cursor = conn.cursor()

            # Get all column names from the DataFrame
            columns = data_df.columns
            all_columns_str = ", ".join(columns)

            # Create the SET clause for UPDATE (excluding key columns)
            set_columns = [col for col in columns if col not in key_columns]
            set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in set_columns])

            # Create key columns string for ON CONFLICT clause
            key_columns_str = ", ".join(key_columns)

            # UPSERT SQL statement
            upsert_sql = f"""
            INSERT INTO {table_name} ({all_columns_str})
            SELECT {all_columns_str} FROM {staging_table}
            ON CONFLICT ({key_columns_str})
            DO UPDATE SET {set_clause};
            """

            # Execute upsert
            cursor.execute(upsert_sql)
            conn.commit()

            # Clean up staging table
            cursor.execute(f"DROP TABLE {staging_table};")
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Data successfully upserted to {table_name}")
            return True

        except Exception as e:
            logger.error(f"Error during upsert operation: {str(e)}")
            # Try to clean up staging table if it exists
            try:
                conn = psycopg2.connect(
                    host=connection_params.get("host"),
                    port=connection_params.get("port", "5432"),
                    database=connection_params.get("database"),
                    user=connection_params.get("user"),
                    password=connection_params.get("password")
                )
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
                conn.commit()
                cursor.close()
                conn.close()
            except:
                pass
            raise

    except Exception as e:
        logger.error(f"Error in upsert_data_via_spark: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    spark = None
    try:
        spark = create_spark()
        conn = get_conn()
        props = get_postgres_jdbc_properties(conn)
        url = conn["jdbc_url"]

        df = load_data(spark, url, props).cache()
        if df.rdd.isEmpty():
            logger.info("No new data. Exiting.")
            return True

        logger.info(f"Loaded {df.count()} rows for processing")

        for cfg in TARGETS:
            agg = df.groupBy(*cfg["group"]) \
                    .agg(spark_sum("odpd_quantity").alias("odpd_quantity"))
            success = upsert_data_via_spark(
                spark=spark,
                data_df=agg,
                table_name=cfg["table"],
                key_columns=cfg["pk"],
                connection_params=conn
            )
            if not success:
                logger.error(f"Failed to upsert data to {cfg['table']}")
                return False

        logger.info("All tables updated successfully with UPSERT")
        return True

    except Exception as e:
        logger.error("ETL Failed", exc_info=True)
        return False
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)

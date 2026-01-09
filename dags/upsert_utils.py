import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, List, Any
import pandas as pd

def sanitize_records(data: List[Dict[str, Any]], key_columns: List[str]) -> List[Dict[str, Any]]:
    """Ensure PK fields are non-null and fix date/time types for all datetime columns."""
    clean_data = []
    for record in data:
        for key in key_columns:
            if record.get(key) is None:
                # Default replacements
                if "date" in key or "time" in key:
                    record[key] = pd.Timestamp.now().normalize()
                else:
                    record[key] = "unknown"

        # Type conversions for all datetime-like columns
        for col_name, value in record.items():
            # Check if column name suggests it's a date/time field
            col_lower = col_name.lower()
            if any(pattern in col_lower for pattern in ['date', 'time', 'timestamp', 'created', 'updated', 'modified']):
                if pd.isna(value):
                    # Convert NaT/NaN/None to None
                    record[col_name] = None
                else:
                    try:
                        # Convert to datetime first
                        dt = pd.to_datetime(value)
                        # If it's just a date (no time component), convert to date object
                        if dt.time() == pd.Timestamp('1900-01-01').time():
                            record[col_name] = dt.date()
                        else:
                            # Otherwise convert to full datetime
                            record[col_name] = dt.to_pydatetime()
                    except (ValueError, TypeError):
                        # If conversion fails, leave as is
                        pass

        clean_data.append(record)
    return clean_data


def upsert_data_via_postgres(
    data: List[Dict[str, Any]],
    table_name: str,
    key_columns: List[str],
    connection_params: Dict[str, str],
) -> bool:
    """Perform bulk UPSERT safely using psycopg2 with execute_values."""
    if not data:
        print("⚠️ No data to upsert")
        return True

    try:
        conn = psycopg2.connect(
            host=connection_params.get("host", "localhost"),
            port=connection_params.get("port", "5432"),
            database=connection_params.get("database", "ssg"),
            user=connection_params.get("user", "postgres"),
            password=connection_params.get("password", "postgres"),
        )
        conn.autocommit = False
        cursor = conn.cursor()

        valid_data = sanitize_records(data, key_columns)
        if not valid_data:
            print("⚠️ No valid records after sanitization")
            return True

        # Columns setup
        columns = list(valid_data[0].keys())
        quoted_cols = ", ".join([f'"{c}"' for c in columns])
        key_cols = ", ".join([f'"{k}"' for k in key_columns])
        set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in columns if c not in key_columns])

        # Final UPSERT SQL
        sql = f"""
            INSERT INTO {table_name} ({quoted_cols})
            VALUES %s
            ON CONFLICT ({key_cols}) DO UPDATE SET {set_clause};
        """

        # Prepare value tuples
        values = [tuple(r[c] for c in columns) for r in valid_data]

        # Execute in batch
        execute_values(cursor, sql, values, page_size=500)
        conn.commit()

        cursor.close()
        conn.close()
        print(f"✅ Upserted {len(valid_data)} records into {table_name}")
        return True

    except Exception as e:
        print(f"❌ Error in upsert_data_via_postgres: {str(e)}")
        return False


def create_connection_params_from_airflow(connection_id: str = "pg-ssg") -> Dict[str, str]:
    """
    Create connection parameters from Airflow connection.
    """
    try:
        from airflow.hooks.base import BaseHook
        connection = BaseHook.get_connection(connection_id)
        return {
            "host": connection.host,
            "port": str(connection.port),
            "database": connection.schema,
            "user": connection.login,
            "password": connection.password,
        }
    except Exception:
        return {
            "host": "localhost",
            "port": "5432",
            "database": "ssg",
            "user": "postgres",
            "password": "P@kistan12",
        }

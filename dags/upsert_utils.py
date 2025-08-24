"""
Utility functions for performing upsert operations on PostgreSQL tables.
"""

import psycopg2
from typing import Dict, List, Any


def upsert_data_via_postgres(
    data: List[Dict[str, Any]], 
    table_name: str, 
    key_columns: List[str], 
    connection_params: Dict[str, str]
) -> bool:
    """
    Perform upsert operation on PostgreSQL table using psycopg2.
    
    Args:
        data: List of dictionaries containing the data to upsert
        table_name: Name of the target table
        key_columns: List of column names that form the primary key
        connection_params: Database connection parameters
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create connection
        conn = psycopg2.connect(
            host=connection_params.get("host", "postgres_grafana"),
            port=connection_params.get("port", "5432"),
            database=connection_params.get("database", "ssg"),
            user=connection_params.get("user", "postgres"),
            password=connection_params.get("password", "postgres")
        )
        cursor = conn.cursor()
        
        if not data:
            print("No data to upsert")
            return True
            
        # Get column names from the first record
        columns = list(data[0].keys())
        all_columns_str = ", ".join(columns)
        
        # Create the staging table
        staging_table = f"{table_name}_staging"
        
        # Drop staging table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
        
        # Create staging table with same structure as target table
        cursor.execute(f"""
            CREATE TABLE {staging_table} (LIKE {table_name} INCLUDING ALL);
        """)
        
        # Insert data into staging table
        # Prepare the INSERT statement
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {staging_table} ({all_columns_str}) VALUES ({placeholders})"
        
        # Prepare data tuples
        data_tuples = [tuple(record[col] for col in columns) for record in data]
        
        # Execute batch insert
        cursor.executemany(insert_sql, data_tuples)
        
        # Perform upsert using ON CONFLICT
        key_columns_str = ", ".join(key_columns)
        
        # Generate the SET clause for UPDATE (excluding key columns)
        set_columns = [col for col in columns if col not in key_columns]
        set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in set_columns])
        
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
        
        print(f"Successfully upserted {len(data)} records to {table_name}")
        return True
        
    except Exception as e:
        print(f"Error in upsert_data_via_postgres: {str(e)}")
        return False


def create_connection_params_from_airflow(connection_id: str = "postgres_grafana") -> Dict[str, str]:
    """
    Create connection parameters from Airflow connection.
    This function is meant to be used in an Airflow environment.
    
    Args:
        connection_id: Airflow connection ID
        
    Returns:
        Dict[str, str]: Connection parameters
    """
    try:
        from airflow.hooks.base import BaseHook
        
        connection = BaseHook.get_connection(connection_id)
        return {
            "host": connection.host,
            "port": str(connection.port),
            "database": connection.schema,
            "user": connection.login,
            "password": connection.password
        }
    except Exception as e:
        print(f"Error getting Airflow connection: {str(e)}")
        # Return default values
        return {
            "host": "postgres_grafana",
            "port": "5432",
            "database": "ssg",
            "user": "postgres",
            "password": "postgres"
        }
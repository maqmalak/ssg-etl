"""
Utility functions for performing upsert operations on PostgreSQL tables.
"""

import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, List, Any


def upsert_data_via_postgres(
    data: List[Dict[str, Any]], 
    table_name: str, 
    key_columns: List[str], 
    connection_params: Dict[str, str]
) -> bool:
    """
    Perform upsert operation on PostgreSQL table using psycopg2 with optimizations.
    
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
            host=connection_params.get("host", "pg-ssg"),
            port=connection_params.get("port", "5432"),
            database=connection_params.get("database", "ssg"),
            user=connection_params.get("user", "postgres"),
            password=connection_params.get("password", "P@akistan12")
        )
        cursor = conn.cursor()
        
        if not data:
            print("No data to upsert")
            return True
            
        # Get column names from the first record
        columns = list(data[0].keys())
        all_columns_str = ", ".join([f'"{col}"' for col in columns])  # Use double quotes for case sensitivity
        
        # Validate that key columns are not NULL
        valid_data = []
        for record in data:
            if all(record.get(col) is not None for col in key_columns):
                valid_data.append(record)
            else:
                print(f"Skipping record with NULL key: {record}")
        
        if not valid_data:
            print("All records have NULL in key columns. Nothing to upsert.")
            return True
            
        # Create the staging table
        staging_table = f"{table_name}_staging"
        
        # Drop staging table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
        
        # Create staging table with same structure as target table
        # Use double quotes for column names to preserve case sensitivity
        cursor.execute(f"""
            CREATE TABLE {staging_table} (LIKE {table_name} INCLUDING ALL);
        """)
        
        # Insert data into staging table using execute_values for better performance
        insert_sql = f"INSERT INTO {staging_table} ({all_columns_str}) VALUES %s"
        
        # Prepare data tuples
        data_tuples = [tuple(record[col] for col in columns) for record in valid_data]
        
        # Execute batch insert with execute_values for 10-100x speedup
        execute_values(cursor, insert_sql, data_tuples)
        
        # Perform upsert using ON CONFLICT
        # Use double quotes for key columns to preserve case sensitivity
        key_columns_str = ", ".join([f'"{col}"' for col in key_columns])
        
        # Generate the SET clause for UPDATE (excluding key columns)
        set_columns = [col for col in columns if col not in key_columns]
        set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in set_columns])
        
        # UPSERT SQL statement with double quotes for column names
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
        
        print(f"Successfully upserted {len(valid_data)} records to {table_name}")
        return True
        
    except Exception as e:
        print(f"Error in upsert_data_via_postgres: {str(e)}")
        return False


def create_connection_params_from_airflow(connection_id: str = "pg-ssg") -> Dict[str, str]:
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
            "host": "pg-ssg",
            "port": "5432",
            "database": "ssg",
            "user": "postgres",
            "password": "P@kistan12"
        }
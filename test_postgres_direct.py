#!/usr/bin/env python3
"""
Simple database connectivity test using psycopg2
"""

import os
import sys

# Add the dags directory to the Python path
dags_path = os.path.join(os.path.dirname(__file__), 'dags')
sys.path.insert(0, dags_path)

try:
    import psycopg2
    print("Successfully imported psycopg2")
    
    # Get connection parameters
    from db_utils import get_postgres_connection_params
    
    params = get_postgres_connection_params("postgres_grafana")
    
    print(f"Attempting to connect to PostgreSQL at {params['host']}:{params['port']}")
    
    # Try to establish a connection
    connection = psycopg2.connect(
        host=params['host'],
        port=params['port'],
        database=params['database'],
        user=params['user'],
        password=params['password']
    )
    
    print("Successfully connected to PostgreSQL!")
    
    # Try to execute a simple query
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"PostgreSQL version: {version[0]}")
    
    # Try to check if the table exists
    try:
        cursor.execute("SELECT COUNT(*) FROM operator_daily_performance;")
        count = cursor.fetchone()
        print(f"Rows in operator_daily_performance table: {count[0]}")
    except Exception as e:
        print(f"Error querying operator_daily_performance table: {e}")
    
    # Close the connection
    cursor.close()
    connection.close()
    print("Database connection test completed successfully!")
    
except ImportError as e:
    print(f"Error importing psycopg2: {e}")
    print("psycopg2 is required for this test. Install it with: pip install psycopg2")
    sys.exit(1)
except Exception as e:
    print(f"Error during database connection test: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
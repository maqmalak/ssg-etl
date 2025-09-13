"""
Test script to check Airflow connection access
"""

import sys
import os

# Add the dags directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'dags'))

def test_airflow_connection():
    """Test if we can access the Airflow connection"""
    print("=== Testing Airflow Connection Access ===")
    
    try:
        from airflow.hooks.base import BaseHook
        print("✓ BaseHook imported successfully")
        
        # Try to get the connection
        print("Attempting to get 'pg-ssg' connection...")
        connection = BaseHook.get_connection("pg-ssg")
        print("✓ Connection retrieved successfully")
        print(f"  Connection ID: {connection.conn_id}")
        print(f"  Host: {connection.host}")
        print(f"  Port: {connection.port}")
        print(f"  Schema: {connection.schema}")
        print(f"  Login: {connection.login}")
        print(f"  Password length: {len(connection.password) if connection.password else 0}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error accessing Airflow connection: {e}")
        print("This is expected if running outside of Airflow environment")
        return False

def test_environment_variables():
    """Test environment variables"""
    print("\n=== Testing Environment Variables ===")
    
    env_vars = [
        "POSTGRES_HOST",
        "POSTGRES_PORT", 
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD"
    ]
    
    for var in env_vars:
        value = os.getenv(var, "Not set")
        if "PASSWORD" in var:
            masked_value = "*" * len(value) if value != "Not set" else "Not set"
            print(f"{var}: {masked_value}")
        else:
            print(f"{var}: {value}")
    
    return True

if __name__ == "__main__":
    print("=== Airflow Connection Test ===\n")
    
    test1_passed = test_airflow_connection()
    test2_passed = test_environment_variables()
    
    print("\n=== Test Summary ===")
    if test1_passed:
        print("✓ Airflow connection accessible")
    else:
        print("⚠ Airflow connection not accessible (expected outside Airflow)")
        
    print("✓ Environment variables checked")
    print("\nIn Airflow environment, the DAG will try Airflow connections first,")
    print("then fall back to environment variables if needed.")
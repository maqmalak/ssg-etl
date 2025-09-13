"""
Debug script to test the check_for_new_data function logic
"""

import sys
import os
from datetime import datetime

# Add the dags directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'dags'))

def debug_check_for_new_data_logic():
    """Debug the check_for_new_data function logic"""
    
    # Simulate the logic from the DAG
    print("Debugging check_for_new_data logic...")
    
    # Test case 1: No previous extract datetime
    print("\n1. Testing with no previous extract datetime:")
    last_extract_dt = None
    if last_extract_dt:
        print(f"   Found last extract: {last_extract_dt}")
        # This would check for new data
        print("   Would check for new data...")
    else:
        print("   No previous extract date found")
        print("   Decision: SAVE PATH (No previous extract)")
        print("   Expected return: True")
    
    # Test case 2: With previous extract datetime
    print("\n2. Testing with previous extract datetime:")
    last_extract_dt = datetime(2025, 9, 10, 12, 0, 0)
    if last_extract_dt:
        print(f"   Found last extract: {last_extract_dt}")
        print("   Would check database for records newer than last extract")
        print("   If count > 0: Decision: SAVE PATH")
        print("   If count = 0: Decision: SKIP PATH")
    
    # Test case 3: Connection error simulation
    print("\n3. Testing connection error handling:")
    error_message = "unable to connect to server"
    connection_error_keywords = [
        "unable to connect", 
        "adaptive server is unavailable", 
        "connection", 
        "timeout", 
        "could not connect",
        "server is unavailable",
        "host not found",
        "name or service not known"
    ]
    
    is_connection_error = any(keyword in error_message.lower() for keyword in connection_error_keywords)
    print(f"   Error message: {error_message}")
    print(f"   Is connection error: {is_connection_error}")
    if is_connection_error:
        print("   Decision: SKIP PATH (Server unavailable)")
        print("   Expected return: False")
    else:
        print("   Decision: SAVE PATH (Non-connection error)")
        print("   Expected return: True")

def check_source_constants():
    """Check what source constants are being used"""
    try:
        from scripts.constans.db_sources import SOURCE_HANGER_LANE
        print(f"\nSOURCE_HANGER_LANE contains: {SOURCE_HANGER_LANE}")
        print(f"Number of lines: {len(SOURCE_HANGER_LANE)}")
        
        # Check if lines are properly defined
        for i, line in enumerate(SOURCE_HANGER_LANE):
            print(f"   Line {i+1}: {line}")
            
    except Exception as e:
        print(f"Error checking source constants: {e}")

def check_airflow_connections():
    """Check if Airflow connections exist"""
    try:
        # Try to import Airflow
        from airflow.hooks.base import BaseHook
        print("\nAirflow BaseHook imported successfully")
        
        # Try to get a connection (this will likely fail in test environment)
        try:
            connection = BaseHook.get_connection("pg-ssg")
            print(f"pg-ssg connection found: {connection}")
        except Exception as e:
            print(f"Could not get pg-ssg connection (expected in test): {e}")
            
        # Try to get MSSQL connections
        for line in ['Line-21', 'Line-22', 'Line-23', 'Line-24']:
            try:
                connection = BaseHook.get_connection(line)
                print(f"{line} connection found: {connection.host}")
            except Exception as e:
                print(f"Could not get {line} connection: {e}")
                
    except Exception as e:
        print(f"Error checking Airflow connections: {e}")

if __name__ == "__main__":
    print("=== Debugging hanger_lane DAG extraction issues ===")
    
    debug_check_for_new_data_logic()
    check_source_constants()
    check_airflow_connections()
    
    print("\n=== Debug Summary ===")
    print("1. Check if ETL log table exists and has data")
    print("2. Verify Airflow connections are properly configured")
    print("3. Check if source databases are accessible")
    print("4. Verify the check_for_new_data function is returning expected values")
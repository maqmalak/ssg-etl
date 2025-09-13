"""
Comprehensive debug script for hanger_lane DAG extraction issues
"""

import sys
import os
import sqlite3
from datetime import datetime

# Add the dags directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'dags'))

def debug_etl_log_table():
    """Debug the ETL log table"""
    print("=== Debugging ETL Log Table ===")
    
    try:
        # Try to check if the ETL log table exists and has data
        from dags.hanger_lane_fixed import get_postgres_engine, create_etl_log_table_if_not_exists
        from sqlalchemy import text
        
        print("Attempting to connect to PostgreSQL...")
        engine = get_postgres_engine()
        
        # Create the ETL log table if it doesn't exist
        print("Creating ETL log table if it doesn't exist...")
        create_etl_log_table_if_not_exists(engine)
        
        # Check if there's data in the ETL log table
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM etl_extract_log")).scalar()
            print(f"Total records in etl_extract_log: {result}")
            
            if result > 0:
                # Get the latest records
                latest_records = conn.execute(text("""
                    SELECT source_connection, lastextractdatetime, success, status 
                    FROM etl_extract_log 
                    ORDER BY lastextractdatetime DESC 
                    LIMIT 5
                """)).fetchall()
                
                print("Latest 5 records in etl_extract_log:")
                for record in latest_records:
                    print(f"  Source: {record[0]}, Last Extract: {record[1]}, Success: {record[2]}, Status: {record[3]}")
            else:
                print("No records found in etl_extract_log")
                
        engine.dispose()
        
    except Exception as e:
        print(f"Error checking ETL log table: {e}")
        print("This might be expected if PostgreSQL is not accessible in this environment")

def debug_connection_strings():
    """Debug connection string building"""
    print("\n=== Debugging Connection String Building ===")
    
    try:
        from dags.hanger_lane_fixed import build_mssql_conn_str
        
        # Create a mock connection object
        class MockConnection:
            def __init__(self, host, schema, login, password):
                self.host = host
                self.schema = schema
                self.login = login
                self.password = password
                self.port = 1433
        
        # Test with sample data
        mock_conn = MockConnection("192.168.1.100", "IHS", "user", "password")
        conn_str = build_mssql_conn_str(mock_conn)
        print(f"Sample connection string: {conn_str}")
        
        # Check for common issues
        issues = []
        if "DRIVER={FreeTDS};" not in conn_str:
            issues.append("Missing FreeTDS driver")
        if "SERVER=" not in conn_str:
            issues.append("Missing SERVER parameter")
        if "PORT=1433;" not in conn_str:
            issues.append("Missing PORT parameter")
        if "DATABASE=" not in conn_str:
            issues.append("Missing DATABASE parameter")
        if "UID=" not in conn_str:
            issues.append("Missing UID parameter")
        if "PWD=" not in conn_str:
            issues.append("Missing PWD parameter")
        if "TDS_Version=7.0;" not in conn_str:
            issues.append("Missing TDS_Version parameter")
            
        if issues:
            print("Connection string issues found:")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print("Connection string format looks correct")
            
    except Exception as e:
        print(f"Error debugging connection strings: {e}")

def debug_data_extraction_logic():
    """Debug the data extraction logic"""
    print("\n=== Debugging Data Extraction Logic ===")
    
    # Simulate the fetch_data_from_source function logic
    print("1. Checking get_last_extract_dt_from_log behavior:")
    print("   - If no previous extract: Should get min creation date from source")
    print("   - If previous extract exists: Should use that date for filtering")
    
    print("\n2. Checking fetch_data_from_source behavior:")
    print("   - Should build query with proper WHERE clause")
    print("   - Should handle parameter binding correctly")
    print("   - Should process data in batches")
    
    # Show the corrected query
    query = """
        SELECT
            [ODP_Date]
            ,[ODP_Key]
            ,CASE WHEN [ODP_Shift]=1 THEN 'Day' ELSE 'Night' END AS [Shift]
            -- ... other columns ...
        FROM [IHS].[dbo].[ODP_Detail] OD
        INNER JOIN [IHS].[dbo].[ODP_Master] OM ON OD.[ODPD_ODP_Key] = OM.[ODP_Key]  
        INNER JOIN [IHS_SHARED].[dbo].[Employee_Master] EM ON OM.[ODP_EM_Key]=EM.[EM_Key]
        -- ... other joins ...
        WHERE 1=1
    """
    
    print(f"\n3. Sample query structure (corrected):")
    print("   - No extra commas in SELECT clause")
    print("   - Proper JOIN syntax")
    print("   - Flexible WHERE clause for parameter binding")

def debug_branching_logic():
    """Debug the branching logic"""
    print("\n=== Debugging Branching Logic ===")
    
    print("The decide_next_task function should:")
    print("  - Return 'extract_{connection_id}' when has_new_data is True")
    print("  - Return 'skip_{connection_id}' when has_new_data is False")
    
    print("\nFlow should be:")
    print("  start -> check_for_new_data -> decide_next_task")
    print("  decide_next_task -> extract_{connection_id} (if has_new_data)")
    print("  decide_next_task -> skip_{connection_id} (if no new data)")
    print("  extract_{connection_id} -> save_{connection_id} -> end")
    print("  skip_{connection_id} -> end")

def create_detailed_debug_script():
    """Create a more detailed debug script"""
    debug_script = '''
"""
Detailed debug script for hanger_lane DAG
"""

import sys
import os
from datetime import datetime

# Add the dags directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'dags'))

def detailed_debug():
    """Detailed debugging of the hanger_lane DAG"""
    print("=== DETAILED DEBUG ===")
    
    # 1. Check source constants
    try:
        from scripts.constans.db_sources import SOURCE_HANGER_LANE
        print(f"SOURCE_HANGER_LANE: {SOURCE_HANGER_LANE}")
    except Exception as e:
        print(f"Error importing SOURCE_HANGER_LANE: {e}")
        return
    
    # 2. For each source, check the logic
    for connection_id in SOURCE_HANGER_LANE:
        print(f"\\n--- Debugging {connection_id} ---")
        
        # Simulate check_for_new_data logic
        try:
            # Try to get last extract datetime
            try:
                from dags.hanger_lane_fixed import get_last_extract_dt_from_log
                last_extract_dt = get_last_extract_dt_from_log(connection_id)
                print(f"  Last extract datetime: {last_extract_dt}")
            except Exception as e:
                print(f"  Error getting last extract datetime: {e}")
                last_extract_dt = None
            
            # Decision logic
            if last_extract_dt:
                print(f"  Found last extract: {last_extract_dt}")
                print("  Would check database for new records...")
                print("  Decision depends on database query result")
            else:
                print("  No previous extract date found")
                print("  DECISION: SAVE PATH (No previous extract)")
                print("  Expected return: True")
                
        except Exception as e:
            print(f"  Error in check_for_new_data logic: {e}")

if __name__ == "__main__":
    detailed_debug()
'''
    
    with open('/home/maqmalak/ETL/ssg-etl/detailed_debug.py', 'w') as f:
        f.write(debug_script)
    
    print("Created detailed_debug.py script")

if __name__ == "__main__":
    print("=== Comprehensive Debug for hanger_lane DAG ===")
    
    debug_etl_log_table()
    debug_connection_strings()
    debug_data_extraction_logic()
    debug_branching_logic()
    create_detailed_debug_script()
    
    print("\n=== Next Steps ===")
    print("1. Run the detailed_debug.py script to check per-line logic")
    print("2. Check if ETL log table has recent entries")
    print("3. Verify Airflow connections are properly configured")
    print("4. Check if source databases are accessible")
    print("5. Look at Airflow logs for specific error messages")
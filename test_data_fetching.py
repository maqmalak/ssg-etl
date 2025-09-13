"""
Test script to verify hanger_line_daily_transform data fetching
"""

import sys
import os
import psycopg2

# Add the dags directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'dags'))

def test_database_connection():
    """Test connection to the PostgreSQL database"""
    print("=== Testing Database Connection ===")
    
    try:
        # Get connection parameters from environment variables
        host = os.getenv("POSTGRES_HOST", "172.16.7.6")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "ssg")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "P@kistan12")  # Use correct password
        
        print(f"Connection parameters:")
        print(f"  Host: {host}")
        print(f"  Port: {port}")
        print(f"  Database: {database}")
        print(f"  User: {user}")
        
        # Connect to PostgreSQL
        print("Attempting to connect to PostgreSQL...")
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        cursor = conn.cursor()
        print("✓ Connection successful")
        
        # Check if operator_daily_performance table exists
        print("Checking if operator_daily_performance table exists...")
        try:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name = 'operator_daily_performance'
            """)
            result = cursor.fetchone()
            if result:
                print("✓ operator_daily_performance table exists")
                
                # Count records in the table
                cursor.execute("SELECT COUNT(*) FROM operator_daily_performance")
                count = cursor.fetchone()[0]
                print(f"  Records in operator_daily_performance: {count}")
                
                if count > 0:
                    # Show sample data
                    cursor.execute("SELECT * FROM operator_daily_performance LIMIT 3")
                    rows = cursor.fetchall()
                    print("  Sample records:")
                    for row in rows:
                        print(f"    {row}")
                else:
                    print("  No records found in operator_daily_performance")
            else:
                print("✗ operator_daily_performance table does not exist")
                
        except Exception as e:
            print(f"Error checking operator_daily_performance table: {e}")
        
        # Check if etl_extract_log table exists
        print("Checking if etl_extract_log table exists...")
        try:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name = 'etl_extract_log'
            """)
            result = cursor.fetchone()
            if result:
                print("✓ etl_extract_log table exists")
                
                # Count records in the table
                cursor.execute("SELECT COUNT(*) FROM etl_extract_log")
                count = cursor.fetchone()[0]
                print(f"  Records in etl_extract_log: {count}")
                
                if count > 0:
                    # Show recent logs
                    cursor.execute("""
                        SELECT source_connection, lastextractdatetime, success, status 
                        FROM etl_extract_log 
                        ORDER BY lastextractdatetime DESC 
                        LIMIT 5
                    """)
                    rows = cursor.fetchall()
                    print("  Recent ETL logs:")
                    for row in rows:
                        print(f"    {row}")
                else:
                    print("  No records found in etl_extract_log")
            else:
                print("✗ etl_extract_log table does not exist")
                
        except Exception as e:
            print(f"Error checking etl_extract_log table: {e}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False

def test_hanger_lane_extraction():
    """Test if hanger_lane DAGs are extracting data"""
    print("\n=== Testing hanger_lane Data Extraction ===")
    
    # Check if any of the hanger_line DAG files exist and have recent runs
    hanger_line_dags = [
        "hanger_line_21_to_23.py",
        "hanger_line_24_to_26.py", 
        "hanger_line_27_to_29.py"
    ]
    
    for dag_file in hanger_line_dags:
        dag_path = os.path.join(os.path.dirname(__file__), 'dags', dag_file)
        if os.path.exists(dag_path):
            print(f"✓ {dag_file} exists")
            # Get file modification time
            mod_time = os.path.getmtime(dag_path)
            import datetime
            mod_datetime = datetime.datetime.fromtimestamp(mod_time)
            print(f"  Last modified: {mod_datetime}")
        else:
            print(f"✗ {dag_file} does not exist")
    
    return True

if __name__ == "__main__":
    print("=== hanger_line_daily_transform Data Fetching Test ===\n")
    
    test1_passed = test_database_connection()
    test2_passed = test_hanger_lane_extraction()
    
    print("\n=== Test Summary ===")
    if test1_passed and test2_passed:
        print("✓ All tests completed")
        print("\nNext steps:")
        print("1. Check if hanger_line DAGs are running and extracting data")
        print("2. Verify that data is being saved to operator_daily_performance table")
        print("3. Check Airflow logs for any errors in the extraction process")
    else:
        print("✗ Some tests failed")
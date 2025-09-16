import psycopg2
from datetime import date

def test_staging_table_direct():
    """Test creating staging table directly and inserting data"""
    try:
        # Connection parameters
        host = "172.16.7.6"
        port = "5432"
        database = "ssg"
        user = "postgres"
        password = "P@kistan12"
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        
        # Create staging table directly with the same structure as odp_date_oc
        staging_table = "odp_date_oc_staging_direct"
        
        # Drop staging table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
        
        # Create staging table directly
        cursor.execute(f"""
            CREATE TABLE {staging_table} (
                ODP_Date DATE,
                Shift VARCHAR(10),
                ODPD_ST_Key INTEGER,
                ST_ID VARCHAR(50),
                ST_Description VARCHAR(100),
                ODPD_Lot_Number VARCHAR(50),
                ODPD_OC_Key INTEGER,
                OC_Description VARCHAR(100),
                OC_Standard_Time NUMERIC(10, 2),
                ODPD_Actual_Time NUMERIC(10, 2),
                ODPD_CM_Key INTEGER,
                CM_Description VARCHAR(100),
                ODPD_SM_Key INTEGER,
                SM_Description VARCHAR(100),
                source_connection VARCHAR(50),
                ODPD_Quantity INTEGER,
                Loading_Qty INTEGER,
                UnLoading_Qty INTEGER,
                record_count INTEGER,
                created_at TIMESTAMP
            );
        """)
        
        # Test data with correct column names (uppercase)
        test_data = {
            'ODP_Date': date.today(),
            'Shift': 'A',
            'ODPD_ST_Key': 1,
            'ST_ID': 'ST001',
            'ST_Description': 'Test Station',
            'ODPD_Lot_Number': 'LOT001',
            'ODPD_OC_Key': 1,
            'OC_Description': 'Test Operation',
            'OC_Standard_Time': 10.5,
            'ODPD_Actual_Time': 9.8,
            'ODPD_CM_Key': 1,
            'CM_Description': 'Test Component',
            'ODPD_SM_Key': 1,
            'SM_Description': 'Test Material',
            'source_connection': 'test_conn',
            'ODPD_Quantity': 100,
            'Loading_Qty': 50,
            'UnLoading_Qty': 50,
            'record_count': 1,
            'created_at': '2025-09-16 10:00:00'
        }
        
        # Get column names and values
        columns = list(test_data.keys())
        values = list(test_data.values())
        
        # Create INSERT statement
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {staging_table} ({columns_str}) VALUES ({placeholders})"
        
        print(f"Executing SQL: {insert_sql}")
        print(f"Values: {values}")
        
        # Execute insert
        cursor.execute(insert_sql, values)
        conn.commit()
        
        print("Data inserted successfully!")
        
        # Clean up
        cursor.execute(f"DROP TABLE {staging_table};")
        conn.commit()
        
        cursor.close()
        conn.close()
        
        print("Test completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_staging_table_direct()
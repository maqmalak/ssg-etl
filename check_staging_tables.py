import psycopg2

def check_table_structures():
    """Check the structure of the staging tables"""
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
        
        # Check structure of odp_date_oc_staging
        print("Checking structure of odp_date_oc_staging...")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'odp_date_oc_staging'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        if columns:
            print("Columns in odp_date_oc_staging:")
            for col_name, data_type in columns:
                print(f"  - {col_name} ({data_type})")
        else:
            print("Table odp_date_oc_staging does not exist or has no columns")
            
        # Check structure of odp_date_shift_staging
        print("\nChecking structure of odp_date_shift_staging...")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'odp_date_shift_staging'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        if columns:
            print("Columns in odp_date_shift_staging:")
            for col_name, data_type in columns:
                print(f"  - {col_name} ({data_type})")
        else:
            print("Table odp_date_shift_staging does not exist or has no columns")
            
        # Check structure of odp_date_employee_staging
        print("\nChecking structure of odp_date_employee_staging...")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'odp_date_employee_staging'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        if columns:
            print("Columns in odp_date_employee_staging:")
            for col_name, data_type in columns:
                print(f"  - {col_name} ({data_type})")
        else:
            print("Table odp_date_employee_staging does not exist or has no columns")
            
        cursor.close()
        conn.close()
        
        print("\nCheck completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_table_structures()
import psycopg2

def check_target_table_structures():
    """Check the structure of the target tables"""
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
        
        # Check structure of odp_date_oc
        print("Checking structure of odp_date_oc...")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'odp_date_oc'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        if columns:
            print("Columns in odp_date_oc:")
            for col_name, data_type in columns:
                print(f"  - {col_name} ({data_type})")
        else:
            print("Table odp_date_oc does not exist or has no columns")
            
        # Check structure of odp_date_shift
        print("\nChecking structure of odp_date_shift...")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'odp_date_shift'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        if columns:
            print("Columns in odp_date_shift:")
            for col_name, data_type in columns:
                print(f"  - {col_name} ({data_type})")
        else:
            print("Table odp_date_shift does not exist or has no columns")
            
        # Check structure of odp_date_employee
        print("\nChecking structure of odp_date_employee...")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'odp_date_employee'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        if columns:
            print("Columns in odp_date_employee:")
            for col_name, data_type in columns:
                print(f"  - {col_name} ({data_type})")
        else:
            print("Table odp_date_employee does not exist or has no columns")
            
        cursor.close()
        conn.close()
        
        print("\nCheck completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_target_table_structures()
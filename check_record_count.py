import psycopg2

def check_table_has_record_count():
    """Check if the target tables have a record_count column"""
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
        
        # Check if odp_date_oc has record_count column
        print("Checking if odp_date_oc has record_count column...")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'odp_date_oc' AND column_name = 'record_count'
        """)
        result = cursor.fetchone()
        if result:
            print("  - odp_date_oc has record_count column")
        else:
            print("  - odp_date_oc does NOT have record_count column")
            
        # Check if odp_date_shift has record_count column
        print("Checking if odp_date_shift has record_count column...")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'odp_date_shift' AND column_name = 'record_count'
        """)
        result = cursor.fetchone()
        if result:
            print("  - odp_date_shift has record_count column")
        else:
            print("  - odp_date_shift does NOT have record_count column")
            
        # Check if odp_date_employee has record_count column
        print("Checking if odp_date_employee has record_count column...")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'odp_date_employee' AND column_name = 'record_count'
        """)
        result = cursor.fetchone()
        if result:
            print("  - odp_date_employee has record_count column")
        else:
            print("  - odp_date_employee does NOT have record_count column")
            
        cursor.close()
        conn.close()
        
        print("\nCheck completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_table_has_record_count()
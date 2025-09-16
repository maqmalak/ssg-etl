import psycopg2

def check_triggers():
    """Check if there are any triggers on the odp_date_oc table"""
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
        
        # Check for triggers on odp_date_oc table
        print("Checking triggers on odp_date_oc table...")
        cursor.execute("""
            SELECT trigger_name, event_manipulation, event_object_table
            FROM information_schema.triggers
            WHERE event_object_table = 'odp_date_oc'
        """)
        triggers = cursor.fetchall()
        
        if triggers:
            print("Triggers on odp_date_oc:")
            for trigger_name, event_manipulation, event_object_table in triggers:
                print(f"  - {trigger_name} ({event_manipulation})")
        else:
            print("No triggers found on odp_date_oc table")
            
        # Check for any functions that might be causing this issue
        print("\nChecking for functions that might reference 'odp_date'...")
        cursor.execute("""
            SELECT proname, prosrc
            FROM pg_proc
            WHERE prosrc ILIKE '%odp_date%'
        """)
        functions = cursor.fetchall()
        
        if functions:
            print("Functions that reference 'odp_date':")
            for func_name, func_src in functions:
                print(f"  - {func_name}")
        else:
            print("No functions found that reference 'odp_date'")
            
        cursor.close()
        conn.close()
        
        print("\nCheck completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_triggers()
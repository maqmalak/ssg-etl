import psycopg2

def check_table_details():
    """Check detailed information about the odp_date_oc table"""
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
        
        # Check table constraints
        print("Checking constraints on odp_date_oc table...")
        cursor.execute("""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'odp_date_oc'
        """)
        constraints = cursor.fetchall()
        
        if constraints:
            print("Constraints on odp_date_oc:")
            for constraint_name, constraint_type in constraints:
                print(f"  - {constraint_name} ({constraint_type})")
        else:
            print("No constraints found on odp_date_oc table")
            
        # Check table indexes
        print("\nChecking indexes on odp_date_oc table...")
        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = 'odp_date_oc'
        """)
        indexes = cursor.fetchall()
        
        if indexes:
            print("Indexes on odp_date_oc:")
            for indexname, indexdef in indexes:
                print(f"  - {indexname}: {indexdef}")
        else:
            print("No indexes found on odp_date_oc table")
            
        # Check table inheritance
        print("\nChecking inheritance for odp_date_oc table...")
        cursor.execute("""
            SELECT inhparent::regclass AS parent_table
            FROM pg_inherits
            WHERE inhrelid = 'odp_date_oc'::regclass
        """)
        inheritance = cursor.fetchall()
        
        if inheritance:
            print("Inheritance for odp_date_oc:")
            for (parent_table,) in inheritance:
                print(f"  - Inherits from: {parent_table}")
        else:
            print("No inheritance found for odp_date_oc table")
            
        # Check table rules
        print("\nChecking rules for odp_date_oc table...")
        cursor.execute("""
            SELECT rulename, definition
            FROM pg_rules
            WHERE tablename = 'odp_date_oc'
        """)
        rules = cursor.fetchall()
        
        if rules:
            print("Rules for odp_date_oc:")
            for rulename, definition in rules:
                print(f"  - {rulename}: {definition}")
        else:
            print("No rules found for odp_date_oc table")
            
        cursor.close()
        conn.close()
        
        print("\nCheck completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_table_details()
import psycopg2
import os

def test_database_connection():
    """Test the database connection and check table structure"""
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
        
        # Check if the target table exists and get its structure
        print("Checking structure of odp_date_oc table...")
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
            
        # Try to create a staging table and check its structure
        print("\nCreating staging table...")
        staging_table = "odp_date_oc_staging"
        
        # Drop staging table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
        
        # Create staging table with same structure as target table
        cursor.execute(f"""
            CREATE TABLE {staging_table} (LIKE odp_date_oc INCLUDING ALL);
        """)
        
        # Check the structure of the staging table
        print(f"Checking structure of {staging_table}...")
        cursor.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{staging_table}'
            ORDER BY ordinal_position
        """)
        staging_columns = cursor.fetchall()
        
        if staging_columns:
            print(f"Columns in {staging_table}:")
            for col_name, data_type in staging_columns:
                print(f"  - {col_name} ({data_type})")
        else:
            print(f"Table {staging_table} does not exist or has no columns")
            
        # Clean up
        cursor.execute(f"DROP TABLE {staging_table};")
        conn.commit()
        
        cursor.close()
        conn.close()
        
        print("\nTest completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_database_connection()
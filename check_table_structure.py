"""
Simple script to check table structure
"""

import psycopg2
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_database_connection():
    """Get database connection with proper error handling"""
    try:
        # Get connection parameters
        host = os.getenv("POSTGRES_HOST", "172.16.7.6")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "ssg")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "P@kistan12")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=30
        )
        
        return conn
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def check_table_structure():
    """Check the actual structure of the aggregated tables"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        tables = ['opd_date_oc', 'opd_date_shift', 'opd_date_employee']
        
        for table in tables:
            try:
                logger.info(f"\n=== Checking structure of {table} ===")
                
                # Get column names
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position
                """)
                columns = cursor.fetchall()
                
                if columns:
                    logger.info(f"Columns in {table}:")
                    for col_name, data_type in columns:
                        logger.info(f"  - {col_name} ({data_type})")
                else:
                    logger.info(f"No columns found in {table} (table may not exist)")
                    
                # Check if table has any data
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    logger.info(f"Total records in {table}: {count}")
                    
                    if count > 0:
                        # Show sample data
                        cursor.execute(f"SELECT * FROM {table} LIMIT 3")
                        sample_rows = cursor.fetchall()
                        logger.info(f"Sample records from {table}:")
                        for i, row in enumerate(sample_rows):
                            logger.info(f"  Row {i+1}: {row}")
                except Exception as e:
                    logger.error(f"Error querying {table}: {e}")
                    
            except Exception as e:
                logger.error(f"Error checking {table}: {e}")
        
        # Also check the source table
        logger.info(f"\n=== Checking structure of operator_daily_performance ===")
        try:
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'operator_daily_performance'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            
            if columns:
                logger.info("Columns in operator_daily_performance:")
                for col_name, data_type in columns:
                    logger.info(f"  - {col_name} ({data_type})")
        except Exception as e:
            logger.error(f"Error checking operator_daily_performance: {e}")
            
    except Exception as e:
        logger.error(f"Error during table structure check: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_table_structure()
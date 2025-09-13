"""
Validation script for aggregated hanger line data tables
"""

import psycopg2
import os
import logging
from datetime import datetime, timedelta

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

def validate_aggregated_tables():
    """Validate the aggregated tables have been populated correctly"""
    logger.info("Starting validation of aggregated tables...")
    
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Validate opd_date_oc table
        logger.info("Validating opd_date_oc table...")
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT ODP_Date) as unique_dates,
                COUNT(DISTINCT OC_Description) as unique_operations,
                COUNT(DISTINCT source_connection) as unique_lines,
                SUM(ODPD_Quantity) as total_quantity,
                MAX(ODP_Date) as latest_date,
                MIN(ODP_Date) as earliest_date
            FROM opd_date_oc
        """)
        oc_stats = cursor.fetchone()
        oc_stats_dict = dict(zip([
            'total_records', 'unique_dates', 'unique_operations', 
            'unique_lines', 'total_quantity', 'latest_date', 'earliest_date'
        ], oc_stats))
        logger.info(f"opd_date_oc statistics: {oc_stats_dict}")
        
        # Validate opd_date_shift table
        logger.info("Validating opd_date_shift table...")
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT ODP_Date) as unique_dates,
                COUNT(DISTINCT Shift) as unique_shifts,
                COUNT(DISTINCT source_connection) as unique_lines,
                SUM(ODPD_Quantity) as total_quantity,
                MAX(ODP_Date) as latest_date,
                MIN(ODP_Date) as earliest_date
            FROM opd_date_shift
        """)
        shift_stats = cursor.fetchone()
        shift_stats_dict = dict(zip([
            'total_records', 'unique_dates', 'unique_shifts', 
            'unique_lines', 'total_quantity', 'latest_date', 'earliest_date'
        ], shift_stats))
        logger.info(f"opd_date_shift statistics: {shift_stats_dict}")
        
        # Validate opd_date_employee table
        logger.info("Validating opd_date_employee table...")
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT ODP_Date) as unique_dates,
                COUNT(DISTINCT ODP_EM_Key) as unique_employees,
                COUNT(DISTINCT source_connection) as unique_lines,
                SUM(ODPD_Quantity) as total_quantity,
                MAX(ODP_Date) as latest_date,
                MIN(ODP_Date) as earliest_date
            FROM opd_date_employee
        """)
        employee_stats = cursor.fetchone()
        employee_stats_dict = dict(zip([
            'total_records', 'unique_dates', 'unique_employees', 
            'unique_lines', 'total_quantity', 'latest_date', 'earliest_date'
        ], employee_stats))
        logger.info(f"opd_date_employee statistics: {employee_stats_dict}")
        
        # Cross-validation: Check if totals match across tables
        logger.info("Performing cross-validation...")
        if oc_stats[4] == shift_stats[4] == employee_stats[4]:
            logger.info("✅ Quantity totals match across all aggregated tables")
        else:
            logger.warning("⚠ Quantity totals do not match across tables")
            logger.warning(f"  opd_date_oc total: {oc_stats[4]}")
            logger.warning(f"  opd_date_shift total: {shift_stats[4]}")
            logger.warning(f"  opd_date_employee total: {employee_stats[4]}")
        
        # Check data freshness
        logger.info("Checking data freshness...")
        for table_name, stats in [
            ("opd_date_oc", oc_stats),
            ("opd_date_shift", shift_stats),
            ("opd_date_employee", employee_stats)
        ]:
            latest_date = stats[5]
            if latest_date:
                days_old = (datetime.now().date() - latest_date).days
                if days_old <= 2:
                    logger.info(f"✅ {table_name} has recent data ({days_old} days old)")
                else:
                    logger.warning(f"⚠ {table_name} data is {days_old} days old")
        
        # Check for data completeness
        logger.info("Checking data completeness...")
        tables_with_issues = []
        for table_name, stats in [
            ("opd_date_oc", oc_stats),
            ("opd_date_shift", shift_stats),
            ("opd_date_employee", employee_stats)
        ]:
            if stats[0] == 0:
                logger.error(f"❌ {table_name} is empty")
                tables_with_issues.append(table_name)
            elif stats[1] == 0:
                logger.error(f"❌ {table_name} has no dates")
                tables_with_issues.append(table_name)
            elif stats[2] == 0:
                logger.error(f"❌ {table_name} has no grouping keys")
                tables_with_issues.append(table_name)
        
        if not tables_with_issues:
            logger.info("✅ All aggregated tables have data and appear complete")
            return True
        else:
            logger.error(f"❌ Issues found in tables: {tables_with_issues}")
            return False
            
    except Exception as e:
        logger.error(f"Error during validation: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def validate_data_quality():
    """Validate data quality in aggregated tables"""
    logger.info("Starting data quality validation...")
    
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Check for negative quantities
        logger.info("Checking for negative quantities...")
        for table in ['opd_date_oc', 'opd_date_shift', 'opd_date_employee']:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE ODPD_Quantity < 0")
            negative_count = cursor.fetchone()[0]
            if negative_count > 0:
                logger.warning(f"⚠ Found {negative_count} negative quantities in {table}")
            else:
                logger.info(f"✅ No negative quantities in {table}")
        
        # Check for null values in key columns
        logger.info("Checking for null values in key columns...")
        null_checks = [
            ("opd_date_oc", ["ODP_Date", "OC_Description", "source_connection"]),
            ("opd_date_shift", ["ODP_Date", "Shift", "source_connection"]),
            ("opd_date_employee", ["ODP_Date", "ODP_EM_Key", "source_connection"])
        ]
        
        for table, columns in null_checks:
            for column in columns:
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
                null_count = cursor.fetchone()[0]
                if null_count > 0:
                    logger.warning(f"⚠ Found {null_count} null values in {table}.{column}")
                else:
                    logger.info(f"✅ No null values in {table}.{column}")
        
        # Check for duplicate records (based on primary keys)
        logger.info("Checking for duplicate records...")
        duplicate_checks = [
            ("opd_date_oc", ["ODP_Date", "OC_Description", "source_connection"]),
            ("opd_date_shift", ["ODP_Date", "Shift", "source_connection"]),
            ("opd_date_employee", ["ODP_Date", "ODP_EM_Key", "source_connection"])
        ]
        
        for table, key_columns in duplicate_checks:
            key_columns_str = ", ".join(key_columns)
            cursor.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT ({key_columns_str}))
                FROM {table}
            """)
            duplicate_count = cursor.fetchone()[0]
            if duplicate_count > 0:
                logger.warning(f"⚠ Found {duplicate_count} duplicate records in {table}")
            else:
                logger.info(f"✅ No duplicate records in {table}")
        
        logger.info("Data quality validation completed")
        return True
        
    except Exception as e:
        logger.error(f"Error during data quality validation: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def generate_validation_report():
    """Generate a comprehensive validation report"""
    logger.info("=" * 50)
    logger.info("HANGER LINE AGGREGATED DATA VALIDATION REPORT")
    logger.info("=" * 50)
    
    # Run validations
    data_validation_passed = validate_aggregated_tables()
    quality_validation_passed = validate_data_quality()
    
    # Generate summary
    logger.info("=" * 50)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 50)
    
    if data_validation_passed and quality_validation_passed:
        logger.info("✅ ALL VALIDATIONS PASSED")
        logger.info("The aggregated tables are complete, consistent, and of good quality.")
        return True
    else:
        logger.error("❌ VALIDATIONS FAILED")
        if not data_validation_passed:
            logger.error("  - Data completeness validation failed")
        if not quality_validation_passed:
            logger.error("  - Data quality validation failed")
        logger.error("Please review the detailed logs above for specific issues.")
        return False

if __name__ == "__main__":
    success = generate_validation_report()
    exit(0 if success else 1)
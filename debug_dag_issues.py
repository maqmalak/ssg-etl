#!/usr/bin/env python3
"""
Debug script to check DAG logs and fix null skipping and does not exist errors.
"""

import sys
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# Add the scripts directory to the Python path
scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
sys.path.append(os.path.abspath(scripts_path))

# Import the necessary functions
from dags.hourly_hanger_line_production_upsert import (
    fetch_recent_source_data, 
    perform_aggregations,
    upsert_aggregated_table
)
from dags.upsert_utils import upsert_data_via_postgres
from scripts.create_table_hourly import create_hourly_table_if_not_exists

def debug_dag_issues():
    """Debug DAG issues and fix null skipping and does not exist errors."""
    print("Starting DAG issue debugging...")
    
    # 1. Verify tables exist
    print("1. Verifying tables exist...")
    try:
        password = quote_plus('P@kistan12')
        uri = f'postgresql://postgres:{password}@172.16.7.6:5432/ssg'
        engine = create_engine(uri)
        
        # Check if tables exist
        table_names = [
            'odp_hourly_oc',
            'odp_hourly_shift', 
            'odp_hourly_employee',
            'odp_hourly_summary'
        ]
        
        for table_name in table_names:
            try:
                # Try to query the table
                result = engine.execute(text(f"SELECT COUNT(*) FROM {table_name} LIMIT 1"))
                count = result.fetchone()[0]
                print(f"   ✓ Table {table_name} exists with {count} records")
            except Exception as e:
                print(f"   ✗ Table {table_name} error: {e}")
                
        engine.dispose()
    except Exception as e:
        print(f"   ✗ Database connection error: {e}")
    
    # 2. Fetch and analyze source data
    print("\n2. Fetching and analyzing source data...")
    try:
        source_data = fetch_recent_source_data(hours_back=1)
        print(f"   ✓ Fetched {len(source_data)} records from source")
        
        # Check for null values in key columns
        key_columns = ['ODP_Date', 'Shift', 'ODP_EM_Key', 'ST_ID', 'ODPD_ST_Key']
        print("   Key column null analysis:")
        for col in key_columns:
            if col in source_data.columns:
                null_count = source_data[col].isna().sum()
                print(f"     {col}: {null_count}/{len(source_data)} null values")
            else:
                print(f"     {col}: Column not found")
                
    except Exception as e:
        print(f"   ✗ Error fetching/analyzing source data: {e}")
        return False
    
    # 3. Perform aggregations and check for nulls
    print("\n3. Performing aggregations and checking for nulls...")
    try:
        if source_data.empty:
            print("   ! No data to aggregate")
            return True
            
        aggregated_results = perform_aggregations(source_data)
        print("   ✓ Aggregations completed:")
        for table_name, df in aggregated_results.items():
            print(f"     - {table_name}: {len(df)} records")
            
            # Check for null values in key columns for each aggregated table
            if not df.empty:
                print(f"       Null analysis for {table_name}:")
                # Check first few rows
                sample_data = df.head(3)
                print(sample_data)
                
    except Exception as e:
        print(f"   ✗ Error in aggregations: {e}")
        return False
    
    # 4. Test upsert with better error handling
    print("\n4. Testing upsert with better error handling...")
    try:
        # Define connection parameters directly
        connection_params = {
            "host": "172.16.7.6",
            "port": "5432",
            "database": "ssg",
            "user": "postgres",
            "password": "P@kistan12"
        }
        
        # Test with one table
        table_name = 'odp_hourly_summary'
        aggregated_data = aggregated_results.get(table_name)
        
        if aggregated_data is not None and not aggregated_data.empty:
            print(f"   Testing upsert for {table_name} with {len(aggregated_data)} records")
            
            # Check for null values in the data
            print("   Checking for null values in aggregated data...")
            null_columns = aggregated_data.columns[aggregated_data.isnull().any()].tolist()
            if null_columns:
                print(f"     Columns with null values: {null_columns}")
                # Show sample null values
                for col in null_columns[:3]:  # Show first 3 columns with nulls
                    null_rows = aggregated_data[aggregated_data[col].isnull()]
                    if not null_rows.empty:
                        print(f"     Sample null values in {col}:")
                        print(null_rows[[col]].head(2))
            else:
                print("     No null values found in aggregated data")
                
            # Try upsert
            key_columns = [
                'hour_timestamp', 'ODP_Date', 'Shift', 'station_id', 'station_description', 
                'operation_code', 'source_connection'
            ]
            
            # Convert DataFrame to list of dictionaries for upsert
            data = aggregated_data.to_dict('records')
            print(f"   Attempting to upsert {len(data)} records...")
            
            # Test connection first
            try:
                from psycopg2 import connect
                conn = connect(
                    host=connection_params["host"],
                    port=connection_params["port"],
                    database=connection_params["database"],
                    user=connection_params["user"],
                    password=connection_params["password"]
                )
                conn.close()
                print("   ✓ Database connection successful")
            except Exception as e:
                print(f"   ✗ Database connection failed: {e}")
                return False
                
            # Perform upsert with better error handling
            success = upsert_data_via_postgres(data, table_name, key_columns, connection_params)
            
            if success:
                print(f"   ✓ Successfully tested upsert for {table_name}")
            else:
                print(f"   ✗ Failed to upsert data to {table_name}")
        else:
            print(f"   ! No data to upsert for {table_name}")
            
    except Exception as e:
        print(f"   ✗ Error in upsert test: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n✓ DAG issue debugging completed!")
    return True

if __name__ == "__main__":
    debug_dag_issues()
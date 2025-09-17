#!/usr/bin/env python3
"""
Test script to demonstrate the hourly aggregation process with correct database connection.
"""

import sys
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
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

def test_complete_process():
    """Test the complete hourly aggregation and upsert process."""
    print("Starting complete hourly aggregation test...")
    
    # 1. Create tables if they don't exist
    print("1. Creating tables if they don't exist...")
    try:
        password = quote_plus('P@kistan12')
        uri = f'postgresql://postgres:{password}@172.16.7.6:5432/ssg'
        engine = create_engine(uri)
        create_hourly_table_if_not_exists(engine)
        engine.dispose()
        print("   ✓ Tables created/verified successfully")
    except Exception as e:
        print(f"   ✗ Error creating tables: {e}")
        return False
    
    # 2. Fetch recent source data
    print("2. Fetching recent source data...")
    try:
        source_data = fetch_recent_source_data(hours_back=1)
        print(f"   ✓ Fetched {len(source_data)} records from source")
    except Exception as e:
        print(f"   ✗ Error fetching source data: {e}")
        return False
    
    # 3. Perform aggregations
    print("3. Performing aggregations...")
    try:
        if source_data.empty:
            print("   ! No data to aggregate")
            return True
            
        aggregated_results = perform_aggregations(source_data)
        print("   ✓ Aggregations completed:")
        for table_name, df in aggregated_results.items():
            print(f"     - {table_name}: {len(df)} records")
    except Exception as e:
        print(f"   ✗ Error in aggregations: {e}")
        return False
    
    # 4. Test upsert for one table (using correct connection params)
    print("4. Testing upsert for hourly_summary table...")
    try:
        # Define connection parameters directly
        connection_params = {
            "host": "172.16.7.6",
            "port": "5432",
            "database": "ssg",
            "user": "postgres",
            "password": "P@kistan12"
        }
        
        # Get the aggregated data for one table
        table_name = 'odp_hourly_summary'
        aggregated_data = aggregated_results.get('odp_hourly_summary')
        
        if aggregated_data is not None and not aggregated_data.empty:
            # Define key columns for this table
            key_columns = [
                'hour_timestamp', 'ODP_Date', 'Shift', 'station_id', 'station_description', 
                'operation_code', 'source_connection'
            ]
            
            print(f"   Attempting to upsert {len(aggregated_data)} records to {table_name}")
            
            # Convert DataFrame to list of dictionaries for upsert
            data = aggregated_data.to_dict('records')
            
            # Create a mapping from lowercase column names to database column names
            column_mapping = {
                'odp_date': 'ODP_Date',
                'shift': 'Shift',
                'odp_em_key': 'ODP_EM_Key',
                'em_rfid': 'EM_RFID',
                'em_department': 'EM_Department',
                'em_first_name': 'EM_FirstName',
                'em_last_name': 'EM_LastName',
                'odp_actual_clock_in': 'ODP_Actual_Clock_In',
                'odp_actual_clock_out': 'ODP_Actual_Clock_Out',
                'odp_shift_clock_in': 'ODP_Shift_Clock_In',
                'odp_shift_clock_out': 'ODP_Shift_Clock_Out',
                'odp_current_station': 'ODP_Current_Station',
                'odpd_workstation': 'ODPD_Workstation',
                'odpd_wc_key': 'ODPD_WC_Key',
                'odpd_quantity': 'ODPD_Quantity',
                'odpd_st_key': 'ODPD_ST_Key',
                'st_id': 'ST_ID',
                'st_description': 'ST_Description',
                'odpd_lot_number': 'ODPD_Lot_Number',
                'odpd_oc_key': 'ODPD_OC_Key',
                'oc_description': 'OC_Description',
                'loading_qty': 'Loading_Qty',
                'unloading_qty': 'UnLoading_Qty',
                'oc_standard_time': 'OC_Standard_Time',
                'odpd_actual_time': 'ODPD_Actual_Time',
                'odpd_cm_key': 'ODPD_CM_Key',
                'cm_description': 'CM_Description',
                'odpd_sm_key': 'ODPD_SM_Key',
                'sm_description': 'SM_Description',
                'odpd_is_overtime': 'ODPD_Is_Overtime',
                'odpd_overtime_factor': 'ODPD_Overtime_Factor',
                'odpd_stpo_key': 'ODPD_STPO_Key',
                'source_connection': 'source_connection',
                'record_count': 'record_count',
                'created_at': 'created_at',
                'hour_timestamp': 'hour_timestamp',
                'station_id': 'station_id',
                'station_description': 'station_description',
                'operation_code': 'operation_code',
                'total_quantity': 'total_quantity',
                'total_loading_qty': 'total_loading_qty',
                'total_unloading_qty': 'total_unloading_qty',
                'avg_actual_time': 'avg_actual_time',
                'total_employees': 'total_employees'
            }
            
            # Map column names in the data to match database schema
            mapped_data = []
            for record in data:
                mapped_record = {}
                for key, value in record.items():
                    # Map the key to the database column name
                    mapped_key = column_mapping.get(key.lower(), key)
                    mapped_record[mapped_key] = value
                mapped_data.append(mapped_record)
            
            # Map key columns to match database schema
            mapped_key_columns = [column_mapping.get(col.lower(), col) for col in key_columns]
            
            # Perform upsert with mapped data and key columns
            success = upsert_data_via_postgres(mapped_data, table_name, mapped_key_columns, connection_params)
            
            if success:
                print(f"   ✓ Successfully tested upsert for {table_name}")
            else:
                print(f"   ✗ Failed to upsert data to {table_name}")
        else:
            print(f"   ! No data to upsert for {table_name}")
            
    except Exception as e:
        print(f"   ✗ Error in upsert test: {e}")
        return False
    
    print("\n✓ Complete hourly aggregation test finished successfully!")
    return True

if __name__ == "__main__":
    success = test_complete_process()
    if success:
        print("\nAll tests passed!")
        sys.exit(0)
    else:
        print("\nSome tests failed!")
        sys.exit(1)
#!/usr/bin/env python3
"""
Enhanced diagnostic script to check the actual data processing pipeline.
"""

import sys
import os
import pandas as pd
from datetime import datetime, date

# Add the scripts directory to the Python path
scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
sys.path.append(os.path.abspath(scripts_path))

def diagnose_actual_processing():
    """Diagnose the actual data processing pipeline."""
    print("Diagnosing actual data processing pipeline...")
    
    # Import the necessary functions
    try:
        from dags.hourly_hanger_line_production_upsert import (
            fetch_recent_source_data, 
            perform_aggregations
        )
        print("✓ Successfully imported DAG functions")
    except Exception as e:
        print(f"✗ Error importing DAG functions: {e}")
        return
    
    # Fetch recent source data
    print("\n1. Fetching recent source data...")
    try:
        source_data = fetch_recent_source_data(hours_back=1)
        print(f"   ✓ Fetched {len(source_data)} records from source")
    except Exception as e:
        print(f"   ✗ Error fetching source data: {e}")
        return
    
    # Perform aggregations
    print("\n2. Performing aggregations...")
    try:
        if source_data.empty:
            print("   ! No data to aggregate")
            return
            
        aggregated_results = perform_aggregations(source_data)
        print("   ✓ Aggregations completed:")
        for table_name, df in aggregated_results.items():
            print(f"     - {table_name}: {len(df)} records")
    except Exception as e:
        print(f"   ✗ Error in aggregations: {e}")
        return
    
    # Check the odp_hourly_employee data specifically
    print("\n3. Analyzing odp_hourly_employee data...")
    employee_df = aggregated_results.get('odp_hourly_employee')
    if employee_df is not None and not employee_df.empty:
        print(f"   Found {len(employee_df)} employee records")
        
        # Define key columns
        key_columns = [
            'hour_timestamp', 'ODP_Date', 'Shift', 'ODP_EM_Key', 'EM_RFID', 
            'EM_Department', 'EM_FirstName', 'EM_LastName', 'ODPD_Workstation', 
            'ODPD_WC_Key', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 
            'ODPD_Lot_Number', 'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 
            'CM_Description', 'ODPD_SM_Key', 'SM_Description', 'ODPD_Is_Overtime', 
            'ODPD_STPO_Key', 'source_connection'
        ]
        
        print("   Key columns to check:")
        for col in key_columns:
            if col in employee_df.columns:
                null_count = employee_df[col].isna().sum()
                print(f"     {col}: {null_count}/{len(employee_df)} null values")
            else:
                print(f"     {col}: MISSING COLUMN")
        
        # Check first few records
        print("\n   First 3 records:")
        for i, (_, row) in enumerate(employee_df.head(3).iterrows()):
            print(f"     Record {i+1}:")
            for col in key_columns:
                if col in employee_df.columns:
                    value = row[col]
                    print(f"       {col}: {value} ({'NULL' if pd.isna(value) else 'OK'})")
                else:
                    print(f"       {col}: MISSING")
            print()
            
        # Test the upsert validation logic on actual data
        print("   Testing upsert validation on actual data:")
        data = employee_df.to_dict('records')
        valid_count = 0
        invalid_count = 0
        
        for i, record in enumerate(data[:10]):  # Check first 10 records
            if all(record.get(col) is not None for col in key_columns):
                valid_count += 1
            else:
                invalid_count += 1
                print(f"     Invalid record {i+1}:")
                for col in key_columns:
                    value = record.get(col)
                    if value is None or pd.isna(value):
                        print(f"       NULL key: {col} = {value}")
        
        print(f"   Validation results: {valid_count} valid, {invalid_count} invalid (out of {min(10, len(data))} checked)")
        
    else:
        print("   No employee data found")
        
    # Check the column mapping process
    print("\n4. Checking column mapping process...")
    column_mapping = {
        'odp_date': 'ODP_Date',
        'shift': 'Shift',
        'odp_em_key': 'ODP_EM_Key',
        'em_rfid': 'EM_RFID',
        'em_department': 'EM_Department',
        'em_first_name': 'EM_FirstName',
        'em_last_name': 'EM_LastName',
        'odpd_workstation': 'ODPD_Workstation',
        'odpd_wc_key': 'ODPD_WC_Key',
        'odpd_st_key': 'ODPD_ST_Key',
        'st_id': 'ST_ID',
        'st_description': 'ST_Description',
        'odpd_lot_number': 'ODPD_Lot_Number',
        'odpd_oc_key': 'ODPD_OC_Key',
        'oc_description': 'OC_Description',
        'odpd_cm_key': 'ODPD_CM_Key',
        'cm_description': 'CM_Description',
        'odpd_sm_key': 'ODPD_SM_Key',
        'sm_description': 'SM_Description',
        'odpd_is_overtime': 'ODPD_Is_Overtime',
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
    
    print("   Column mapping verification:")
    key_columns = [
        'hour_timestamp', 'ODP_Date', 'Shift', 'ODP_EM_Key', 'EM_RFID', 
        'EM_Department', 'EM_FirstName', 'EM_LastName', 'ODPD_Workstation', 
        'ODPD_WC_Key', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 
        'ODPD_Lot_Number', 'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 
        'CM_Description', 'ODPD_SM_Key', 'SM_Description', 'ODPD_Is_Overtime', 
        'ODPD_STPO_Key', 'source_connection'
    ]
    
    for col in key_columns:
        found = False
        for key, value in column_mapping.items():
            if value == col:
                found = True
                break
        if found:
            print(f"     ✓ {col} found in mapping")
        else:
            print(f"     ✗ {col} NOT found in mapping")

if __name__ == "__main__":
    diagnose_actual_processing()
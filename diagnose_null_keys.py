#!/usr/bin/env python3
"""
Diagnostic script to check why records are being skipped due to NULL keys.
"""

import sys
import os
import pandas as pd
from datetime import datetime, date

# Add the scripts directory to the Python path
scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
sys.path.append(os.path.abspath(scripts_path))

def diagnose_null_key_skipping():
    """Diagnose why records are being skipped due to NULL keys."""
    print("Diagnosing NULL key skipping issue...")
    
    # Define the key columns for odp_hourly_employee table
    key_columns = [
        'hour_timestamp', 'ODP_Date', 'Shift', 'ODP_EM_Key', 'EM_RFID', 
        'EM_Department', 'EM_FirstName', 'EM_LastName', 'ODPD_Workstation', 
        'ODPD_WC_Key', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 
        'ODPD_Lot_Number', 'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 
        'CM_Description', 'ODPD_SM_Key', 'SM_Description', 'ODPD_Is_Overtime', 
        'ODPD_STPO_Key', 'source_connection'
    ]
    
    # Create a sample record based on the logs
    sample_record = {
        'ODP_Date': date(2025, 9, 17), 
        'Shift': 'Night', 
        'ODP_EM_Key': 91357, 
        'EM_RFID': '091357', 
        'EM_Department': '026n', 
        'EM_FirstName': 'Hannan', 
        'EM_LastName': 'sajjad', 
        'ODPD_Workstation': '41', 
        'ODPD_WC_Key': 22, 
        'ODPD_ST_Key': 38, 
        'ST_ID': '9350', 
        'ST_Description': '9350-Classic Jacket', 
        'ODPD_Lot_Number': 'B332192', 
        'ODPD_OC_Key': 88880265, 
        'OC_Description': 'Final QC', 
        'ODPD_CM_Key': 41, 
        'CM_Description': 'Purple-10', 
        'ODPD_SM_Key': 34, 
        'SM_Description': '152', 
        'ODPD_Is_Overtime': False, 
        'ODPD_STPO_Key': 256, 
        'source_connection': 'line-26', 
        'ODPD_Quantity': 1, 
        'Loading_Qty': 0, 
        'UnLoading_Qty': 0, 
        'ODPD_Actual_Time': 455.0, 
        'ODPD_Overtime_Factor': 0.0, 
        'ODP_Actual_Clock_In': pd.Timestamp('2025-09-17 17:18:12'), 
        'record_count': 0, 
        'created_at': pd.Timestamp('2025-09-17 17:51:12.747944'), 
        'hour_timestamp': pd.Timestamp('2025-09-17 17:00:00')
    }
    
    print("Sample record:")
    for key, value in sample_record.items():
        print(f"  {key}: {value} ({type(value)})")
    
    print("\nKey columns to check:")
    for col in key_columns:
        print(f"  {col}")
    
    print("\nChecking for NULL values in key columns:")
    has_null_keys = False
    for col in key_columns:
        value = sample_record.get(col)
        if value is None:
            print(f"  NULL key found: {col} = {value}")
            has_null_keys = True
        elif pd.isna(value):
            print(f"  NaN key found: {col} = {value}")
            has_null_keys = True
        else:
            print(f"  ✓ {col} = {value}")
    
    if not has_null_keys:
        print("  No NULL keys found in sample record")
    
    # Test the validation logic used in upsert_utils.py
    print("\nTesting upsert validation logic:")
    if all(sample_record.get(col) is not None for col in key_columns):
        print("  ✓ Record would PASS validation")
    else:
        print("  ✗ Record would FAIL validation")
        # Check which specific columns are failing
        for col in key_columns:
            value = sample_record.get(col)
            if value is None:
                print(f"    NULL: {col}")
            elif pd.isna(value):
                print(f"    NaN: {col}")
    
    # Check if there's a mismatch between column names
    print("\nChecking column name mappings:")
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
    
    # Check if all key columns are in the column mapping
    for col in key_columns:
        if col not in column_mapping.values():
            # Check if it's a key we're looking for in lowercase mapping
            found = False
            for key, value in column_mapping.items():
                if value == col:
                    found = True
                    break
            if not found:
                print(f"  Warning: {col} not found in column mapping")

if __name__ == "__main__":
    diagnose_null_key_skipping()
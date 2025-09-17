#!/usr/bin/env python3
"""
Diagnostic script to understand why odp_date_employee aggregation produces 0 records.
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add the scripts directory to the Python path
scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
sys.path.append(os.path.abspath(scripts_path))

# Import the necessary functions
from dags.hourly_hanger_line_production_upsert import fetch_recent_source_data

def diagnose_employee_aggregation():
    """Diagnose the employee aggregation issue."""
    print("Starting employee aggregation diagnosis...")
    
    # Fetch recent source data
    print("1. Fetching recent source data...")
    try:
        source_data = fetch_recent_source_data(hours_back=1)
        print(f"   ✓ Fetched {len(source_data)} records from source")
        print(f"   Columns: {list(source_data.columns)}")
    except Exception as e:
        print(f"   ✗ Error fetching source data: {e}")
        return False
    
    # Check for employee-related columns
    print("\n2. Checking for employee-related columns...")
    employee_columns = [
        'ODP_EM_Key', 'EM_RFID', 'EM_Department', 'EM_FirstName', 'EM_LastName',
        'odp_em_key', 'em_rfid', 'em_department', 'em_first_name', 'em_last_name'
    ]
    
    found_columns = [col for col in employee_columns if col in source_data.columns]
    missing_columns = [col for col in employee_columns if col not in source_data.columns]
    
    print(f"   Found employee columns: {found_columns}")
    print(f"   Missing employee columns: {missing_columns}")
    
    # Check if we have employee data
    print("\n3. Checking employee data...")
    if 'ODP_EM_Key' in source_data.columns:
        employee_count = source_data['ODP_EM_Key'].notna().sum()
        unique_employees = source_data['ODP_EM_Key'].nunique()
        print(f"   Records with ODP_EM_Key: {employee_count}")
        print(f"   Unique employees: {unique_employees}")
        
        # Check sample data
        if employee_count > 0:
            sample_employee_data = source_data[source_data['ODP_EM_Key'].notna()].head(3)
            print("   Sample employee data:")
            for idx, row in sample_employee_data.iterrows():
                print(f"     Employee Key: {row['ODP_EM_Key']}, Name: {row.get('EM_FirstName', 'N/A')} {row.get('EM_LastName', 'N/A')}")
    
    # Check for grouping columns that might be causing issues
    print("\n4. Checking grouping columns for employee aggregation...")
    grouping_columns = [
        "ODP_Date", "Shift",
        "ODP_EM_Key", "EM_RFID", "EM_Department", "EM_FirstName", "EM_LastName",
        "ODP_Current_Station", "ODPD_Workstation", "ODPD_WC_Key",
        "ODPD_ST_Key", "ST_ID", "ST_Description", "ODPD_Lot_Number",
        "ODPD_OC_Key", "OC_Description",
        "ODPD_CM_Key", "CM_Description", "ODPD_SM_Key", "SM_Description",
        "ODPD_Is_Overtime", "ODPD_STPO_Key",
        "source_connection"
    ]
    
    print("   Grouping columns status:")
    for col in grouping_columns:
        if col in source_data.columns:
            non_null_count = source_data[col].notna().sum()
            print(f"     {col}: {non_null_count}/{len(source_data)} non-null")
        else:
            print(f"     {col}: MISSING")
    
    # Check if the combination of grouping columns results in unique records
    print("\n5. Checking uniqueness of grouping combinations...")
    available_grouping_cols = [col for col in grouping_columns if col in source_data.columns]
    if available_grouping_cols:
        try:
            # Check how many unique combinations we get
            unique_combinations = source_data.groupby(available_grouping_cols).size().reset_index(name='count')
            print(f"   Unique grouping combinations: {len(unique_combinations)}")
            
            # Check if any combinations have all required data
            non_null_combinations = unique_combinations.dropna()
            print(f"   Non-null combinations: {len(non_null_combinations)}")
            
            if len(non_null_combinations) > 0:
                print("   Sample combinations:")
                print(non_null_combinations.head(3))
        except Exception as e:
            print(f"   Error checking combinations: {e}")
    else:
        print("   No grouping columns available!")
    
    print("\n✓ Diagnosis complete!")
    return True

if __name__ == "__main__":
    diagnose_employee_aggregation()

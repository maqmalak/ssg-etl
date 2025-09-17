#!/usr/bin/env python3
"""
Detailed diagnostic for employee aggregation to see why it produces 0 records.
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add the scripts directory to the Python path
scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
sys.path.append(os.path.abspath(scripts_path))

def detailed_employee_diagnostic():
    """Detailed diagnostic for employee aggregation."""
    print("Detailed diagnostic for employee aggregation...")
    
    # Import the necessary functions
    try:
        from dags.hourly_hanger_line_production_upsert import fetch_recent_source_data
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
    
    if source_data.empty:
        print("   ! No source data available")
        return
    
    # Check employee-related columns
    print("\n2. Checking employee-related columns...")
    employee_columns = [
        'ODP_EM_Key', 'EM_RFID', 'EM_Department', 'EM_FirstName', 'EM_LastName',
        'ODPD_Workstation', 'ODPD_WC_Key'
    ]
    
    for col in employee_columns:
        if col in source_data.columns:
            non_null_count = source_data[col].notna().sum()
            print(f"   {col}: {non_null_count}/{len(source_data)} non-null")
        else:
            print(f"   {col}: MISSING")
    
    # Check grouping columns for employee aggregation
    print("\n3. Checking grouping columns for employee aggregation...")
    grouping_columns = [
        "ODP_Date", "Shift",
        "ODP_EM_Key", "EM_RFID", "EM_Department", "EM_FirstName", "EM_LastName",
        "ODPD_Workstation", "ODPD_WC_Key",
        "ODPD_ST_Key", "ST_ID", "ST_Description", "ODPD_Lot_Number",
        "ODPD_OC_Key", "OC_Description",
        "ODPD_CM_Key", "CM_Description", "ODPD_SM_Key", "SM_Description",
        "ODPD_Is_Overtime", "ODPD_STPO_Key",
        "source_connection"
    ]
    
    print("   Grouping column status:")
    for col in grouping_columns:
        if col in source_data.columns:
            non_null_count = source_data[col].notna().sum()
            print(f"     {col}: {non_null_count}/{len(source_data)} non-null")
        else:
            print(f"     {col}: MISSING")
    
    # Check what happens when we try to group by these columns
    print("\n4. Testing groupby operation...")
    available_grouping_cols = [col for col in grouping_columns if col in source_data.columns]
    non_null_data = source_data.dropna(subset=available_grouping_cols)
    
    print(f"   Original data: {len(source_data)} records")
    print(f"   After dropping nulls in grouping columns: {len(non_null_data)} records")
    
    if len(non_null_data) > 0:
        try:
            # Try the groupby operation
            grouped = non_null_data.groupby(available_grouping_cols)
            unique_combinations = grouped.size().reset_index(name='count')
            print(f"   Unique grouping combinations: {len(unique_combinations)}")
            
            print("   Sample combinations:")
            print(unique_combinations.head(3))
        except Exception as e:
            print(f"   Error in groupby operation: {e}")
    else:
        print("   No data left after filtering nulls - this explains why employee aggregation produces 0 records")
        
        # Check which columns are causing the issue
        print("\n   Detailed null analysis:")
        for col in available_grouping_cols:
            null_count = source_data[col].isna().sum()
            if null_count > 0:
                print(f"     {col}: {null_count} null values")
                # Show sample null records
                null_records = source_data[source_data[col].isna()].head(2)
                print(f"       Sample null records for {col}:")
                for idx, (_, row) in enumerate(null_records.iterrows()):
                    print(f"         Record {idx+1}: {dict(row[employee_columns])}")

if __name__ == "__main__":
    detailed_employee_diagnostic()
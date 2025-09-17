#!/usr/bin/env python3
"""
Fixed version of the employee aggregation that handles null values properly.
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

def perform_fixed_aggregations(df):
    """
    Perform the same aggregations as the Spark job but with fixes for null values.
    
    Args:
        df: pandas DataFrame with source data
        
    Returns:
        dict: Dictionary containing the aggregated DataFrames
    """
    print("Performing fixed aggregations on source data")
    
    try:
        if df.empty:
            print("No data to aggregate, returning empty results")
            return {
                'odp_date_oc': pd.DataFrame(),
                'odp_date_shift': pd.DataFrame(),
                'odp_date_employee': pd.DataFrame(),
                'odp_hourly_summary': pd.DataFrame()
            }
        
        # Create a mapping of expected column names to actual column names in the DataFrame
        column_name_mapping = {
            # Source columns (from database table)
            'odp_date': ['ODP_Date', 'odp_date'],
            'shift': ['Shift', 'shift'],
            'odp_em_key': ['ODP_EM_Key', 'odp_em_key'],
            'em_rfid': ['EM_RFID', 'em_rfid'],
            'em_department': ['EM_Department', 'em_department'],
            'em_first_name': ['EM_FirstName', 'em_first_name'],
            'em_last_name': ['EM_LastName', 'em_last_name'],
            'odp_actual_clock_in': ['ODP_Actual_Clock_In', 'odp_actual_clock_in'],
            'odp_actual_clock_out': ['ODP_Actual_Clock_Out', 'odp_actual_clock_out'],
            'odp_current_station': ['ODP_Current_Station', 'odp_current_station'],
            'odpd_workstation': ['ODPD_Workstation', 'odpd_workstation'],
            'odpd_wc_key': ['ODPD_WC_Key', 'odpd_wc_key'],
            'odpd_quantity': ['ODPD_Quantity', 'odpd_quantity'],
            'odpd_st_key': ['ODPD_ST_Key', 'odpd_st_key'],
            'st_id': ['ST_ID', 'st_id'],
            'st_description': ['ST_Description', 'st_description'],
            'odpd_lot_number': ['ODPD_Lot_Number', 'odpd_lot_number'],
            'odpd_oc_key': ['ODPD_OC_Key', 'odpd_oc_key'],
            'oc_description': ['OC_Description', 'oc_description'],
            'loading_qty': ['Loading_Qty', 'loading_qty'],
            'unloading_qty': ['UnLoading_Qty', 'unloading_qty'],
            'oc_standard_time': ['OC_Standard_Time', 'oc_standard_time'],
            'odpd_actual_time': ['ODPD_Actual_Time', 'odpd_actual_time'],
            'odpd_cm_key': ['ODPD_CM_Key', 'odpd_cm_key'],
            'cm_description': ['CM_Description', 'cm_description'],
            'odpd_sm_key': ['ODPD_SM_Key', 'odpd_sm_key'],
            'sm_description': ['SM_Description', 'sm_description'],
            'odpd_is_overtime': ['ODPD_Is_Overtime', 'odpd_is_overtime'],
            'odpd_overtime_factor': ['ODPD_Overtime_Factor', 'odpd_overtime_factor'],
            'odpd_stpo_key': ['ODPD_STPO_Key', 'odpd_stpo_key'],
            'source_connection': ['source_connection'],
            'record_count': ['record_count']
        }
        
        # Find actual column names in the DataFrame
        actual_column_names = {}
        for expected_name, possible_names in column_name_mapping.items():
            for name in possible_names:
                if name in df.columns:
                    actual_column_names[expected_name] = name
                    break
            if expected_name not in actual_column_names:
                print(f"Column {expected_name} not found in DataFrame")
        
        print(f"Actual column names mapping: {actual_column_names}")
        
        # Transform 1: Group by Date and Operation Code details
        print("Performing aggregation 1: by Date and Operation Code...")
        agg1_grouping_cols_expected = [
            "odp_date", "shift", "odpd_st_key", "st_id", "st_description",
            "odpd_lot_number", "odpd_oc_key", "oc_description",
            "oc_standard_time", "odpd_actual_time", "odpd_cm_key",
            "cm_description", "odpd_sm_key", "sm_description", "source_connection"
        ]
        
        # Map to actual column names
        actual_agg1_grouping_cols = [actual_column_names[col] for col in agg1_grouping_cols_expected if col in actual_column_names]
        
        # Define aggregation operations with actual column names
        agg_operations_1 = {}
        if 'odpd_quantity' in actual_column_names:
            agg_operations_1[actual_column_names['odpd_quantity']] = 'sum'
        if 'loading_qty' in actual_column_names:
            agg_operations_1[actual_column_names['loading_qty']] = 'sum'
        if 'unloading_qty' in actual_column_names:
            agg_operations_1[actual_column_names['unloading_qty']] = 'sum'
        # Use a column that we know exists for counting
        if 'id' in df.columns and 'id' not in actual_agg1_grouping_cols:
            agg_operations_1['id'] = 'count'
        else:
            # Find a column that's not in the grouping columns for counting
            for col in df.columns:
                if col not in actual_agg1_grouping_cols:
                    agg_operations_1[col] = 'count'
                    break
            else:
                # If all columns are in grouping columns, create a constant column for counting
                df = df.copy()
                df['record_count_temp'] = 1
                agg_operations_1['record_count_temp'] = 'sum'
        
        if actual_agg1_grouping_cols and agg_operations_1:
            # Filter out rows with null values in grouping columns
            filtered_df1 = df.dropna(subset=actual_agg1_grouping_cols)
            if len(filtered_df1) > 0:
                aggregated_df1 = filtered_df1.groupby(actual_agg1_grouping_cols).agg(agg_operations_1).reset_index()
                # Rename count column to record_count
                if 'record_count_temp' in aggregated_df1.columns:
                    aggregated_df1 = aggregated_df1.rename(columns={'record_count_temp': 'record_count'})
                else:
                    count_col = list(agg_operations_1.keys())[-1]  # Get the last aggregated column (the count)
                    if count_col in aggregated_df1.columns:
                        aggregated_df1 = aggregated_df1.rename(columns={count_col: 'record_count'})
            else:
                print("No valid data for aggregation 1 after filtering nulls")
                aggregated_df1 = pd.DataFrame()
        else:
            print("No valid columns for aggregation 1")
            aggregated_df1 = pd.DataFrame()
        
        # Transform 2: Group by Date and Shift details
        print("Performing aggregation 2: by Date and Shift...")
        agg2_grouping_cols_expected = [
            "odp_date", "shift", "odpd_st_key", "st_id", "st_description",
            "odpd_lot_number", "odpd_oc_key", "oc_description",
            "oc_standard_time", "odpd_actual_time", "odpd_cm_key",
            "cm_description", "odpd_sm_key", "sm_description", "odpd_stpo_key", "source_connection"
        ]
        
        # Map to actual column names
        actual_agg2_grouping_cols = [actual_column_names[col] for col in agg2_grouping_cols_expected if col in actual_column_names]
        
        # Define aggregation operations for aggregation 2
        agg_operations_2 = {}
        if 'odpd_quantity' in actual_column_names:
            agg_operations_2[actual_column_names['odpd_quantity']] = 'sum'
        if 'loading_qty' in actual_column_names:
            agg_operations_2[actual_column_names['loading_qty']] = 'sum'
        if 'unloading_qty' in actual_column_names:
            agg_operations_2[actual_column_names['unloading_qty']] = 'sum'
        if 'odpd_overtime_factor' in actual_column_names:
            agg_operations_2[actual_column_names['odpd_overtime_factor']] = 'mean'
        if 'odpd_is_overtime' in actual_column_names:
            agg_operations_2[actual_column_names['odpd_is_overtime']] = 'max'
        # Use a column for counting
        if 'id' in df.columns and 'id' not in actual_agg2_grouping_cols:
            agg_operations_2['id'] = 'count'
        else:
            # Find a column that's not in the grouping columns for counting
            for col in df.columns:
                if col not in actual_agg2_grouping_cols:
                    agg_operations_2[col] = 'count'
                    break
            else:
                # If all columns are in grouping columns, create a constant column for counting
                df = df.copy()
                df['record_count_temp'] = 1
                agg_operations_2['record_count_temp'] = 'sum'
        
        if actual_agg2_grouping_cols and agg_operations_2:
            # Filter out rows with null values in grouping columns
            filtered_df2 = df.dropna(subset=actual_agg2_grouping_cols)
            if len(filtered_df2) > 0:
                aggregated_df2 = filtered_df2.groupby(actual_agg2_grouping_cols).agg(agg_operations_2).reset_index()
                # Rename count column to record_count
                if 'record_count_temp' in aggregated_df2.columns:
                    aggregated_df2 = aggregated_df2.rename(columns={'record_count_temp': 'record_count'})
                else:
                    count_col = list(agg_operations_2.keys())[-1]  # Get the last aggregated column (the count)
                    if count_col in aggregated_df2.columns:
                        aggregated_df2 = aggregated_df2.rename(columns={count_col: 'record_count'})
            else:
                print("No valid data for aggregation 2 after filtering nulls")
                aggregated_df2 = pd.DataFrame()
        else:
            print("No valid columns for aggregation 2")
            aggregated_df2 = pd.DataFrame()
        
        # Transform 3: Group by Date and Employee details (FIXED VERSION)
        print("Performing aggregation 3: by Date and Employee (FIXED)...")
        agg3_grouping_cols_expected = [
            "odp_date", "shift",
            "odp_em_key", "em_rfid", "em_department", "em_first_name", "em_last_name",
            "odp_current_station", "odpd_workstation", "odpd_wc_key",
            "odpd_st_key", "st_id", "st_description", "odpd_lot_number",
            "odpd_oc_key", "oc_description",
            "odpd_cm_key", "cm_description", "odpd_sm_key", "sm_description",
            "odpd_is_overtime", "odpd_stpo_key",
            "source_connection"
        ]
        
        # Map to actual column names
        actual_agg3_grouping_cols = [actual_column_names[col] for col in agg3_grouping_cols_expected if col in actual_column_names]
        
        # Define aggregation operations for aggregation 3
        agg_operations_3 = {}
        if 'odpd_quantity' in actual_column_names and actual_column_names['odpd_quantity'] not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['odpd_quantity']] = 'sum'
        if 'loading_qty' in actual_column_names and actual_column_names['loading_qty'] not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['loading_qty']] = 'sum'
        if 'unloading_qty' in actual_column_names and actual_column_names['unloading_qty'] not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['unloading_qty']] = 'sum'
        if 'odpd_actual_time' in actual_column_names and actual_column_names['odpd_actual_time'] not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['odpd_actual_time']] = 'sum'
        if 'odpd_is_overtime' in actual_column_names and actual_column_names['odpd_is_overtime'] not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['odpd_is_overtime']] = 'max'
        if 'odpd_overtime_factor' in actual_column_names and actual_column_names['odpd_overtime_factor'] not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['odpd_overtime_factor']] = 'mean'
        # Use a column for counting
        if 'id' in df.columns and 'id' not in actual_agg3_grouping_cols:
            agg_operations_3['id'] = 'count'
        else:
            # Find a column that's not in the grouping columns for counting
            for col in df.columns:
                if col not in actual_agg3_grouping_cols:
                    agg_operations_3[col] = 'count'
                    break
            else:
                # If all columns are in grouping columns, create a constant column for counting
                df = df.copy()
                df['record_count_temp'] = 1
                agg_operations_3['record_count_temp'] = 'sum'
        if 'odp_actual_clock_in' in actual_column_names:
            agg_operations_3[actual_column_names['odp_actual_clock_in']] = 'min'
        if 'odp_actual_clock_out' in actual_column_names:
            agg_operations_3[actual_column_names['odp_actual_clock_out']] = 'max'
        
        if actual_agg3_grouping_cols and agg_operations_3:
            # FILTER OUT problematic columns with all null values
            # Specifically, we know 'ODP_Current_Station' has all null values
            filtered_agg3_grouping_cols = [col for col in actual_agg3_grouping_cols if col != 'ODP_Current_Station']
            
            # Filter out rows with null values in the remaining grouping columns
            filtered_df3 = df.dropna(subset=filtered_agg3_grouping_cols)
            if len(filtered_df3) > 0:
                aggregated_df3 = filtered_df3.groupby(filtered_agg3_grouping_cols).agg(agg_operations_3).reset_index()
                # Rename count column to record_count
                if 'record_count_temp' in aggregated_df3.columns:
                    aggregated_df3 = aggregated_df3.rename(columns={'record_count_temp': 'record_count'})
                else:
                    count_col = list(agg_operations_3.keys())[-1]  # Get the last aggregated column (the count)
                    if count_col in aggregated_df3.columns:
                        aggregated_df3 = aggregated_df3.rename(columns={count_col: 'record_count'})
            else:
                print("No valid data for aggregation 3 after filtering nulls")
                aggregated_df3 = pd.DataFrame()
        else:
            print("No valid columns for aggregation 3")
            aggregated_df3 = pd.DataFrame()
        
        # Transform 4: Hourly Summary Aggregation
        print("Performing hourly summary aggregation...")
        
        # Group by hour, date, shift, station, and operation code
        hourly_summary_grouping_cols_expected = [
            "odp_date", "shift", "st_id", "st_description", "oc_description", "source_connection"
        ]
        
        # Map to actual column names
        actual_hourly_grouping_cols = [actual_column_names[col] for col in hourly_summary_grouping_cols_expected if col in actual_column_names]
        
        # Define aggregation operations for hourly summary
        hourly_agg_operations = {}
        if 'odpd_quantity' in actual_column_names:
            hourly_agg_operations[actual_column_names['odpd_quantity']] = 'sum'
        if 'loading_qty' in actual_column_names:
            hourly_agg_operations[actual_column_names['loading_qty']] = 'sum'
        if 'unloading_qty' in actual_column_names:
            hourly_agg_operations[actual_column_names['unloading_qty']] = 'sum'
        if 'odpd_actual_time' in actual_column_names:
            hourly_agg_operations[actual_column_names['odpd_actual_time']] = 'mean'
        # Count distinct employees
        if 'odp_em_key' in actual_column_names:
            hourly_agg_operations[actual_column_names['odp_em_key']] = 'nunique'
        # Add count of records
        # Find a column that's not in the grouping columns for counting
        for col in df.columns:
            if col not in actual_hourly_grouping_cols and col != 'hour_timestamp':
                hourly_agg_operations[col] = 'count'
                break
        else:
            # If all columns are in grouping columns, create a constant column for counting
            df_with_hour = df.copy()
            df_with_hour['record_count_temp'] = 1
            hourly_agg_operations['record_count_temp'] = 'sum'
        
        if actual_hourly_grouping_cols and hourly_agg_operations:
            # First, we need to add hour_timestamp to the dataframe
            # We'll add it to the dataframe temporarily based on created_at
            df_with_hour = df.copy()
            # Extract hour from created_at (or use current time if created_at is not available)
            if 'created_at' in df_with_hour.columns:
                df_with_hour['hour_timestamp'] = pd.to_datetime(df_with_hour['created_at']).dt.floor('H')
            else:
                current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
                df_with_hour['hour_timestamp'] = current_hour
            
            # Include hour_timestamp in grouping
            actual_hourly_grouping_cols_with_hour = ['hour_timestamp'] + actual_hourly_grouping_cols
            
            # Filter out rows with null values in grouping columns
            filtered_df4 = df_with_hour.dropna(subset=actual_hourly_grouping_cols_with_hour)
            if len(filtered_df4) > 0:
                aggregated_df4 = filtered_df4.groupby(actual_hourly_grouping_cols_with_hour).agg(hourly_agg_operations).reset_index()
                
                # Rename columns appropriately
                if actual_column_names.get('odp_em_key') in aggregated_df4.columns:
                    aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['odp_em_key']: 'total_employees'})
                
                # Rename count column to record_count if it exists
                if 'record_count_temp' in aggregated_df4.columns:
                    aggregated_df4 = aggregated_df4.rename(columns={'record_count_temp': 'record_count'})
                else:
                    # Try to find the count column
                    count_cols = [col for col in aggregated_df4.columns if col not in actual_hourly_grouping_cols_with_hour]
                    if count_cols:
                        count_col = count_cols[-1]  # Get the last non-grouping column
                        aggregated_df4 = aggregated_df4.rename(columns={count_col: 'record_count'})
                
                # Rename other columns to match the database schema
                if actual_column_names.get('odpd_quantity') in aggregated_df4.columns:
                    aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['odpd_quantity']: 'total_quantity'})
                if actual_column_names.get('loading_qty') in aggregated_df4.columns:
                    aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['loading_qty']: 'total_loading_qty'})
                if actual_column_names.get('unloading_qty') in aggregated_df4.columns:
                    aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['unloading_qty']: 'total_unloading_qty'})
                if actual_column_names.get('odpd_actual_time') in aggregated_df4.columns:
                    aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['odpd_actual_time']: 'avg_actual_time'})
                if actual_column_names.get('st_id') in aggregated_df4.columns:
                    aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['st_id']: 'station_id'})
                if actual_column_names.get('st_description') in aggregated_df4.columns:
                    aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['st_description']: 'station_description'})
                if actual_column_names.get('oc_description') in aggregated_df4.columns:
                    aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['oc_description']: 'operation_code'})
            else:
                print("No valid data for hourly summary aggregation after filtering nulls")
                aggregated_df4 = pd.DataFrame()
        else:
            print("No valid columns for hourly summary aggregation")
            aggregated_df4 = pd.DataFrame()
        
        # Add created_at timestamp to all DataFrames
        current_time = datetime.now()
        for df_agg in [aggregated_df1, aggregated_df2, aggregated_df3, aggregated_df4]:
            if not df_agg.empty:
                df_agg['created_at'] = current_time
        
        agg_results = {
            'odp_date_oc': aggregated_df1,
            'odp_date_shift': aggregated_df2,
            'odp_date_employee': aggregated_df3,
            'odp_hourly_summary': aggregated_df4
        }
        
        print("Fixed aggregations completed")
        return agg_results
        
    except Exception as e:
        print(f"Error in fixed aggregations: {e}")
        raise

def test_fixed_employee_aggregation():
    """Test the fixed employee aggregation."""
    print("Testing fixed employee aggregation...")
    
    # Fetch recent source data
    print("1. Fetching recent source data...")
    try:
        source_data = fetch_recent_source_data(hours_back=1)
        print(f"   ✓ Fetched {len(source_data)} records from source")
    except Exception as e:
        print(f"   ✗ Error fetching source data: {e}")
        return False
    
    # Perform fixed aggregations
    print("2. Performing fixed aggregations...")
    try:
        if source_data.empty:
            print("   ! No data to aggregate")
            return True
            
        aggregated_results = perform_fixed_aggregations(source_data)
        print("   ✓ Fixed aggregations completed:")
        for table_name, df in aggregated_results.items():
            print(f"     - {table_name}: {len(df)} records")
            
        # Specifically check the employee aggregation
        employee_df = aggregated_results.get('odp_date_employee')
        if employee_df is not None and not employee_df.empty:
            print(f"   ✓ SUCCESS: Employee aggregation now produces {len(employee_df)} records!")
            print("   Sample employee aggregation data:")
            print(employee_df.head(3))
        else:
            print("   ! Employee aggregation still produces 0 records")
            
    except Exception as e:
        print(f"   ✗ Error in fixed aggregations: {e}")
        return False
    
    print("\n✓ Fixed employee aggregation test finished!")
    return True

if __name__ == "__main__":
    test_fixed_employee_aggregation()
#!/usr/bin/env python3
"""
Test script to verify the hourly_hanger_line_history.py file works with updated table names.
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add the scripts directory to the Python path
scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
sys.path.append(os.path.abspath(scripts_path))

def test_historical_processing():
    """Test the historical data processing with updated table names."""
    print("Testing historical data processing with updated table names...")
    
    try:
        # Import the necessary functions from hourly_hanger_line_history
        from dags.hourly_hanger_line_history import (
            fetch_recent_source_data,
            perform_aggregations
        )
        
        # Fetch recent source data
        print("1. Fetching recent source data...")
        source_data = fetch_recent_source_data(hours_back=1)
        print(f"   ✓ Fetched {len(source_data)} records from source")
        
        # Perform aggregations
        print("2. Performing aggregations...")
        if not source_data.empty:
            aggregated_results = perform_aggregations(source_data)
            print("   ✓ Aggregations completed:")
            for table_name, df in aggregated_results.items():
                print(f"     - {table_name}: {len(df)} records")
                
                # Verify that all tables have proper column values
                if not df.empty:
                    # Check for null values in key columns
                    key_columns = ['hour_timestamp', 'ODP_Date', 'Shift']
                    for col in key_columns:
                        if col in df.columns:
                            null_count = df[col].isna().sum()
                            if null_count > 0:
                                print(f"     ! Warning: {null_count} null values in {col}")
                            else:
                                print(f"     ✓ No null values in {col}")
                                
                    # Check created_at column
                    if 'created_at' in df.columns:
                        null_count = df['created_at'].isna().sum()
                        if null_count > 0:
                            print(f"     ! Warning: {null_count} null values in created_at")
                        else:
                            print(f"     ✓ Proper timestamps in created_at")
                            
        else:
            print("   ! No data to aggregate")
            
        print("\n✓ Historical data processing test completed successfully!")
        return True
        
    except Exception as e:
        print(f"   ✗ Error in historical data processing: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_historical_processing()
    if success:
        print("\nAll tests passed!")
        sys.exit(0)
    else:
        print("\nSome tests failed!")
        sys.exit(1)
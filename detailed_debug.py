
"""
Detailed debug script for hanger_lane DAG
"""

import sys
import os
from datetime import datetime

# Add the dags directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'dags'))

def detailed_debug():
    """Detailed debugging of the hanger_lane DAG"""
    print("=== DETAILED DEBUG ===")
    
    # 1. Check source constants
    try:
        from scripts.constans.db_sources import SOURCE_HANGER_LANE
        print(f"SOURCE_HANGER_LANE: {SOURCE_HANGER_LANE}")
    except Exception as e:
        print(f"Error importing SOURCE_HANGER_LANE: {e}")
        return
    
    # 2. For each source, check the logic
    for connection_id in SOURCE_HANGER_LANE:
        print(f"\n--- Debugging {connection_id} ---")
        
        # Simulate check_for_new_data logic
        try:
            # Try to get last extract datetime
            try:
                from dags.hanger_lane_fixed import get_last_extract_dt_from_log
                last_extract_dt = get_last_extract_dt_from_log(connection_id)
                print(f"  Last extract datetime: {last_extract_dt}")
            except Exception as e:
                print(f"  Error getting last extract datetime: {e}")
                last_extract_dt = None
            
            # Decision logic
            if last_extract_dt:
                print(f"  Found last extract: {last_extract_dt}")
                print("  Would check database for new records...")
                print("  Decision depends on database query result")
            else:
                print("  No previous extract date found")
                print("  DECISION: SAVE PATH (No previous extract)")
                print("  Expected return: True")
                
        except Exception as e:
            print(f"  Error in check_for_new_data logic: {e}")

if __name__ == "__main__":
    detailed_debug()

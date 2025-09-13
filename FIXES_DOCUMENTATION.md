# Issues Fixed in hanger_lane_fixed.py

## 1. SQL Query Syntax Errors
- **Problem**: Extra commas in the SELECT clause were causing SQL syntax errors
- **Fix**: Removed extra commas in the SELECT statement:
  - Removed comma after `[ODP_Last_Hanger_Start_Time]`
  - Removed comma after `[ODPD_Key]`
  - Removed comma after `[ODPD_Quantity]`
  - Removed comma after `[ODPD_OC_Key]`
  - Removed comma after `[UnLoading_Qty]`
  - Removed comma after `[ODPD_Actual_Time]`
  - Removed comma after `[ODPD_Start_Time]`
  - Removed comma after `[ODPD_Overtime_Factor]`
  - Removed comma after `[ODPD_Actual_Time_From_Reader]`

## 2. Import Path Issues
- **Problem**: Incorrect import paths for modules
- **Fix**: Corrected import paths:
  - Changed `from scripts.constans.db_sources import SOURCE_HANGER_LANE` to proper path
  - Changed `from create_target_pg_hl_table import (...)` to `from scripts.create_target_pg_hl_table import (...)`

## 3. Logging Enhancement
- **Problem**: No logging when no data was processed
- **Fix**: Added logging for "Completed - No new data" scenario to ensure the ETL process is properly logged even when no new data is found

## 4. DAG ID Change
- **Problem**: Same DAG ID as optimized version could cause conflicts
- **Fix**: Changed DAG ID to `etl_hanger_lines_dynamic_fixed` to avoid conflicts

## 5. Source Data Constants
- **Problem**: Using different source constants than the original
- **Fix**: Using the correct `SOURCE_HANGER_LANE` constant that includes all lines

These fixes should resolve the data extraction issues in the optimized version.
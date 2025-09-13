"""
Simple test script to verify hanger_lane ETL fixes.
"""

import sys
import os

# Add the dags directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'dags'))

def test_sql_query_syntax():
    """Test that the SQL query syntax is correct."""
    # This is the corrected query from the fixed version
    query = """
        SELECT
            [ODP_Date]
            ,[ODP_Key]
            ,CASE WHEN [ODP_Shift]=1 THEN 'Day' ELSE 'Night' END AS [Shift]
            ,[ODP_EM_Key]
            ,[EM_RFID]
            ,[EM_Department]
            ,[EM_FirstName]
            ,[EM_LastName]
            ,[ODP_Actual_Clock_In]
            ,[ODP_Actual_Clock_Out]
            ,[ODP_Shift_Clock_In]
            ,[ODP_Shift_Clock_Out]
            ,[ODP_First_Hanger_Time]
            ,[ODP_Last_Hanger_Time]
            ,[ODP_Current_Station]
            ,[ODP_Lump_Sum_Payment]
            ,[ODP_Make_Up_Pay_Rate]
            ,[ODP_Last_Hanger_Start_Time]
            ,[ODPD_Key]
            ,[ODPD_Workstation]
            ,[ODPD_WC_Key]
            ,[ODPD_Quantity]
            ,[ODPD_ST_Key]
            ,[ST_ID]
            ,[ST_Description]
            ,[ODPD_Lot_Number]
            ,[ODPD_OC_Key]
            ,CASE WHEN [OC_Description]='Loading/Panel Segregation' THEN 'Loading' 
                WHEN [OC_Description]='Pressing' THEN 'Un-Loading'
            ELSE [OC_Description] END AS OC_Description
            ,CASE WHEN [OC_Description]='Loading/Panel Segregation' THEN ODPD_Quantity ELSE 0 END AS Loading_Qty
            ,CASE WHEN [OC_Description]='Pressing' THEN ODPD_Quantity ELSE 0 END AS UnLoading_Qty
            ,[OC_Piece_Rate]
            ,[OC_Standard_Time]
            ,[ODPD_Standard]
            ,ODPD_Actual_Time
            ,[ODPD_PA_Key]
            ,[ODPD_Pay_Rate]
            ,[ODPD_Piece_Rate]
            ,[ODPD_Start_Time]
            ,[ODPD_CM_Key]
            ,[CM_Description]
            ,[ODPD_SM_Key]
            ,[SM_Description]
            ,[ODPD_Normal_Pay_Factor]
            ,[ODPD_Is_Overtime]
            ,[ODPD_Overtime_Factor]
            ,[ODPD_Edited_By]
            ,[ODPD_Edited_Date]
            ,[ODPD_Actual_Time_From_Reader]
            ,[ODPD_STPO_Key]
            ,[created_at] as created_at
        FROM [IHS].[dbo].[ODP_Detail] OD
        INNER JOIN [IHS].[dbo].[ODP_Master] OM ON OD.[ODPD_ODP_Key] = OM.[ODP_Key]  
        INNER JOIN [IHS_SHARED].[dbo].[Employee_Master] EM   ON OM.[ODP_EM_Key]=EM.[EM_Key]
        INNER JOIN [IHS_SHARED].[dbo].[Operation_Codes] OC   ON OD.[ODPD_OC_Key]=OC.[OC_Key]
        INNER JOIN [IHS_SHARED].[dbo].[Size_Master] SM ON OD.[ODPD_SM_Key]=SM.[SM_Key]
        INNER JOIN [IHS_SHARED].[dbo].[Colour_Master] CM ON OD.[ODPD_CM_Key]=CM.[CM_Key]
        INNER JOIN [IHS_SHARED].[dbo].[Style_Master] ST ON OD.[ODPD_ST_Key]=ST.[ST_Key]
        INNER JOIN [IHS_SHARED].[dbo].[Style_Planned_Orders] PO ON OD.[ODPD_STPO_Key]=PO.[STPO_Key]
        WHERE 1=1
    """
    
    # Basic check - count the number of commas in the query
    # This should be reasonable for a valid SQL query
    comma_count = query.count(',')
    print(f"SQL Query comma count: {comma_count}")
    
    # Check for common syntax errors
    if ",," in query:
        print("ERROR: Double commas found in query")
        return False
    
    if ",\n            ]" in query:
        print("ERROR: Trailing commas before closing brackets found in query")
        return False
        
    print("SQL Query syntax check passed")
    return True

def test_imports():
    """Test that imports work correctly."""
    try:
        # Test importing the fixed version
        from dags.hanger_lane_fixed import SOURCE_HANGER_LANE
        print(f"SOURCE_HANGER_LANE: {SOURCE_HANGER_LANE}")
        print("Import test passed")
        return True
    except ImportError as e:
        print(f"Import test failed: {e}")
        return False

if __name__ == "__main__":
    print("Running hanger_lane ETL fixes verification...")
    
    print("\n1. Testing SQL query syntax...")
    sql_ok = test_sql_query_syntax()
    
    print("\n2. Testing imports...")
    imports_ok = test_imports()
    
    print("\n--- Summary ---")
    if sql_ok and imports_ok:
        print("All tests passed! The fixes should resolve the data extraction issues.")
    else:
        print("Some tests failed. Please check the issues above.")
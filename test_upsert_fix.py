import sys
import os
from datetime import date

# Add the dags directory to the Python path
dags_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dags')
sys.path.append(dags_path)

from dags.upsert_utils import (
    upsert_data_via_postgres,
    create_connection_params_from_airflow
)

def test_upsert():
    # Test data with correct column names (uppercase)
    test_data = [
        {
            'ODP_Date': date.today(),
            'Shift': 'A',
            'ODPD_ST_Key': 1,
            'ST_ID': 'ST001',
            'ST_Description': 'Test Station',
            'ODPD_Lot_Number': 'LOT001',
            'ODPD_OC_Key': 1,
            'OC_Description': 'Test Operation',
            'OC_Standard_Time': 10.5,
            'ODPD_Actual_Time': 9.8,
            'ODPD_CM_Key': 1,
            'CM_Description': 'Test Component',
            'ODPD_SM_Key': 1,
            'SM_Description': 'Test Material',
            'source_connection': 'test_conn',
            'ODPD_Quantity': 100,
            'Loading_Qty': 50,
            'UnLoading_Qty': 50,
            'record_count': 1,
            'created_at': '2025-09-16 10:00:00'
        }
    ]
    
    # Key columns (using correct column names)
    key_columns = [
        'ODP_Date', 'Shift', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 'ODPD_Lot_Number',
        'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 'CM_Description', 
        'ODPD_SM_Key', 'SM_Description', 'source_connection'
    ]
    
    # Connection parameters (using values from the environment)
    connection_params = {
        "host": "172.16.7.6",
        "port": "5432",
        "database": "ssg",
        "user": "postgres",
        "password": "P@kistan12"
    }
    
    # Test upsert to odp_date_oc table
    success = upsert_data_via_postgres(test_data, 'odp_date_oc', key_columns, connection_params)
    
    if success:
        print("Test upsert successful!")
    else:
        print("Test upsert failed!")

if __name__ == "__main__":
    test_upsert()
#!/usr/bin/env python3
"""
Script to create the general_ledger_transactions table in PostgreSQL
"""
import os
import sys
# from sqlalchemy import create_engine, Column, Integer, String, DateTime, Numeric, Text, Date, Float
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker

# Add the scripts directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import the model from the DAG
from dags.mssql_gl_to_pg_etl import GeneralLedgerTransaction, get_postgres_engine

def create_table():
    """Create the general_ledger_transactions table in PostgreSQL"""
    try:
        # Get the PostgreSQL engine
        engine = get_postgres_engine()
        
        # Create the table
        GeneralLedgerTransaction.metadata.create_all(engine)
        
        print("Table 'general_ledger_transactions' created successfully!")
        return True
    except Exception as e:
        print(f"Error creating table: {e}")
        return False

# if __name__ == "__main__":
#     create_table()
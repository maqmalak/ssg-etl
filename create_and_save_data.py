"""
Standalone script to demonstrate creating a table and saving data using the get_postgres_engine function.
This script shows how to use the database utility functions from db_utils.py to work with PostgreSQL.
"""

import sys
import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Numeric
from sqlalchemy.orm import declarative_base, sessionmaker

# Add the dags directory to the Python path so we can import db_utils
sys.path.append(os.path.join(os.path.dirname(__file__), 'dags'))

from db_utils import get_postgres_engine

# Define a sample table model
Base = declarative_base()

class SampleData(Base):
    __tablename__ = 'sample_data_table'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100))
    value = Column(Numeric(10, 2))
    created_at = Column(DateTime, default=datetime.utcnow)

    
engine = get_postgres_engine()

def create_table_if_not_exists(engine):
    """Create the sample data table if it doesn't exist"""
    print("Creating table if it doesn't exist...")
    Base.metadata.create_all(engine)
    print("Table creation completed.")

def save_sample_data():
    """Save sample data to the database"""
    try:
        print("Getting database engine...")
        # Get database engine
        engine = get_postgres_engine()
        
        # Create table if it doesn't exist
        create_table_if_not_exists(engine)
        
        # Create a session
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Create sample data records
        sample_records = [
            SampleData(name="Record 1", value=100.50),
            SampleData(name="Record 2", value=200.75),
            SampleData(name="Record 3", value=300.25),
        ]
        
        print(f"Adding {len(sample_records)} sample records to the database...")
        
        # Add records to the session and commit
        session.add_all(sample_records)
        session.commit()
        
        print(f"Successfully saved {len(sample_records)} records to the database")
        
        # Query the data to verify it was saved
        print("Querying saved records...")
        saved_records = session.query(SampleData).all()
        print(f"Total records in table: {len(saved_records)}")
        for record in saved_records:
            print(f"ID: {record.id}, Name: {record.name}, Value: {record.value}, Created: {record.created_at}")
        
        # Close the session
        session.close()
        
        return True
        
    except Exception as e:
        print(f"Error saving data: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if 'engine' in locals():
            engine.dispose()

def test_db_connection():
    """Test if we can connect to the database"""
    try:
        print("Testing database connection...")
        engine = get_postgres_engine()
        # Try to connect
        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            print("Database connection successful!")
            return True
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    print("Creating table and saving sample data...")
    print("=" * 50)
    
    # First test the connection
    if not test_db_connection():
        print("Cannot connect to database. Please ensure:")
        print("1. The PostgreSQL database is running")
        print("2. The database connection parameters are correct")
        print("3. You're running this script in the correct environment")
        print("If running locally, you might need to set the environment variables:")
        print("  export POSTGRES_HOST=localhost")
        print("  export POSTGRES_PORT=5432")
        print("  export POSTGRES_DB=your_database_name")
        print("  export POSTGRES_USER=your_username")
        print("  export POSTGRES_PASSWORD=your_password")
        sys.exit(1)
    
    success = save_sample_data()
    if success:
        print("Data saved successfully!")
        sys.exit(0)
    else:
        print("Failed to save data!")
        sys.exit(1)
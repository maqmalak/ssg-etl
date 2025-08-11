import time
import logging
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Text, Unicode, UnicodeText, VARCHAR
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
from airflow.models import Variable
# from scripts.exports.constants import POSTGRES_LOCAL_CONN_ID, S3_BUCKET, EXPORTS_LOCAL_PARQUET_FILE_S3_KEY
from scripts.constants import POSTGRES_LOCAL_CONN_ID
from scripts.utils.utils import read_sql_file

Base = declarative_base()
logger = logging.getLogger("airflow.task")


class BaseModel(Base):
    __abstract__ = True

    def __init__(self, connection_id = None, connection_type = 'postgres', **kwargs):
        super().__init__(**kwargs)
        self.connection = None
        self.engine = None
        self.Session = None
        self.Base = None
        self.sql_connection_id = connection_id
        self.connection_type = 'connection_type'

    def set_sql_connection_id(self, sql_connection_id):
        self.sql_connection_id = sql_connection_id

    def set_connection_type(self, connection_type):
        self.connection_type = connection_type
        
    def get_table_name(self):
        if hasattr(self, '__tablename__'):
            return self.__tablename__
        else:
            raise AttributeError("Table name not set. Please set the table name using set_table_name method.")
        
    def create_table_if_not_exists(engine):
        """Create the transactions table if it doesn't exist"""
        Base.metadata.create_all(engine)

    def get_db_session(self):
        max_retries = 5
        retry_delay = 5  # initial delay in seconds
        for attempt in range(max_retries):
            try:
                if not self.sql_connection_id:
                    self.sql_connection_id = 'postgres_grafana'

                self.connection = BaseHook.get_connection(self.sql_connection_id)
                uri = self.connection.get_uri()
                # Only replace the scheme (protocol) part of the URI
                if uri.startswith('postgres://'):
                    uri = 'postgresql://' + uri[len('postgres://'):]
                self.engine = create_engine(uri)
                self.Session = sessionmaker(bind=self.engine)()
                self.Base = declarative_base()
                self.Base.metadata.create_all(self.engine)
                return self.Session
            except OperationalError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"OperationalError: {e}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # exponential backoff
                else:
                    logger.error(f"Failed to connect after {max_retries} attempts. Error: {e}")
                    raise

    def create_table_from_sql_file(self, table_name, sql_file_path):
        sql_statements = read_sql_file(sql_file_path)
        print(f"Dropping table if exists {table_name}")
        self.get_db_session()
        for i, sql_statement in enumerate(sql_statements):
            print(sql_statement)
            print(f"Creating table {table_name} - {i + 1} of {len(sql_statements)}")
            create_table_sql_statement = sql_statement.replace(self.__tablename__, table_name)
            self.engine.execute(create_table_sql_statement)

    def create_child_table(self, table_name):
        sql_file_path = os.path.abspath(
            os.path.join('scripts', 'database', 'schema', f'{self.__tablename__}.sql'))
        sql_statements = read_sql_file(sql_file_path)
        print(f"Dropping table if exists {table_name}")
        self.get_db_session()
        self.engine.execute(f'DROP TABLE IF EXISTS {table_name} CASCADE')

        for i, sql_statement in enumerate(sql_statements):            
            print(f"Creating table {table_name} - {i + 1} of {len(sql_statements)}")
            create_table_sql_statement = sql_statement.replace(self.__tablename__, table_name)
            self.engine.execute(create_table_sql_statement)

    def set_table_name(self, table_name):
        # Set the new table name
        self.__tablename__ = table_name
        # Update the table name in the model's metadata
        if hasattr(self.__class__, '__table__'):
            self.__class__.__table__.name = table_name
        # If the table hasn't been created yet, we don't need to do anything else
        else:
            self.__class__.__tablename__ = table_name

    def get_model_columns(self):
        """Get all columns for a specific model"""
        return [column for column in self.__table__.columns]
    
    def execute_sql(self, sql_statement):
        """Execute a raw SQL statement"""
        session = self.get_db_session()
        try:
            result = session.execute(sql_statement)
            session.commit()
            return result
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_query(self):
        """Execute a query and return the results"""
        session = self.get_db_session()
        try:
            query = session.query(self)
            return query
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def bulk_insert(self, data):
        session = self.get_db_session()
        try:
            session.bulk_insert_mappings(self.__class__, data)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def close_session(self):
        if self.Session:
            self.Session.close()

        if self.engine:
            self.engine.dispose()

        print('Session closed and engine disposed')

    def __del__(self):
        self.close_session()

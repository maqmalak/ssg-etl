from sqlalchemy import Column, Integer, String, DateTime, Numeric, Text, Date, Float, Boolean, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData
from datetime import datetime

# Define the target table model
Base = declarative_base()


class OdpDailyOc(Base):
    __tablename__ = 'odp_daily_oc'
    
    # Define columns
    odp_date = Column('ODP_Date', Date)
    shift = Column('Shift', String(10))
    odpd_st_key = Column('ODPD_ST_Key', Integer)
    st_id = Column('ST_ID', String(50))
    st_description = Column('ST_Description', String(100))
    odpd_lot_number = Column('ODPD_Lot_Number', String(50))
    odpd_oc_key = Column('ODPD_OC_Key', Integer)
    oc_description = Column('OC_Description', String(100))
    oc_standard_time = Column('OC_Standard_Time', Numeric(10, 2))
    odpd_actual_time = Column('ODPD_Actual_Time', Numeric(10, 2))
    odpd_cm_key = Column('ODPD_CM_Key', Integer)
    cm_description = Column('CM_Description', String(100))
    odpd_sm_key = Column('ODPD_SM_Key', Integer)
    sm_description = Column('SM_Description', String(100))
    source_connection = Column('source_connection', String(50))
    odpd_quantity = Column('ODPD_Quantity', Integer)
    loading_qty = Column('Loading_Qty', Integer)
    unloading_qty = Column('UnLoading_Qty', Integer)
    record_count = Column('record_count', Integer)
    created_at = Column('created_at', DateTime, default=datetime.utcnow)
    
    # Composite primary key - using actual database column names
    __table_args__ = (
        PrimaryKeyConstraint('ODP_Date', 'Shift', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 'ODPD_Lot_Number',
                             'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 
                             'CM_Description', 'ODPD_SM_Key', 'SM_Description', 'source_connection'),
    )


class OdpDailyShift(Base):
    __tablename__ = 'odp_daily_shift'
    
    # Define columns
    odp_date = Column('ODP_Date', Date)
    shift = Column('Shift', String(10))
    odpd_quantity = Column('ODPD_Quantity', Integer)
    odpd_st_key = Column('ODPD_ST_Key', Integer)
    st_id = Column('ST_ID', String(50))
    st_description = Column('ST_Description', String(100))
    odpd_lot_number = Column('ODPD_Lot_Number', String(50))
    odpd_oc_key = Column('ODPD_OC_Key', Integer)
    oc_description = Column('OC_Description', String(100))
    loading_qty = Column('Loading_Qty', Integer)
    unloading_qty = Column('UnLoading_Qty', Integer)
    oc_standard_time = Column('OC_Standard_Time', Numeric(10, 2))
    odpd_actual_time = Column('ODPD_Actual_Time', Numeric(10, 2))
    odpd_cm_key = Column('ODPD_CM_Key', Integer)
    cm_description = Column('CM_Description', String(100))
    odpd_sm_key = Column('ODPD_SM_Key', Integer)
    sm_description = Column('SM_Description', String(100))
    odpd_is_overtime = Column('ODPD_Is_Overtime', Boolean)
    odpd_overtime_factor = Column('ODPD_Overtime_Factor', Float)
    odpd_stpo_key = Column('ODPD_STPO_Key', Integer)
    source_connection = Column('source_connection', String(50))
    record_count = Column('record_count', Integer)
    created_at = Column('created_at', DateTime, default=datetime.utcnow)
    
    # Composite primary key - using actual database column names
    __table_args__ = (
        PrimaryKeyConstraint('ODP_Date', 'Shift', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 
                             'ODPD_Lot_Number', 'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 
                             'CM_Description', 'ODPD_SM_Key', 'SM_Description', 'ODPD_Is_Overtime', 
                             'ODPD_STPO_Key', 'source_connection'),
    )


class OdpDailyEmployee(Base):
    __tablename__ = 'odp_daily_employee'

    # Define columns
    odp_date = Column('ODP_Date', Date)
    shift = Column('Shift', String(10))
    odp_em_key = Column('ODP_EM_Key', Integer)
    em_dept_first_last_rfid = Column('EM_Department', String(50))
    em_first_name = Column('EM_FirstName', String(50))
    odpd_workstation = Column('ODPD_Workstation', String(50))
    odpd_wc_key = Column('ODPD_WC_Key', Integer)
    odpd_st_key = Column('ODPD_ST_Key', Integer)
    st_id = Column('ST_ID', String(50))
    st_description = Column('ST_Description', String(100))
    odpd_lot_number = Column('ODPD_Lot_Number', String(50))
    odpd_oc_key = Column('ODPD_OC_Key', Integer)
    oc_description = Column('OC_Description', String(100))
    loading_qty = Column('Loading_Qty', Integer)
    unloading_qty = Column('UnLoading_Qty', Integer)
    oc_standard_time = Column('OC_Standard_Time', Numeric(10, 2))
    odpd_actual_time = Column('ODPD_Actual_Time', Numeric(10, 2))
    odpd_cm_key = Column('ODPD_CM_Key', Integer)
    cm_description = Column('CM_Description', String(100))
    odpd_sm_key = Column('ODPD_SM_Key', Integer)
    sm_description = Column('SM_Description', String(100))
    odpd_is_overtime = Column('ODPD_Is_Overtime', Boolean)
    odpd_overtime_factor = Column('ODPD_Overtime_Factor', Float)
    odpd_stpo_key = Column('ODPD_STPO_Key', Integer)
    source_connection = Column('source_connection', String(50))
    record_count = Column('record_count', Integer)
    created_at = Column('created_at', DateTime, default=datetime.utcnow)
    
    # Composite primary key - using actual database column names
    __table_args__ = (
        PrimaryKeyConstraint('ODP_Date', 'Shift', 'ODP_EM_Key',
                                'ODP_Current_Station', 'ODPD_Workstation', 'ODPD_WC_Key', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 
                                'ODPD_Lot_Number', 'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 
                                'CM_Description', 'ODPD_SM_Key', 'SM_Description', 'ODPD_Is_Overtime', 
                                'ODPD_STPO_Key', 'source_connection'),
    )


class OdpDailySummary(Base):
    __tablename__ = 'odp_daily_summary'
    
    # Define columns
    odp_date = Column('ODP_Date', Date)
    shift = Column('Shift', String(10))
    station_id = Column('station_id', String(50))
    station_description = Column('station_description', String(100))
    operation_code = Column('operation_code', String(100))
    total_quantity = Column('total_quantity', Integer)
    total_loading_qty = Column('total_loading_qty', Integer)
    total_unloading_qty = Column('total_unloading_qty', Integer)
    avg_actual_time = Column('avg_actual_time', Numeric(10, 2))
    total_employees = Column('total_employees', Integer)
    source_connection = Column('source_connection', String(50))
    record_count = Column('record_count', Integer)
    created_at = Column('created_at', DateTime, default=datetime.utcnow)
    
    # Composite primary key
    __table_args__ = (
        PrimaryKeyConstraint('ODP_Date', 'Shift', 'station_id', 'station_description', 
                             'operation_code', 'source_connection'),
    )



def create_etl_log_odp_table_if_not_exists(engine):
    """Create ETL aggregated OPD_Date base data log table if it doesn't exist."""
    meta = MetaData()
    log_table = Table(
        'etl_extract_daily_log', meta,
        Column('extractlogid', Integer, primary_key=True, autoincrement=True),
        Column('processlogid', String(100)),
        Column('source_connection', String(255)),
        Column('saved_count', Integer),
        Column('starttime', DateTime),
        Column('endtime', DateTime),
        Column('OPD_Date', Date),
        Column('min_created_at', DateTime),
        Column('max_created_at', DateTime),
        Column('lastextractdatetime', DateTime),
        Column('success', Boolean),
        Column('status', String(50)),
        Column('errormessage', Text),
        extend_existing=True
    )
    meta.create_all(engine)  # Will only create if not exists
    return log_table

def create_target_table_if_not_exists(engine):
    """Create the transactions table if it doesn't exist"""
    Base.metadata.create_all(engine)
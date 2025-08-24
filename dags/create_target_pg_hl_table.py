from sqlalchemy import Column, Integer, String, DateTime, Numeric, Text, Date, Float, Boolean, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData

# Define the target table model
Base = declarative_base()

class HangerLaneData(Base):
    __tablename__ = 'operator_daily_performance'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ODP_Key = Column(String(50))
    ODP_Date = Column(Date)
    Shift = Column(String(10))
    ODP_EM_Key = Column(Integer)
    EM_RFID = Column(String(50))
    EM_Department = Column(String(50))
    EM_FirstName = Column(String(50))
    EM_LastName = Column(String(50))
    ODP_Actual_Clock_In = Column(DateTime)
    ODP_Actual_Clock_Out = Column(DateTime)
    ODP_Shift_Clock_In = Column(DateTime)
    ODP_Shift_Clock_Out = Column(DateTime)
    ODP_First_Hanger_Time = Column(DateTime)
    ODP_Last_Hanger_Time = Column(DateTime)
    ODP_Current_Station = Column(String(50))
    ODP_Lump_Sum_Payment = Column(Numeric(10, 2))
    ODP_Make_Up_Pay_Rate = Column(Numeric(10, 2))
    ODP_Last_Hanger_Start_Time = Column(DateTime)
    ODPD_Key = Column(String(50))
    ODPD_Workstation = Column(String(50))
    ODPD_WC_Key = Column(Integer)
    ODPD_Quantity = Column(Integer)
    ODPD_ST_Key = Column(Integer)
    ST_ID = Column(String(50))
    ST_Description = Column(String(100))
    ODPD_Lot_Number = Column(String(50))
    ODPD_OC_Key = Column(Integer)
    OC_Description = Column(String(100))
    Loading_Qty = Column(Integer)
    UnLoading_Qty = Column(Integer)
    OC_Piece_Rate = Column(Numeric(10, 2))
    OC_Standard_Time = Column(Numeric(10, 2))
    ODPD_Standard = Column(Numeric(10, 2))
    ODPD_Actual_Time = Column(Numeric(10, 2))
    ODPD_PA_Key = Column(Integer)
    ODPD_Pay_Rate = Column(Numeric(10, 2))
    ODPD_Piece_Rate = Column(Numeric(10, 2))
    ODPD_Start_Time = Column(DateTime)
    ODPD_CM_Key = Column(Integer)
    CM_Description = Column(String(100))
    ODPD_SM_Key = Column(Integer)
    SM_Description = Column(String(100))
    ODPD_Normal_Pay_Factor = Column(Float)
    ODPD_Is_Overtime = Column(Boolean)
    ODPD_Overtime_Factor = Column(Float)
    ODPD_Edited_By = Column(String(50))
    ODPD_Edited_Date = Column(DateTime)
    ODPD_Actual_Time_From_Reader = Column(Numeric(10, 2))
    ODPD_STPO_Key = Column(Integer)
    created_at = Column(DateTime)
    source_connection = Column(String(50))


class OpdDateShift(Base):
    __tablename__ = 'opd_date_shift'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ODP_Date = Column(Date, nullable=False)
    Shift = Column(String(10), nullable=False)
    ODPD_Quantity = Column(Integer)
    source_connection = Column(String(50), nullable=False)
    
    # Composite primary key
    __table_args__ = (
        PrimaryKeyConstraint('ODP_Date', 'Shift', 'source_connection'),
    )


class OpdDateOc(Base):
    __tablename__ = 'opd_date_oc'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ODP_Date = Column(Date, nullable=False)
    OC_Description = Column(String(100), nullable=False)
    ODPD_Quantity = Column(Integer)
    source_connection = Column(String(50), nullable=False)
    
    # Composite primary key
    __table_args__ = (
        PrimaryKeyConstraint('ODP_Date', 'OC_Description', 'source_connection'),
    )


class OpdDateEmployee(Base):
    __tablename__ = 'opd_date_employee'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ODP_Date = Column(Date, nullable=False)
    ODP_EM_Key = Column(Integer, nullable=False)
    EM_RFID = Column(String(50))
    EM_Department = Column(String(50))
    EM_FirstName = Column(String(50))
    EM_LastName = Column(String(50))
    ODPD_Quantity = Column(Integer)
    source_connection = Column(String(50), nullable=False)
    
    # Composite primary key
    __table_args__ = (
        PrimaryKeyConstraint('ODP_Date', 'ODP_EM_Key', 'source_connection'),
    )


def create_etl_log_table_if_not_exists(engine):
    """Create ETL log table if it doesn't exist."""
    meta = MetaData()
    log_table = Table(
        'etl_extract_log', meta,
        Column('extractlogid', Integer, primary_key=True, autoincrement=True),
        Column('processlogid', String(100)),
        Column('source_connection', String(255)),
        Column('saved_count', Integer),
        Column('starttime', DateTime),
        Column('endtime', DateTime),
        Column('lastextractdatetime', DateTime),
        Column('success', Boolean),
        Column('status', String(50)),
        Column('errormessage', Text),
        extend_existing=True
    )
    meta.create_all(engine)  # Will only create if not exists
    return log_table

def create_table_if_not_exists(engine):
    """Create the transactions table if it doesn't exist"""
    Base.metadata.create_all(engine)
from sqlalchemy import Column, Integer, String, DateTime, Numeric, Text, Date, Float, Boolean, Index, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData

# Define the target table model
Base = declarative_base()

class HangerLaneData(Base):
    __tablename__ = 'operator_daily_performance'

    id = Column(Integer, primary_key=True, autoincrement=True)
    odp_key = Column(String(50))
    odp_date = Column(Date)
    shift = Column(String(10))
    odp_em_key = Column(Integer)
    em_rfid = Column(String(50))
    em_department = Column(String(100))
    em_firstname = Column(String(100))
    em_lastname = Column(String(100))
    odp_actual_clock_in = Column(DateTime)
    odp_actual_clock_out = Column(DateTime)
    odp_shift_clock_in = Column(DateTime)
    odp_shift_clock_out = Column(DateTime)
    odp_first_hanger_time = Column(DateTime)
    odp_last_hanger_time = Column(DateTime)
    odp_current_station = Column(String(100))
    odp_lump_sum_payment = Column(Numeric(10, 2))
    odp_make_up_pay_rate = Column(Numeric(10, 2))
    odp_last_hanger_start_time = Column(DateTime)
    odpd_key = Column(String(50))
    odpd_workstation = Column(String(50))
    odpd_wc_key = Column(Integer)
    odpd_quantity = Column(Integer)
    odpd_st_key = Column(String(50))
    st_id = Column(String(50))
    st_description = Column(String(100))
    odpd_lot_number = Column(String(50))
    odpd_oc_key = Column(String(50))
    oc_description = Column(String(100))
    loading_qty = Column(Integer)
    unloading_qty = Column(Integer)
    oc_piece_rate = Column(Numeric(10, 2))
    oc_standard_time = Column(Numeric(10, 2))
    odpd_standard = Column(Numeric(10, 2))
    odpd_actual_time = Column(Numeric(10, 2))
    odpd_pa_key = Column(Integer)
    odpd_pay_rate = Column(Numeric(10, 2))
    odpd_piece_rate = Column(Numeric(10, 2))
    odpd_start_time = Column(DateTime)
    odpd_cm_key = Column(String(50))
    cm_description = Column(String(100))
    odpd_sm_key = Column(String(50))
    sm_description = Column(String(100))
    odpd_normal_pay_factor = Column(Float)
    odpd_is_overtime = Column(Boolean)
    odpd_overtime_factor = Column(Float)
    odpd_edited_by = Column(String(50))
    odpd_edited_date = Column(DateTime)
    odpd_actual_time_from_reader = Column(Numeric(10, 2))
    odpd_stpo_key = Column(String(50))
    created_at = Column(DateTime)
    source_connection = Column(String(50))
    fg_item_key = Column(String(50))
    efficiency = Column(Float)
    ppd_tvwh = Column(Float)

    __table_args__ = (
        Index('idx_odp_date', 'odp_date'),
        Index('idx_created_at', 'created_at'),
        Index('idx_shift', 'shift'),
        Index('idx_source_connection', 'source_connection'),
        Index('idx_st_id', 'st_id'),
        Index('idx_odpd_lot_number', 'odpd_lot_number'),
        Index('idx_odpd_oc_key', 'odpd_oc_key'),
        Index('idx_odpd_cm_key', 'odpd_cm_key'),
        Index('idx_odpd_sm_key', 'odpd_sm_key'),
        UniqueConstraint('source_connection', 'odp_key', 'odpd_key', name='uq_source_connection_odpd_key'),
    )


def create_etl_log_table_if_not_exists(engine):
    """Create ETL aggregated OPD_Date base data log table if it doesn't exist."""
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
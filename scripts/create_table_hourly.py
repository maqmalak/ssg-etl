from sqlalchemy import (
    Column, Integer, String, DateTime, Numeric, Text, Date, Float, Boolean,
    PrimaryKeyConstraint, Table, MetaData
)
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

# Define the base
Base = declarative_base()

# -------------------- Main Table -------------------- #
class OdpHourlySummary(Base):
    __tablename__ = 'odp_hourly_summary'

    # Keys
    hour_timestamp = Column('hour_timestamp', DateTime, nullable=False)
    odp_date = Column('odp_date', Date, nullable=False)
    shift = Column('shift', String(10), nullable=False)
    odp_em_key = Column('odp_em_key', Integer, nullable=False)
    em_description = Column('em_description', String(500))
    odpd_workstation = Column('odpd_workstation', String(50))
    odpd_wc_key = Column('odpd_wc_key', Integer)
    odpd_st_key = Column('odpd_st_key', Integer)
    st_id = Column('st_id', String(50))
    st_description = Column('st_description', String(100))
    odpd_lot_number = Column('odpd_lot_number', String(50))
    odpd_oc_key = Column('odpd_oc_key', Integer)
    oc_description = Column('oc_description', String(100))
    odpd_cm_key = Column('odpd_cm_key', Integer)
    cm_description = Column('cm_description', String(100))
    odpd_sm_key = Column('odpd_sm_key', Integer)
    sm_description = Column('sm_description', String(100))
    odpd_is_overtime = Column('odpd_is_overtime', Boolean)
    odpd_stpo_key = Column('odpd_stpo_key', Integer)
    source_connection = Column('source_connection', String(50), nullable=False)

    # Aggregated KPIs

    total_quantity = Column('total_quantity', Integer)
    total_loading_qty = Column('total_loading_qty', Integer)
    total_unloading_qty = Column('total_unloading_qty', Integer)
    total_actual_time = Column('total_actual_time', Numeric(10, 2))
    total_standard_time = Column('total_standard_time', Numeric(10, 2))
    record_count = Column('record_count', Integer)

    # Metadata
    created_at = Column('created_at', DateTime, default=datetime.utcnow)

    __table_args__ = (
        PrimaryKeyConstraint(
            'hour_timestamp', 'odp_date', 'shift', 'odp_em_key', 'em_description',
            'odpd_workstation', 'odpd_wc_key', 'odpd_st_key', 'st_id', 'st_description',
            'odpd_lot_number', 'odpd_oc_key', 'oc_description', 'odpd_cm_key',
            'cm_description', 'odpd_sm_key', 'sm_description', 'odpd_is_overtime',
            'odpd_stpo_key', 'source_connection'
        ),
    )


# -------------------- Staging Table -------------------- #
class OdpHourlySummaryStaging(Base):
    __tablename__ = 'odp_hourly_summary_staging'

    # Same schema as main
    hour_timestamp = Column('hour_timestamp', DateTime, nullable=False)
    odp_date = Column('odp_date', Date, nullable=False)
    shift = Column('shift', String(10), nullable=False)
    odp_em_key = Column('odp_em_key', Integer, nullable=False)
    em_description = Column('em_description', String(500))
    odpd_workstation = Column('odpd_workstation', String(50))
    odpd_wc_key = Column('odpd_wc_key', Integer)
    odpd_st_key = Column('odpd_st_key', Integer)
    st_id = Column('st_id', String(50))
    st_description = Column('st_description', String(100))
    odpd_lot_number = Column('odpd_lot_number', String(50))
    odpd_oc_key = Column('odpd_oc_key', Integer)
    oc_description = Column('oc_description', String(100))
    odpd_cm_key = Column('odpd_cm_key', Integer)
    cm_description = Column('cm_description', String(100))
    odpd_sm_key = Column('odpd_sm_key', Integer)
    sm_description = Column('sm_description', String(100))
    odpd_is_overtime = Column('odpd_is_overtime', Boolean)
    odpd_stpo_key = Column('odpd_stpo_key', Integer)
    source_connection = Column('source_connection', String(50), nullable=False)

    # Aggregated KPIs
    total_quantity = Column('total_quantity', Integer)
    total_loading_qty = Column('total_loading_qty', Integer)
    total_unloading_qty = Column('total_unloading_qty', Integer)
    total_actual_time = Column('total_actual_time', Numeric(10, 2))
    total_standard_time = Column('total_standard_time', Numeric(10, 2))
    record_count = Column('record_count', Integer)

    created_at = Column('created_at', DateTime, default=datetime.utcnow)

    __table_args__ = (
        PrimaryKeyConstraint(
            'hour_timestamp', 'odp_date', 'shift', 'odp_em_key', 'em_description',
            'odpd_workstation', 'odpd_wc_key', 'odpd_st_key', 'st_id', 'st_description',
            'odpd_lot_number', 'odpd_oc_key', 'oc_description', 'odpd_cm_key',
            'cm_description', 'odpd_sm_key', 'sm_description', 'odpd_is_overtime',
            'odpd_stpo_key', 'source_connection'
        ),
    )


# -------------------- Log Table -------------------- #
def create_etl_hourly_log_odp_table_if_not_exists(engine):
    """Create ETL log table if not exists"""
    meta = MetaData()
    log_table = Table(
        'etl_extract_hourly_log', meta,
        Column('extractlogid', Integer, primary_key=True, autoincrement=True),
        Column('processlogid', String(100)),
        Column('source_connection', String(255)),
        Column('saved_count', Integer),
        Column('starttime', DateTime),
        Column('endtime', DateTime),
        Column('opd_date', Date),  # ✅ fixed lowercase
        Column('min_created_at', DateTime),
        Column('max_created_at', DateTime),
        Column('lastextractdatetime', DateTime),
        Column('success', Boolean),
        Column('status', String(50)),
        Column('errormessage', Text),
        extend_existing=True
    )
    meta.create_all(engine)
    return log_table


# -------------------- Create Tables -------------------- #
def create_hourly_table_if_not_exists(engine):
    """Create the main + staging tables if not exist"""
    Base.metadata.create_all(engine)


def create_staging_tables_if_not_exists(engine):
    """Create staging tables if not exist"""
    Base.metadata.create_all(engine)

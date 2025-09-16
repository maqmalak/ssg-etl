from sqlalchemy import Column, Integer, String, DateTime, Text, Date, Boolean, PrimaryKeyConstraint, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData
from datetime import datetime

Base = declarative_base()

class QualityControlRepair(Base):
    __tablename__ = 'quality_control_repair'

    # Define all columns with Column() and SQLAlchemy types
    id = Column(Integer, primary_key=True, autoincrement=True)
    qcr_key = Column(String(255))
    qcr_stpo_key = Column(Integer)
    qcr_defect_datetime = Column(DateTime, index=True)  # Indexed for better query performance
    shift = Column(String(1))  # 'A' or 'B'
    qcr_defect_em_key = Column(Integer)
    qcr_defect_st_key = Column(Integer)
    qcr_defect_oc_key = Column(Integer)
    qcr_sent_to_rework_by_em_key = Column(Integer)
    qcr_defect_quantity = Column(Integer)
    qcr_from_qc_station = Column(Integer)
    qcr_hm_id = Column(String(255))
    qcr_qc_datetime = Column(DateTime)
    qcr_repair_em_key = Column(Integer)
    qcr_repair_datetime = Column(DateTime)
    qcr_repair_quantity = Column(Integer)
    qcr_defect_cm_key = Column(Integer)
    qcr_defect_sm_key = Column(Integer)
    qcr_qcsc_key = Column(Integer)
    qcr_hm_key = Column(Integer)
    qcsc_description = Column(Text)
    em_firstname = Column(String(255))
    em_key = Column(Integer)
    em_rfid = Column(String(255))
    st_id = Column(String(255))
    st_description = Column(Text)
    stpo_st_key = Column(Integer)
    stpo_id = Column(String(255))
    stpo_ci_name = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow, index=True)  # Indexed for better query performance
    source_connection = Column(String(255), index=True)  # Indexed for better query performance

    # ✅ Define table arguments - removed conflicting primary key constraint
    # The primary key is already defined on the 'id' column above
    __table_args__ = (
        # Add indexes for commonly queried columns
        Index('idx_qcr_defect_datetime', 'qcr_defect_datetime'),
        Index('idx_created_at', 'created_at'),
        Index('idx_source_connection', 'source_connection'),
        Index('idx_qcr_stpo_key', 'qcr_stpo_key'),
        Index('idx_shift', 'shift'),
        Index('idx_qcr_defect_em_key', 'qcr_defect_em_key'),
        # Composite index for common query patterns
        Index('idx_source_connection_defect_datetime', 'source_connection', 'qcr_defect_datetime'),
        # Add a unique constraint on qcr_key to prevent duplicates
        UniqueConstraint('qcr_key', name='uq_qcr_key')
    )


# ================================
# LOG TABLE (optional: declarative style)
# ================================

class EtlExtractLog(Base):
    __tablename__ = 'etl_qcr_extract_log'

    extractlogid = Column(Integer, primary_key=True, autoincrement=True)
    processlogid = Column(String(100))
    source_connection = Column(String(255))
    saved_count = Column(Integer)
    starttime = Column(DateTime)
    endtime = Column(DateTime)
    lastextractdatetime = Column(DateTime)
    success = Column(Boolean)
    status = Column(String(50))
    errormessage = Column(Text)


# ================================
# Helper Functions
# ================================

def create_etl_log_qcr_table_if_not_exists(engine):
    """Create ETL log table if it doesn't exist."""
    # Since we're using declarative, just create_all
    # But if you want only this table:
    EtlExtractLog.__table__.create(bind=engine, checkfirst=True)
    return EtlExtractLog.__table__


def create_qcr_table_if_not_exists(engine):
    """Create the quality_control_repair table if it doesn't exist"""
    QualityControlRepair.__table__.create(bind=engine, checkfirst=True)
    return QualityControlRepair.__table__
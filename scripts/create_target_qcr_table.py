from sqlalchemy import Column, Integer, String, DateTime, Text, Date, Boolean, PrimaryKeyConstraint, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData
from datetime import datetime

Base = declarative_base()

class QualityControlRepair(Base):
    __tablename__ = 'quality_control_repair'

    id = Column(Integer, primary_key=True, autoincrement=True)
    qcr_key = Column(String(36), nullable=False)                 # UUID
    qcr_stpo_key = Column(Integer)
    qcr_defect_datetime = Column(DateTime)
    shift = Column(String(10))                                   # "Day"/"Night"
    qcr_defect_em_key = Column(Integer)
    defect_em_firstname = Column(String(100))
    defect_em_lastname = Column(String(100))
    defect_em_rfid = Column(String(50))
    qcr_defect_st_key = Column(Integer)
    qcr_defect_oc_key = Column(Integer)
    oc_description = Column(String(255))
    qcr_sent_to_rework_by_em_key = Column(Integer)
    qcr_defect_quantity = Column(Integer)
    qcr_from_qc_station = Column(String(50))
    qcr_hm_id = Column(String(50))
    qcr_qc_datetime = Column(DateTime)
    qcr_repair_em_key = Column(Integer)
    em_repair_firstname = Column(String(100))
    em_repair_lastname = Column(String(100))
    em_repair_rfid = Column(String(50))
    qcr_repair_datetime = Column(DateTime)
    qcr_repair_quantity = Column(Integer)
    qcr_defect_cm_key = Column(Integer)
    cm_description = Column(String(100))
    qcr_defect_sm_key = Column(Integer)
    sm_description = Column(String(100))
    qcr_qcsc_key = Column(String(36))                            # GUID
    qcr_hm_key = Column(Integer, nullable=True)
    qcsc_description = Column(String(100))
    em_firstname = Column(String(100))
    em_key = Column(Integer)
    em_rfid = Column(String(50))
    st_id = Column(String(50))
    st_description = Column(String(100))
    stpo_st_key = Column(Integer)
    stpo_id = Column(String(50))
    stpo_ci_name = Column(String(100))
    created_at = Column(DateTime)
    source_connection = Column(String(50))

    # ✅ Define table arguments - removed conflicting primary key constraint
    # The primary key is already defined on the 'id' column above
    __table_args__ = (
        # Add indexes for commonly queried columns
        Index('idx_qcr_defect_datetime', 'qcr_defect_datetime'),
        Index('idx_qcr_created_at', 'created_at'),
        Index('idx_qcr_em_key', 'em_key'),
        Index('idx_qcr_defect_st_key', 'qcr_defect_st_key'),
        Index('idx_qcr_defect_oc_key', 'qcr_defect_oc_key'),
        Index('idx_qcr_defect_cm_key', 'qcr_defect_cm_key'),
        Index('idx_qcr_defect_sm_key', 'qcr_defect_sm_key'),        
        Index('idx_qcr_source_connection', 'source_connection'),
        Index('idx_qcr_stpo_key', 'qcr_stpo_key'),
        Index('idx_qcr_shift', 'shift'),
        Index('idx_qcr_defect_em_key', 'qcr_defect_em_key'),
        # Composite index for common query patterns
        Index('idx_qcr_source_connection_defect_datetime', 'source_connection', 'qcr_defect_datetime'),
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
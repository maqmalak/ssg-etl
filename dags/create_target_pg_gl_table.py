from sqlalchemy import Column, Integer, String, DateTime, Numeric, Text, Date, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData

# Define the target table model
Base = declarative_base()

class GeneralLedgerTransaction(Base):
    __tablename__ = 'general_ledger_transactions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    Dated = Column(Date)
    Vtp = Column(String(10))
    ScrVoucher_NO = Column(String(100))
    EntryNo = Column(Numeric)
    debit = Column(Float)
    credit = Column(Float)
    net = Column(Float)
    Narration = Column(Text)
    FinancialYear = Column(String(10))
    CreationDate = Column(DateTime)
    account_id = Column(String(20))
    sub_id = Column(String(20))
    Account_Title = Column(String(255))
    source_connection = Column(String(255))
    root_type = Column(String(50))
    account_type = Column(String(50))
    Level_id1 = Column(String(20))
    Upper_Level_1_Title = Column(String(255))
    Level_id2 = Column(String(20))
    Upper_Level_2_Title = Column(String(255))
    TypeOfID = Column(Numeric)
    LevelNo = Column(Numeric)
    VoucherDate = Column(Date)
    Prp_Date = Column(Date)
    Prp_ID = Column(String(50))
    ChqDate = Column(Date)
    ChqClearanceDate = Column(Date)
    Chk_Date = Column(Date)
    ChqNo = Column(String(50))
    ChallanNo = Column(String(50))
    CancellDate = Column(Date)
    CancelledBy = Column(String(50))
    atp2_ID = Column(String(50))


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
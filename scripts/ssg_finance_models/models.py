from scripts.base_model import BaseModel
from sqlalchemy import Column, Integer, String, DateTime, Numeric, Text, Date, Float



class SourceGeneralLedger(BaseModel):
    __tablename__ = 'GeneralLedger'

    Dated = Column(Date)
    Vtp = Column(String(10))
    ScrVoucher_NO = Column(String(100))
    EntryNo = Column(Numeric)
    debit = Column(Float)
    credit = Column(Float)
    Narration = Column(Text)
    FinancialYear = Column(String(10))
    CreationDate = Column(DateTime)
    id = Column(String(20))
    Account_Title = Column(String(255))

class SourceUlCoa(BaseModel):
    __tablename__ = 'UL_COA'

    ID = Column(String(20))
    ID1 = Column(String(20))
    ID2 = Column(String(20))
    Title = Column(String(255))
class SourceCoa3(BaseModel):
    __tablename__ = 'Coa3'

    ID = Column(String(20))
    ULID1 = Column(String(20))
    ULID2 = Column(String(20))
    id1 = Column(String(20))
    id2 = Column(String(20))
    Title = Column(String(255))

class SourceUlCoa(BaseModel):
    __tablename__ = 'UL_COA'

    ID = Column(String(20))
    ID1 = Column(String(20))
    ID2 = Column(String(20))
    Title = Column(String(255))


class TargetGeneralLedger(BaseModel):
    __tablename__ = 'GeneralLedger'

    id = Column(Integer, primary_key=True, autoincrement=True)
    Dated = Column(Date)
    Vtp = Column(String(10))
    ScrVoucher_NO = Column(String(100))
    EntryNo = Column(Numeric)
    debit = Column(Numeric)
    credit = Column(Numeric)
    net = Column(Numeric)
    Narration = Column(Text)
    FinancialYear = Column(String(10))
    CreationDate = Column(DateTime)
    account_id = Column(String(20))
    sub_id = Column(String(20))
    main_id = Column(String(20))
    Account_Title = Column(String(255))
    Sub_Title = Column(String(255))
    Main_Title = Column(String(255))
    source_connection = Column(String(255))


class ApparalOperatorPerformance(BaseModel):
    __tablename__ = 'ApparalOperatorsPerformance'

    Dated = Column(Date)
    Vtp = Column(String(20))
    Mnth = Column(String(8))
    vYear = Column(String(8))
    Location = Column(String(100))
    ForLocation = Column(String(100))
    vno = Column(Numeric)
    ScrVoucher_No = Column(String(100))
    Voucher_No = Column(String(100))
    MainDeptt_ID = Column(String(50))
    EmpDeptt_ID = Column(String(50))
    Narration = Column(Text)
    EntryNo = Column(Numeric)
    CreationDate = Column(DateTime)
    prp_id = Column(String(50))
    ComputerName = Column(String(100))


class ApparalOperatorPerformanceDet(BaseModel):
    __tablename__ = 'ApparalOperatorsPerformance_Det'

    Dated = Column(Date)
    Vtp = Column(String(10))
    Mnth = Column(String(8))
    vYear = Column(String(8))
    Location = Column(String(100))
    vno = Column(Numeric)
    srno = Column(Numeric)
    Voucher_No = Column(String(100))
    DVoucher_No = Column(String(100))
    ProductionDate = Column(Date)
    Emp_ID = Column(String(50))
    Location_ID = Column(String(50))
    Line_ID = Column(String(50))
    Article_ID = Column(String(50))
    SubOperation_ID = Column(String(50))
    Qty = Column(Numeric)
    EntryNo = Column(Numeric)
    PONo = Column(String(50))
    SMV = Column(Float)
    Conversion = Column(Float)

from sqlalchemy import MetaData, Table, Column, String, Date, Integer, Float, TIMESTAMP, func
from sqlalchemy.orm import declared_attr, Mapped
from src.database_client import database_client

class TableMixin(object):
    @declared_attr
    def id(self):
        return Column(name='id', type_=Integer, primary_key=True, autoincrement=True)

# ------------------- Table loan_data -------------------
class LoanData(database_client.Base, TableMixin):
    __tablename__ = "loan_data"
                    
    id = Column('id', Integer, primary_key=True, autoincrement=True)
    region = Column('region', String(255), nullable=True)
    issue_d = Column('issue_d', Date, nullable=True)
    loan_describe_int = Column('loan_describe_int', Integer, nullable=True)
    last_fico_range_high = Column('last_fico_range_high', Integer, nullable=True)
    last_fico_range_low = Column('last_fico_range_low', Integer, nullable=True)
    int_rate = Column('int_rate', Float, nullable=True)
    fico_range_low = Column('fico_range_low', Float, nullable=True)
    acc_open_past_24mths = Column('acc_open_past_24mths', Integer, nullable=True)
    dti = Column('dti', Float, nullable=True)
    num_tl_op_past_12m = Column('num_tl_op_past_12m', Integer, nullable=True)
    loan_amnt = Column('loan_amnt', Float, nullable=True)
    mort_acc = Column('mort_acc', Integer, nullable=True)
    avg_cur_bal = Column('avg_cur_bal', Float, nullable=True)
    bc_open_to_buy = Column('bc_open_to_buy', Float, nullable=True)
    num_actv_rev_tl = Column('num_actv_rev_tl', Integer, nullable=True)
    debt_settlement_flag = Column('debt_settlement_flag', String(255), nullable=True)
    term = Column('term', String(255), nullable=True)
    purpose = Column('purpose', String(255), nullable=True)
    hardship_flag = Column('hardship_flag', String(255), nullable=True)
    pymnt_plan = Column('pymnt_plan', String(255), nullable=True)

# ------------------- Table pred_loan_data -------------------
class PredLoanData(database_client.Base, TableMixin):
    __tablename__ = "pred_loan_data"

    loan_data_id = Column('loan_data_id', Float)
    prediction = Column('prediction', String(255), nullable=True)
    created_at = Column(name="created_at", type_=TIMESTAMP, nullable=True, unique=False, default=func.now())
    
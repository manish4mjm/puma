from db.dao.generic_sqlalchemy_dao import GenericSqlAlchemyDao
import logging
import pandas as pd
from db.model import *
import re


class StocksDao(GenericSqlAlchemyDao):

    def __init__(self, tz='Asia/Singapore', logger=None):
        super().__init__()
        self.tz = tz
        self.logger = logger or logging.getLogger(__name__)

    def __del__(self):
        super().__del__()

    def get_all_stocks(self):
        statement = self.session.query(Equity).statement
        data_df = pd.read_sql(statement, self.session.bind)
        return data_df

    def get_all_stocks_prices(self):
        statement = self.session.query(EquityEodData).order_by(EquityEodData.trading_date).statement
        data_df = pd.read_sql(statement, self.session.bind)
        return data_df

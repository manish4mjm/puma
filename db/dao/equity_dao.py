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

    def get_stock_info_by_date(self, date):
        filters = [(EquityEodData.trading_date == date)]

        statement = self.session.query(Equity.local_code, Equity.comp_name, Equity.sector,
                                       EquityEodData.close, EquityEodData.adj_close, EquityAnrData.anr_count,
                                       EquityAnrData.anr_med, EquityAnrData.anr_reco,
                                       EquityIndicators.rsi_14, EquityIndicators.rsi_6, EquityIndicators.macd,
                                       EquityIndicators.ema_50,
                                       EquityIndicators.ema_100, EquityIndicators.ema_200, EquityIndicators.macd) \
            .join(Equity, Equity.equity_id == EquityEodData.equity_id) \
            .join(EquityAnrData, and_(EquityAnrData.equity_id == EquityEodData.equity_id,
                                      EquityAnrData.trading_date == EquityEodData.trading_date)) \
            .join(EquityIndicators, and_(EquityIndicators.equity_id == EquityEodData.equity_id,
                                      EquityIndicators.trading_date == EquityEodData.trading_date)) \
            .order_by(EquityEodData.trading_date) \
            .filter(and_(*filters)).statement

        data_df = pd.read_sql(statement, self.session.bind)

        return data_df

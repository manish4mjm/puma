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

    def get_all_stocks(self, exchange):
        filters = [(Equity.exchange == exchange)]
        statement = self.session.query(Equity).filter(and_(*filters)).statement
        data_df = pd.read_sql(statement, self.session.bind)
        return data_df

    def get_all_stocks_prices(self, exchange):
        filters = [(Equity.exchange == exchange)]
        statement = self.session.query(EquityEodData).order_by(EquityEodData.trading_date) \
            .join(Equity, Equity.equity_id == EquityEodData.equity_id) \
            .order_by(EquityEodData.trading_date) \
            .filter(and_(*filters)).statement
        data_df = pd.read_sql(statement, self.session.bind)
        return data_df

    def get_all_stocks_anr(self, exchange):
        filters = [(Equity.exchange == exchange)]
        statement = self.session.query(EquityAnrData).order_by(EquityAnrData.trading_date) \
            .join(Equity, Equity.equity_id == EquityAnrData.equity_id) \
            .order_by(EquityAnrData.trading_date) \
            .filter(and_(*filters)).statement
        data_df = pd.read_sql(statement, self.session.bind)
        return data_df

    def get_all_stocks_prices_max_entry_date(self, exchange):
        filters = [(Equity.exchange == exchange)]

        statement = self.session.query(EquityEodData.equity_id,
                                       label('last_entry', func.max(EquityEodData.trading_date))) \
            .join(Equity, Equity.equity_id == EquityEodData.equity_id) \
            .filter(and_(*filters)) \
            .group_by(EquityEodData.equity_id) \
            .statement

        data_df = pd.read_sql(statement, self.session.bind)
        return data_df

    def get_all_stocks_anr_max_entry_date(self, exchange):
        filters = [(Equity.exchange == exchange)]

        statement = self.session.query(EquityAnrData.equity_id,
                                       label('last_entry', func.max(EquityAnrData.trading_date))) \
            .join(Equity, Equity.equity_id == EquityAnrData.equity_id) \
            .filter(and_(*filters)) \
            .group_by(EquityAnrData.equity_id) \
            .statement

        data_df = pd.read_sql(statement, self.session.bind)
        return data_df

    def get_all_stocks_indicators_max_entry_date(self, exchange):
        filters = [(Equity.exchange == exchange)]

        statement = self.session.query(EquityIndicators.equity_id,
                                       label('last_entry', func.max(EquityIndicators.trading_date))) \
            .join(Equity, Equity.equity_id == EquityIndicators.equity_id) \
            .filter(and_(*filters)) \
            .group_by(EquityIndicators.equity_id) \
            .statement

        data_df = pd.read_sql(statement, self.session.bind)
        return data_df

    def get_stock_info_by_date(self, exchange, date):
        filters = [(EquityEodData.trading_date == date),
                   (Equity.exchange == exchange)]

        statement = self.session.query(Equity.local_code, Equity.comp_name, Equity.sector,
                                       EquityEodData.close, EquityEodData.adj_close, EquityAnrData.anr_count,
                                       EquityAnrData.anr_med, EquityAnrData.anr_reco,
                                       EquityIndicators.rsi_14, EquityIndicators.rsi_6, EquityIndicators.macd,
                                       EquityIndicators.ema_50, EquityIndicators.mfi_14, EquityIndicators.mfi_6,
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

from db.base import *


class Equity(Base):
    __tablename__ = 'equity'
    __table_args__ = ({'schema': 'ref'})
    equity_id = Column(Integer, primary_key=True)
    comp_name = Column(String, nullable=False)
    sector = Column(String, nullable=False)
    local_code = Column(String, nullable=False)
    ric_code = Column(String, nullable=False)
    ds_code = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    currency = Column(String, nullable=False)


class EquityEodData(Base):
    __tablename__ = 'equity_eod_data'
    __table_args__ = ({'schema': 'time_series'})
    trading_date = Column(Date, primary_key=True)
    equity_id = Column(Integer, primary_key=True)
    open = Column(Numeric, nullable=False)
    high = Column(Numeric, nullable=False)
    low = Column(Numeric, nullable=False)
    close = Column(Numeric, nullable=False)
    adj_close = Column(Numeric, nullable=False)
    volume = Column(Numeric, nullable=False)


class EquityIndicators(Base):
    __tablename__ = 'equity_indicators'
    __table_args__ = ({'schema': 'time_series'})
    trading_date = Column(Date, primary_key=True)
    equity_id = Column(Integer, primary_key=True)
    rsi_6 = Column(Numeric, nullable=False)
    rsi_14 = Column(Numeric, nullable=False)
    boll_up = Column(Numeric, nullable=False)
    boll_lw = Column(Numeric, nullable=False)
    ema_200 = Column(Numeric, nullable=False)
    ema_100 = Column(Numeric, nullable=False)
    ema_50 = Column(Numeric, nullable=False)
    macd = Column(Numeric, nullable=False)


class EquityAnrData(Base):
    __tablename__ = 'equity_anr_data'
    __table_args__ = ({'schema': 'time_series'})
    trading_date = Column(Date, primary_key=True)
    equity_id = Column(Integer, primary_key=True)
    anr_reco = Column(Numeric, nullable=False)
    anr_count = Column(Numeric, nullable=False)
    anr_med = Column(Numeric, nullable=False)

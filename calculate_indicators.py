from ta.utils import dropna
from ta.volatility import BollingerBands
from ta.trend import EMAIndicator
from ta.trend import SMAIndicator
from ta.trend import MACD
from ta.momentum import RSIIndicator
from db.model import *
from download import *
from db.dao.equity_dao import StocksDao
import traceback

if __name__ == '__main__':

    exchange='HEX'
    dao = StocksDao()
    equity_ref_df = dao.get_all_stocks(exchange)
    eod_df = dao.get_all_stocks_prices(exchange)

    for idx1, row1 in equity_ref_df.iterrows():
        equity_id = row1['equity_id']
        ticker = row1['local_code']
        try:
            insert_list = list()
            # get all the prices for equity
            ticker_eod_df = eod_df[eod_df['equity_id'] == equity_id]
            ticker_eod_df = ticker_eod_df.sort_values(by=['trading_date'])
            df = dropna(ticker_eod_df)

            # Initialize Bollinger Bands Indicator
            indicator_bb = BollingerBands(close=df["adj_close"], window=20, window_dev=2)
            # Add Bollinger Bands features
            df['bb_bbm'] = indicator_bb.bollinger_mavg()
            df['bb_bbh'] = indicator_bb.bollinger_hband()
            df['bb_bbl'] = indicator_bb.bollinger_lband()

            # EMA Indicator
            indicator_ema_200 = EMAIndicator(close=df["adj_close"], window=200)
            df['ema_200'] = indicator_ema_200.ema_indicator()
            indicator_ema_100 = EMAIndicator(close=df["adj_close"], window=100)
            df['ema_100'] = indicator_ema_100.ema_indicator()
            indicator_ema_50 = EMAIndicator(close=df["adj_close"], window=50)
            df['ema_50'] = indicator_ema_50.ema_indicator()

            # SMA Indicator
            indicator_sma_200 = SMAIndicator(close=df["adj_close"], window=200)
            df['sma_200'] = indicator_sma_200.sma_indicator()
            indicator_sma_100 = SMAIndicator(close=df["adj_close"], window=100)
            df['sma_100'] = indicator_sma_100.sma_indicator()
            indicator_sma_50 = SMAIndicator(close=df["adj_close"], window=50)
            df['sma_50'] = indicator_sma_50.sma_indicator()

            # RSI Indicator
            indicator_rsi_6 = RSIIndicator(close=df["adj_close"], window=6)
            df['rsi_6'] = indicator_rsi_6.rsi()
            indicator_rsi_14 = RSIIndicator(close=df["adj_close"], window=14)
            df['rsi_14'] = indicator_rsi_14.rsi()

            # MACD Indicator
            indicator_macd = MACD(close=df["adj_close"])
            df['macd'] = indicator_macd.macd()
            # df = df.dropna(how='all)

            for idx, row in df.iterrows():
                trade_date = row['trading_date']
                bb_bbh = row['bb_bbh']
                bb_bbl = row['bb_bbl']
                ema_200 = row['ema_200']
                ema_100 = row['ema_100']
                ema_50 = row['ema_50']
                rsi_14 = row['rsi_14']
                rsi_6 = row['rsi_6']
                macd = row['macd']

                equity_indicators = EquityIndicators(equity_id=equity_id, trading_date=trade_date, boll_up=bb_bbh,
                                                     boll_lw=bb_bbl, ema_200=ema_200, ema_100=ema_100, ema_50=ema_50,
                                                     rsi_14=rsi_14, rsi_6=rsi_6, macd=macd)
                insert_list.append(equity_indicators)

            dao.bulk_save(insert_list)
            dao.commit()
            print('Completed insertion for ticker {}'.format(ticker))
        except Exception as e:
            print('Data not found for ticker {} with error'.format(ticker))
            traceback.print_exc()
        finally:
            dao.rollback()

    print('Done')

from ta.utils import dropna
from ta.others import DailyReturnIndicator
from ta.others import DailyLogReturnIndicator
from ta.others import CumulativeReturnIndicator
from db.model import *
from download import *
from db.dao.equity_dao import StocksDao
import traceback
from constants import *

if __name__ == '__main__':

    exchange_list = [SGX, HEX, NASDAQ, NYSE]

    for exchange in exchange_list:
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

                df['return'] = DailyReturnIndicator(close=df["adj_close"]).daily_return()
                df['log_return'] = DailyLogReturnIndicator(close=df["adj_close"]).daily_log_return()
                df['cum_return'] = CumulativeReturnIndicator(close=df["adj_close"]).cumulative_return()

                for idx, row in df.iterrows():
                    trade_date = row['trading_date']
                    daily_return = row['return']
                    log_return = row['log_return']
                    cum_return = row['cum_return']

                    equity_returns = EquityReturns(equity_id=equity_id, trading_date=trade_date,
                                                   daily_return=daily_return,
                                                   daily_log_return=log_return, cumulative_return=cum_return)
                    insert_list.append(equity_returns)

                dao.bulk_save(insert_list)
                dao.commit()
                print('Completed insertion for ticker {}'.format(ticker))
            except Exception as e:
                print('Data not found for ticker {} with error'.format(ticker))
                traceback.print_exc()
            finally:
                dao.rollback()

        print('Done {}'.format(exchange))

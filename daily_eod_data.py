from db.model import *
from download import *
from db.dao.equity_dao import StocksDao
import traceback
import datetime as dt
from constants import *

if __name__ == '__main__':

    exchange_list = [NASDAQ, NYSE, SGX, HEX]

    for exchange in exchange_list:

        dao = StocksDao()
        # Ref data
        equity_ref_df = dao.get_all_stocks(exchange)
        # Prices data
        eod_df = dao.get_all_stocks_prices_max_entry_date(exchange)
        last_entry_dict = dict(zip(eod_df['equity_id'].tolist(), eod_df['last_entry'].tolist()))

        if exchange == 'HEX':
            equity_ref_df['y_ticker'] = equity_ref_df['ric_code']
        elif exchange == 'SGX':
            equity_ref_df['y_ticker'] = equity_ref_df['local_code'].apply(lambda x: x[2:] + '.SI')
        elif exchange == 'NASDAQ':
            equity_ref_df['y_ticker'] = equity_ref_df['local_code']
        elif exchange == 'NYSE':
            equity_ref_df['y_ticker'] = equity_ref_df['local_code']

        tickers = equity_ref_df['y_ticker'].unique().tolist()

        for idx1, row1 in equity_ref_df.iterrows():
            ticker = row1['y_ticker']
            equity_id = row1['equity_id']

            try:
                insert_list = list()
                data_one = download_one(ticker, period='1y')
                ticker_df = parse_quotes(data_one["chart"]["result"][0])
                # Filter based on last entry in db
                ticker_df = ticker_df[ticker_df.index > last_entry_dict[equity_id]]

                for idx, row in ticker_df.iterrows():
                    trade_date = idx
                    open = row['open']
                    high = row['high']
                    low = row['low']
                    close = row['close']
                    volume = row['volume']
                    adj_close = row['adj_close']

                    equity_eod_data = EquityEodData(equity_id=equity_id, trading_date=trade_date, open=open, low=low,
                                                    high=high, close=close, adj_close=adj_close, volume=volume,
                                                    data_source='YAHOO')
                    insert_list.append(equity_eod_data)

                dao.bulk_save(insert_list)
                dao.commit()
                print('Completed insertion for ticker {}'.format(ticker))
            except Exception as e:
                print('Data not found for ticker {} with error as {}'.format(ticker, data_one['chart']['error']))
                traceback.print_exc()
            finally:
                dao.rollback()

        print('Done {}'.format(exchange))

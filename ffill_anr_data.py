from db.model import *
from download import *
from db.dao.equity_dao import StocksDao
import traceback
import PyDSWS
import datetime as dt
from pandas.tseries.offsets import BDay
import pandas as pd
from constants import *
from constants import *

if __name__ == '__main__':

    exchange_list = [SGX]
    conn = PyDSWS.Datastream(username='ZINX001', password='PEACH709')

    for exchange in exchange_list:

        # start_date = '20160101'
        start_date = dt.date.today().strftime('%Y%m%d')
        end_date = dt.date.today().strftime('%Y%m%d')

        dao = StocksDao()
        # Ref data
        equity_ref_df = dao.get_all_stocks(exchange)
        anr_df = dao.get_all_stocks_anr(exchange)
        equity_list = anr_df['equity_id'].unique().tolist()

        # Anr data
        eod_df = dao.get_all_stocks_anr_max_entry_date(exchange)
        last_entry_dict = dict(zip(eod_df['equity_id'].tolist(), eod_df['last_entry'].tolist()))

        for equity_id in equity_list:
            try:

                # get previous date
                insert_list = list()
                prev_date = dt.date.today() - dt.timedelta(days=1)
                last_entry_dt = last_entry_dict[equity_id]
                stock_anr_df = anr_df[anr_df['equity_id'] == equity_id]
                stock_anr_df.set_index('trading_date', inplace=True)
                date_range = pd.bdate_range(last_entry_dt, prev_date, tz=None, freq='B')

                # check the gap of range
                if len(date_range.tolist()) <= 15:
                    stock_anr_df = stock_anr_df.reindex(date_range)
                    stock_anr_df = stock_anr_df.fillna(method='ffill')
                    stock_anr_df.index = pd.to_datetime(stock_anr_df.index).date
                    data_df = stock_anr_df[stock_anr_df.index > last_entry_dict[equity_id]]

                    for idx, row in data_df.iterrows():
                        trade_date = idx
                        rec_med = row['anr_med']
                        rec_count = row['anr_count']
                        rec_avg = row['anr_reco']

                        equity_anr_data = EquityAnrData(equity_id=equity_id, trading_date=trade_date, anr_reco=rec_avg,
                                                        anr_count=rec_count, anr_med=rec_med)
                        insert_list.append(equity_anr_data)

                    dao.bulk_save(insert_list)
                    dao.commit()
                    print('Completed insertion for ticker {}'.format(equity_id))
            except Exception as e:
                print('Data not found for ticker {} with error '.format(equity_id))
                traceback.print_exc()
            finally:
                dao.rollback()

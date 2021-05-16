from db.model import *
from download import *
from db.dao.equity_dao import StocksDao
import traceback
import PyDSWS

if __name__ == '__main__':

    start_date = '20160101'
    end_date = '20210514'

    dao = StocksDao()
    equity_ref_df = dao.get_all_stocks()

    conn = PyDSWS.Datastream(username='ZINX001', password='PEACH709')

    for idx1, row1 in equity_ref_df.iterrows():
        ticker = row1['ds_code']
        equity_id = row1['equity_id']
        try:
            insert_list = list()
            data = conn.get_data(tickers=ticker, fields=['RECMED', 'RECNO', 'RECCON'], start=start_date, end=end_date)
            data.fillna(0, inplace=True)
            data_df = data[ticker]
            data_df = data_df[data_df['RECNO'] != 0]

            for idx, row in data_df.iterrows():
                trade_date = idx
                rec_med = row['RECMED']
                rec_count = row['RECCON']
                rec_avg = row['RECNO']

                equity_anr_data = EquityAnrData(equity_id=equity_id, trading_date=trade_date, anr_reco=rec_avg,
                                                anr_count=rec_count, anr_med=rec_med)
                insert_list.append(equity_anr_data)

            dao.bulk_save(insert_list)
            dao.commit()
            print('Completed insertion for ticker {}'.format(ticker))
        except Exception as e:
            print('Data not found for ticker {} with error '.format(ticker))
            traceback.print_exc()
        finally:
            dao.rollback()

    print('Done')

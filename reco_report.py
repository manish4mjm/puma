from db.model import *
from download import *
from db.dao.equity_dao import StocksDao
import traceback

if __name__ == '__main__':
    exchange = 'HEX'
    trade_date = '2021-05-14'
    dao = StocksDao()
    stock_info_df = dao.get_stock_info_by_date(exchange, '2021-05-14')
    stock_info_df['range'] = (abs(stock_info_df['adj_close'] - stock_info_df['ema_200']) / stock_info_df['ema_200'])*100

    report_df = stock_info_df[ (stock_info_df['range'] <=10 ) & (stock_info_df['anr_med'] <=3.0)]
    report_df['reco'] = report_df['rsi_14'].apply(lambda x : 'HOT-BUYS' if x <=40 else 'COLD-BUY')
    report_df = report_df.sort_values(by=['reco', 'anr_med', 'anr_count', 'range'], ascending=False)

    report_df.to_csv('reco_{}_{}.csv'.format(exchange, trade_date))

    print(report_df)

    print('Done')

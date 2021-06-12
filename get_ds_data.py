import pandas as pd
import PyDSWS

# fields = ['RIC', 'NAME', 'INDM2', 'INDM3', 'LOC', 'MNEM', 'ISIN']

fields = ['RECCON', 'RECMED', 'RECNO', 'RECSTD', 'RECSBUY', 'RECSSELL',   'RECBUY', 'RECSELL', 'RECMBUY', 'RECHOLD', 'RECMBUY', 'RECMSELL']

ref_df = pd.read_csv(r'C:\work-area\data\us\stock_info_all.csv')

ref_df = ref_df.dropna(how='any')
ref_df['SYMBOL'] = ref_df['SYMBOL'].apply(lambda x : 'U:{}'.format(x))

ds_list = ref_df['SYMBOL'].unique().tolist()

conn = PyDSWS.Datastream(username='ZINX001', password='PEACH709')

df_list = list()
for ticker in ds_list:
    data = conn.get_data(tickers=ticker, fields=fields)
    df = data[ticker]
    df['Exchange'] = 'NYSE'
    df['Currency'] = 'USD'
    df_list.append(df)

res_df = pd.concat(df_list)
ref_df = res_df.dropna(how='any')

res_df.to_csv(r'ds_data_nasdaq.csv')

print('done')

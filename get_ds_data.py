import pandas as pd
import PyDSWS

fields = ['RIC', 'NAME', 'INDM2', 'INDM3', 'LOC', 'MNEM', 'ISIN']

ref_df = pd.read_csv(r'C:\work-area\data\sg\stock_info_all.csv')

ref_df = ref_df.dropna(how='any')

ds_list = ref_df['DS'].unique().tolist()

conn = PyDSWS.Datastream(username='ZINX001', password='PEACH709')

df_list = list()
for ticker in ds_list:
    data = conn.get_data(tickers=ticker, fields=fields)
    df = data[ticker]
    df_list.append(df)

res_df = pd.concat(df_list)

res_df.to_csv(r'ds_data_sg.csv')

print('done')

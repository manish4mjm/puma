import pandas as pd
import PyDSWS

fields = ['RIC', 'NAME']

ref_df = pd.read_csv(r'C:\work-area\data\sg\stock_info_all.csv')

ds_list = ref_df['DS'].unique().tolist()

conn = PyDSWS.Datastream(username='ZINX001', password='PEACH709')

data = conn.get_data(tickers=ds_list[:1], fields=fields)

print('done')

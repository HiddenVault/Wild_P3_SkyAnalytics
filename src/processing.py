import moduleProcessing
import pandas as pd

link = 'data/preprocessing/degradations_FULL.csv'
df = pd.read_csv(link)

df = moduleProcessing.create_column_id(df, 'degradations', 'measure_day', 'id')
df.to_csv('data/processing/degradations.csv', index=False)


link = 'data/preprocessing/logs_vols_FULL.csv'
df = pd.read_csv(link)
df = moduleProcessing.create_column_id(df, 'logs_vols', 'jour_vol', 'id')
df = moduleProcessing.split_column_json(df, column_name = 'sensor_data')
df.to_csv('data/processing/logs_vols.csv', index=False)
import moduleProcessing
import pandas as pd

# Traitement du dataset degradations_FULL
link = 'data/preprocessing/degradations_FULL.csv'
df = pd.read_csv(link)

df = moduleProcessing.create_column_id(df, 'degradations', 'measure_day', 'id')
df.to_csv('data/processing/degradations.csv', index=False)

# Traitement du dataset logs_vols_FULL
link = 'data/preprocessing/logs_vols_FULL.csv'
df = pd.read_csv(link)
df = moduleProcessing.create_column_id(df, 'logs_vols', 'jour_vol', 'id')
df = moduleProcessing.split_column_json(df, column_name = 'sensor_data')
df.to_csv('data/processing/logs_vols.csv', index=False)

# Traitement du dataset  
# link = 'data/incoming/composants_2023-11-07.csv'
# df = pd.read_csv(link)


# Traitement du dataset  
link = 'data/incoming/aeronefs_2023-11-07.csv'
df = pd.read_csv(link)
df = moduleProcessing.create_column(df, created_column= 'img', position_column = 2, default_value = 'none.png')
df.to_csv('data/processing/aeronefs.csv', index=False)

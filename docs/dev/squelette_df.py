import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import datetime as dt
import math
import re

deg_maj = pd.read_csv('degradations.csv')
vol_maj = pd.read_csv('logs_vols.csv')
aero = pd.read_csv('aeronefs.csv')
comp = pd.read_csv('composants.csv')

aero['debut_service'] = pd.to_datetime(aero['debut_service'])
aero['last_maint'] = pd.to_datetime(aero['last_maint'])
aero['end_maint'] = pd.to_datetime(aero['end_maint'])

deg_maj['measure_day'] = pd.to_datetime(deg_maj['measure_day'])
deg_maj.drop_duplicates(subset=['compo_concerned'], inplace=True)

vol_maj['jour_vol'] = pd.to_datetime(vol_maj['jour_vol'])
vol_maj['time_en_air_minute'] = vol_maj['time_en_air'].apply(lambda x: x*60)

df = aero.merge(comp, on='ref_aero', how ='left')
df = df.drop('img',axis=1)
df = df.drop('en_maintenance',axis=1)
df = df.drop('descr',axis=1)
df['usure_2023-11-07']= df['taux_usure_actuel']
df = df.drop('taux_usure_actuel',axis=1)


pivot_deg = deg_maj.pivot_table(
    values='usure_cumulee',
    index=['ref_aero','compo_concerned'],
    columns=['measure_day'],
    aggfunc='sum',
    fill_value=0
).reset_index()

def remove_time_from_date(date_str):
    return re.sub(r'\s00:00:00$', '', str(date_str))

pivot_deg.columns = pivot_deg.columns[:2].tolist() + ['usure_' + remove_time_from_date(col) if col != 'mesure_day' else str(col) for col in pivot_deg.columns[2:]]

pivot_vol_time = vol_maj.pivot_table(
    index='ref_aero',
    columns=['jour_vol'],
    values=['time_en_air_minute'],
    aggfunc='sum',
    fill_value=0,  
).reset_index()

pivot_vol_time.columns = ['{}_{}'.format('min_vol', re.sub(r'^time_en_air_minute_', '', str(col[1])).split()[0]) if col[1] else col[0] for col in pivot_vol_time.columns]
pivot_vol_time = pivot_vol_time.rename(columns={'min_vol_NaT':'ref_aero'})

df = df.merge(pivot_vol_time, on='ref_aero', how='left')

pivot_deg = pivot_deg.rename(columns={'compo_concerned':'ref_compo'})

df=df.merge(pivot_deg, on=['ref_aero', 'ref_compo'], how='left')


columns = df.columns

date_column_tuples = [(re.search(r'\d{4}-\d{2}-\d{2}', col).group(), col) for col in columns[8:] if re.search(r'\d{4}-\d{2}-\d{2}', col)]

sorted_date_column_tuples = sorted(date_column_tuples, key=lambda x: x[0])

sorted_columns = [col[1] for col in sorted_date_column_tuples]

desired_order = ['type_model', 'ref_aero', 'debut_service', 'last_maint', 'end_maint', 'ref_compo', 'cout_composant', 'lifespan']
sorted_columns = desired_order + sorted_columns

df_sorted = df[sorted_columns]

df_sorted2.iloc[:, 8:] = df_sorted2.iloc[:, 8:].fillna(0)


df_sorted2.to_csv('squelette_df.csv',index=False)
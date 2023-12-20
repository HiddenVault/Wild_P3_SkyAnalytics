import moduleProcessing
import pandas as pd

# Traitement du dataset degradations_FULL
print(f"\n🚀 Traitement du dataset degradations", end='')
link = 'data/preprocessing/degradations_FULL.csv'
df = pd.read_csv(link)
df = moduleProcessing.drop_duplicates(df, original_column = 'compo_concerned', inplace = 'true')
df = moduleProcessing.create_column_id(df, prefix= 'degradations', original_column = 'measure_day', target_column = 'id')
df = moduleProcessing.rename_column(df, original_column= 'linked_aero', renamed_column = 'ref_aero')
df = moduleProcessing.rename_column(df, original_column= 'usure_cumulée', renamed_column = 'usure_cumulee')
df = moduleProcessing.change_column_type(df, original_column = 'measure_day', new_type = 'datetime64[ns]')
df = moduleProcessing.convert_date(df, original_column = 'measure_day')
df = moduleProcessing.rounded_column(df, original_column= 'usure_cumulee', rounded_value = 'sup')
df.to_csv('data/processing/degradations.csv', index=False)

# Traitement du dataset logs_vols_FULL
print(f"\n🚀 Traitement du dataset logs_vols", end='')
link = 'data/preprocessing/logs_vols_FULL.csv'
df = pd.read_csv(link)
df = moduleProcessing.create_column_id(df, 'logs_vols', 'jour_vol', 'id')
df = moduleProcessing.split_column_json(df, column_name = 'sensor_data')
df = moduleProcessing.rename_column(df, original_column= 'aero_linked', renamed_column = 'ref_aero')
df = moduleProcessing.change_column_type(df, original_column = 'jour_vol', new_type = 'datetime64[ns]')
df = moduleProcessing.convert_date(df, original_column = 'jour_vol')
df.to_csv('data/processing/logs_vols.csv', index=False)

# Traitement du dataset composant
print(f"\n🚀 Traitement du dataset composants", end='')
link = 'data/incoming/composants_2023-11-07.csv'
df = pd.read_csv(link)
df = moduleProcessing.rename_column(df, original_column= 'aero', renamed_column = 'ref_aero')
df = moduleProcessing.rename_column(df, original_column= 'desc', renamed_column = 'descr')
df = moduleProcessing.rounded_column(df, original_column= 'taux_usure_actuel', rounded_value = 'sup')
df.to_csv('data/processing/composants.csv', index=False)

# Traitement du dataset aeronefs
print(f"\n🚀 Traitement du dataset aeronefs", end='')  
link = 'data/incoming/aeronefs_2023-11-07.csv'
df = pd.read_csv(link)
df = moduleProcessing.create_column(df, created_column= 'img', position_column = 2, default_value = 'none.png')
df = moduleProcessing.change_column_type(df, original_column = 'debut_service', new_type = 'datetime64[ns]')
df = moduleProcessing.convert_date(df, original_column = 'debut_service')
df = moduleProcessing.change_column_type(df, original_column = 'last_maint', new_type = 'datetime64[ns]')
df = moduleProcessing.convert_date(df, original_column = 'last_maint')
df = moduleProcessing.change_column_type(df, original_column = 'end_maint', new_type = 'datetime64[ns]')
df = moduleProcessing.convert_date(df, original_column = 'end_maint')
df = moduleProcessing.fillna_column(df, original_column = 'end_maint', type_NaN = 'NaT')
df.to_csv('data/processing/aeronefs.csv', index=False)

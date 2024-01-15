import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import datetime as dt
import math
import re

# chargement des fichiers
aero = pd.read_csv('aeronefs.csv')
compo = pd.read_csv('composants.csv')
deg = pd.read_csv('degradations.csv')
vol = pd.read_csv('vols.csv')

# transformation au format date
aero['debut_service'] = pd.to_datetime(aero['debut_service'])
aero['last_maint'] = pd.to_datetime(aero['last_maint'])
aero['end_maint'] = pd.to_datetime(aero['end_maint'],errors='coerce')
aero['end_maint'] = aero['end_maint'].dt.strftime('%Y-%m-%d')
aero['end_maint'] = pd.to_datetime(aero['end_maint'],errors='coerce')
deg['measure_day'] = pd.to_datetime(deg['measure_day'])
vol['jour_vol'] = pd.to_datetime(vol['jour_vol'])

# création du df maitre
maitre = aero.merge(compo, on='ref_aero', how ='left')

# travail sur le df degradation pour préparer la création des colonnes
pivot_deg = deg.pivot_table(
    values='usure_nouvelle',
    index=['ref_aero','ref_compo'],
    columns=['measure_day'],
    aggfunc='sum',
    fill_value=0
).reset_index()

# fonction pour créer les colonnes de mesure/jour et classement
def remove_time_from_date(date_str):
    return re.sub(r'\s00:00:00$', '', str(date_str))

pivot_deg.columns = pivot_deg.columns[:2].tolist() + ['usure_' + remove_time_from_date(col) if col != 'mesure_day' else str(col) for col in pivot_deg.columns[2:]]

# merge sur le maitre
maitre = maitre.merge(pivot_deg, on=['ref_aero', 'ref_compo'], how='left')

# même travail sur le df log vol
pivot_vol_time = vol.pivot_table(
    index='ref_aero',
    columns=['jour_vol'],
    values=['time_en_air'],
    aggfunc='sum',
    fill_value=0,  
).reset_index()

pivot_vol_time.columns = ['{}_{}'.format('h_vol', re.sub(r'^time_en_air_', '', str(col[1])).split()[0]) if col[1] else col[0] for col in pivot_vol_time.columns]
pivot_vol_time = pivot_vol_time.rename(columns={'h_vol_NaT':'ref_aero'})

# merge avec le maitre
maitre = maitre.merge(pivot_vol_time, on='ref_aero', how='left')

# travail sur le maitre pour organiser les colonnes
columns = maitre.columns

# Extraire les dates de chaque colonne et créer une liste de tuples (date, colonne)
date_column_tuples = [(re.search(r'\d{4}-\d{2}-\d{2}', col).group(), col) for col in columns[8:] if re.search(r'\d{4}-\d{2}-\d{2}', col)]

# Trier la liste de tuples par date
sorted_date_column_tuples = sorted(date_column_tuples, key=lambda x: x[0])

# Extraire la liste triée des colonnes
sorted_columns = [col[1] for col in sorted_date_column_tuples]

# Réorganiser les colonnes selon la spécification demandée
desired_order = ['type_model', 'ref_aero', 'debut_service', 'last_maint', 'end_maint', 'ref_compo', 'categorie','cout', 'lifespan']
sorted_columns = desired_order + sorted_columns

# Créer un DataFrame avec les colonnes triées
maitre = maitre[sorted_columns]

# mise au format csv
pmaitre = maitre.to_csv('maitre.csv',index=False)
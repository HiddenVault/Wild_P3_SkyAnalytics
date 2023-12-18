import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import datetime as dt
import math

# modification des noms de colonne servant de clés, la base gardé étant le nom du csv 'aeronefs' à savoir 'ref_aero'
colonne_ref = 'ref_aero'
df_logs_vols = df_logs_vols.rename(columns={'aero_linked':colonne_ref})
df_degradations = df_degradations.rename(columns={'linked_aero':colonne_ref})
df_composants = df_composants.rename(columns={'aero':colonne_ref})

# application d'un arrondi sur la colonne 'taux_usure_actuel' du csv composants, 
# idem pour la colonne 'usure_cumulée' du csv dégradations
df_composants['taux_usure_actuel'] = df_composants['taux_usure_actuel'].apply(lambda x: math.ceil(x * 100) / 100)
df_degradations['usure_cumulée'] = df_degradations['usure_cumulée'].apply(lambda x: math.ceil(x * 100) / 100)

# suppressions des duplications de lignes dans le csv dégradations sur les valeurs de la colonne 'compo_concerned' ~2%
df_degradations.drop_duplicates(subset=['compo_concerned'], inplace=True)

# transformation de la colonne 'time_en_air' du csv logs_vols au format minutes
df_logs_vols['time_en_air']= df_logs_vols['time_en_air'].apply(lambda x: x*60)

# mise au format date des colonnes concernées
df_logs_vols['jour_vol'] = pd.to_datetime(df_logs_vols['jour_vol']).dt.date
df_degradations['measure_day'] = pd.to_datetime(df_degradations['measure_day'])
df_aeronefs['end_maint'] = df_aeronefs['end_maint'].map(lambda x: pd.to_datetime(x,errors='coerce').date()).fillna('NaT')
df_aeronefs['debut_service'] = pd.to_datetime(df_aeronefs['debut_service'])
df_aeronefs['last_maint'] = pd.to_datetime(df_aeronefs['last_maint'])
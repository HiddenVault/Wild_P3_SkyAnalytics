import pandas as pd
import datetime as dt
import numpy as np
import math
import ast

#chargement des df
aero = pd.read_csv('aeronefs_2023-11-07.csv')
compo = pd.read_csv('composants_2023-11-07.csv')
deg = pd.read_csv('degradations_2023-11-07.csv')
vol = pd.read_csv('logs_vols_2023-11-07.csv')

# passage au type datetime des df
aero['debut_service'] = pd.to_datetime(aero['debut_service'])
aero['last_maint'] = pd.to_datetime(aero['last_maint'])
aero['end_maint'] = pd.to_datetime(aero['end_maint'],errors='coerce')
aero['end_maint'] = aero['end_maint'].dt.strftime('%Y-%m-%d')
aero['end_maint'] = pd.to_datetime(aero['end_maint'],errors='coerce')
deg['measure_day'] = pd.to_datetime(deg['measure_day'])
vol['jour_vol'] = pd.to_datetime(vol['jour_vol'])

# modification des noms de colonne servant de clés, la base gardé étant le nom du csv 'aeronefs' à savoir 'ref_aero'
colonne_ref = 'ref_aero'
vol = vol.rename(columns={'aero_linked':colonne_ref})
deg = deg.rename(columns={'linked_aero':colonne_ref})
compo = compo.rename(columns={'aero':colonne_ref})

# modif noms de colonne des composants
deg = deg.rename(columns={'compo_concerned':'ref_compo'})

# application d'un arrondi sur les floats 
compo['taux_usure_actuel'] = compo['taux_usure_actuel'].apply(lambda x: math.ceil(x * 100) / 100)
deg['usure_nouvelle'] = deg['usure_nouvelle'].apply(lambda x: math.ceil(x * 100) / 100)

# !!! Peut être temporaire !!!
# drop de l'avion B737_4325 qui est en double dans la table aeronef et donc génère des doublons dans les autres tables.
# la correction est possible mais on ne peut pas l'automatisé car il n'exitse pas de colonne dernier vol dans les df que l'on récupère
# Liste des DataFrames
list_of_dataframes = [aero, compo, deg, vol]

# Boucle pour itérer sur chaque DataFrame
for df in list_of_dataframes:
    
    indices_a_supprimer = df[df['ref_aero'] == 'B737_4325'].index
    
    df.drop(indices_a_supprimer, inplace=True)
# !!! Peut être temporaire !!!
    
# Fonction pour extraire les valeurs des champs
def extract_sensor_data(sensor_data, field):
    try:
        sensor_data_dict = ast.literal_eval(sensor_data)
        return sensor_data_dict[field]
    except (ValueError, KeyError):
        return None

# Appliquer la fonction pour extraire les valeurs
vol['temp_°C'] = vol['sensor_data'].apply(lambda x: extract_sensor_data(x, 'temp').replace('°C', '') if extract_sensor_data(x, 'temp') else None)
vol['pressure_hPa'] = vol['sensor_data'].apply(lambda x: extract_sensor_data(x, 'pressure').replace('hPa', '') if extract_sensor_data(x, 'pressure') else None)
vol['vibration_m/s²'] = vol['sensor_data'].apply(lambda x: round(float(extract_sensor_data(x, 'vibrations').split()[0]), 2) if extract_sensor_data(x, 'vibrations') else None)


# transformation des format en float et suppression de sensor_data
vol = vol.drop('sensor_data', axis=1)
vol['temp_°C'] = vol['temp_°C'].astype(float)
vol['pressure_hPa'] = vol['pressure_hPa'].astype(float)

# print en csv des tables modifié
p_aero = aero.to_csv('aeronefs.csv',index=False)
p_compo = compo.to_csv('composants.csv',index=False)
p_deg = deg.to_csv('degradations.csv',index=False)
p_vol = vol.to_csv('vols.csv',index=False)
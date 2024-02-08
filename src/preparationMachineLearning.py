# Importation des bibliothèques nécessaires
import pandas as pd
import numpy as np
import os
import moduleConnection  # Module personnalisé pour la connexion à la base de données
from datetime import datetime
import re  # Module pour les expressions régulières
import math  # Module pour les opérations mathématiques

print("Importation des bibliothèques terminée.")

# Importation des données depuis la base de données et création des DataFrames

print("Connexion à la base de données...")
# Connexion à la base de données en utilisant les variables d'environnement
env_path = os.path.join(os.path.dirname(__file__), 'env', 'adm.env')
moduleConnection.load_environment(env_path)

db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")

connection = moduleConnection.get_db_connection(db_host, db_user, db_password, db_name)

# Définissez une fonction pour charger les données
def load_sql(table_name, connection):
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql_query(query, connection)

# Utilisation de la fonction
connection = moduleConnection.get_db_connection(db_host, db_user, db_password, db_name)
aero = load_sql("aeronefs", connection)
compo = load_sql("composants", connection)
deg = load_sql("degradations", connection)
vol = load_sql("logs_vols", connection)
connection.close()


print("Toutes les données ont été récupérées depuis la base de données.")

# Préparation des données

print("Préparation des données...")

# Conversion des colonnes de dates en type datetime
aero['debut_service'] = pd.to_datetime(aero['debut_service'])
aero['last_maint'] = pd.to_datetime(aero['last_maint'])
aero['end_maint'] = pd.to_datetime(aero['end_maint'], errors='coerce')
aero['end_maint'] = aero['end_maint'].dt.strftime('%Y-%m-%d')
aero['end_maint'] = pd.to_datetime(aero['end_maint'], errors='coerce')
deg['measure_day'] = pd.to_datetime(deg['measure_day'])
vol['jour_vol'] = pd.to_datetime(vol['jour_vol'])
compo = compo.rename(columns={'descr': 'desc'})

# Renommer les colonnes servant de clés
colonne_ref = 'ref_aero'
vol = vol.rename(columns={'aero_linked': colonne_ref})
deg = deg.rename(columns={'linked_aero': colonne_ref})
compo = compo.rename(columns={'aero': colonne_ref})

# Renommer les colonnes des composants
deg = deg.rename(columns={'compo_concerned': 'ref_compo'})

# Arrondir les valeurs float
compo['taux_usure_actuel'] = compo['taux_usure_actuel'].apply(lambda x: math.ceil(x * 100) / 100)
deg['usure_nouvelle'] = deg['usure_nouvelle'].apply(lambda x: math.ceil(x * 100) / 100)

# Suppression des doublons pour l'avion B737_4325
print("Suppression des doublons pour l'avion B737_4325...")
list_of_dataframes = [aero, compo, deg, vol]
for df in list_of_dataframes:
    indices_a_supprimer = df[df['ref_aero'] == 'B737_4325'].index
    df.drop(indices_a_supprimer, inplace=True)

# Sélection des colonnes utiles
print("Sélection des colonnes utiles...")
compo = compo.drop(columns=['desc'], axis=1)
deg = deg[['ref_aero', 'ref_compo', 'usure_nouvelle', 'measure_day']]
vol = vol[['ref_aero', 'jour_vol', 'time_en_air', 'etat_voyant']]

# Création du DataFrame maître
print("Création du DataFrame maître...")
maitre = aero.merge(compo, on='ref_aero', how='left')

# Transformation des données pour les dégradations
print("Transformation des données pour les dégradations...")
pivot_deg = deg.pivot_table(
    values='usure_nouvelle',
    index=['ref_aero', 'ref_compo'],
    columns=['measure_day'],
    aggfunc='first'
).reset_index()

# Suppression du temps des dates et réorganisation des colonnes
def remove_time_from_date(date_str):
    return re.sub(r'\s00:00:00$', '', str(date_str))

pivot_deg.columns = pivot_deg.columns[:2].tolist() + ['usure_' + remove_time_from_date(col) if col != 'measure_day' else str(col) for col in pivot_deg.columns[2:]]

# Remplissage des valeurs NaN avec les valeurs suivantes
print("Remplissage des valeurs NaN avec les valeurs suivantes...")
for t in range(33):
    for i in range(len(pivot_deg.columns) - 1, 3, -1):
        nom_colonne = pivot_deg.columns[i]
        if pivot_deg[nom_colonne].isnull().any():
            j = i - 1
            while j >= 0 and pivot_deg[pivot_deg.columns[j]].isnull().all():
                j -= 1
            if j >= 0:
                pivot_deg.loc[pivot_deg[nom_colonne].isnull(), nom_colonne] = pivot_deg[pivot_deg.columns[j]]

for t in range(35):
    for i in range(2, len(pivot_deg.columns)):
        nom_colonne = pivot_deg.columns[i]
        if pivot_deg[nom_colonne].isnull().any():
            j = i + 1
            while j < len(pivot_deg.columns) and pivot_deg[pivot_deg.columns[j]].isnull().all():
                j += 1
            if j < len(pivot_deg.columns):
                pivot_deg.loc[pivot_deg[nom_colonne].isnull(), nom_colonne] = pivot_deg[pivot_deg.columns[j]]

# Calcul de l'évolution des usures
print("Calcul de l'évolution des usures...")
mesure_columns = [col for col in pivot_deg.columns if col.startswith('usure')]
evolution_df = pd.DataFrame()
for i in range(1, len(mesure_columns)):
    evolution_col_name = f"evolution_{mesure_columns[i]}"
    evolution_df[evolution_col_name] = pivot_deg[mesure_columns[i]] - pivot_deg[mesure_columns[i - 1]]
    evolution_df.loc[pivot_deg[mesure_columns[i]] == 0, evolution_col_name] = pd.NA
    evolution_df.loc[pivot_deg[mesure_columns[i - 1]] == 0, evolution_col_name] = pd.NA

pivot_deg = pd.concat([pivot_deg, evolution_df], axis=1)

# Réorganisation des colonnes par date croissante
print("Réorganisation des colonnes par date croissante...")
colonnes_triees = sorted(pivot_deg.columns[2:], key=lambda x: pd.to_datetime(x.split('_')[-1]))
colonnes_ordre = pivot_deg.columns[:2].tolist() + colonnes_triees
pivot_deg = pivot_deg[colonnes_ordre]

# Fusion avec le DataFrame maître
print("Fusion avec le DataFrame maître...")
maitre = maitre.merge(pivot_deg, on=['ref_aero', 'ref_compo'], how='left')

# Création de colonnes pivot pour les logs de vol
print("Création de colonnes pivot pour les logs de vol...")
def create_pivot_column(df, variable):
    pivot_result = df.pivot_table(
        index='ref_aero',
        columns=['jour_vol'],
        values=[variable],
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    pivot_result.columns = ['{}_{}'.format(variable, re.sub(r'^{}_'.format(variable), '', str(col[1])).split()[0]) if col[1] else col[0] for col in pivot_result.columns]
    pivot_result = pivot_result.rename(columns={'{}_NaT'.format(variable): 'ref_aero'})
    return pivot_result

variables = ['etat_voyant', 'time_en_air']
for variable in variables:
    pivot_result = create_pivot_column(vol, variable)
    vol = pd.merge(vol, pivot_result, on='ref_aero', how='left')
    vol = vol.drop([variable], axis=1)

vol = vol.drop('jour_vol', axis=1)

# Fusion avec le DataFrame maître
print("Fusion avec le DataFrame maître...")
maitre = maitre.merge(vol, on=['ref_aero'], how='left')

# Suppression des doublons
print("Suppression des doublons...")
maitre = maitre.drop_duplicates(subset='ref_compo')

# Suppression de la colonne 'taux_usure_actuel'
print("Suppression de la colonne 'taux_usure_actuel'...")
maitre = maitre.drop("taux_usure_actuel", axis=1)

def deplacer_colonne(df, nom_colonne, nouvelle_position):
    # Vérification de l'existence de la colonne
    if nom_colonne in df.columns:
        # Suppression de la colonne du DataFrame
        col = df[nom_colonne]
        df = df.drop(columns=[nom_colonne])

        # Insertion de la colonne à la nouvelle position
        df.insert(nouvelle_position, nom_colonne, col)

        print("Colonne déplacée avec succès.")
    else:
        print("La colonne spécifiée n'existe pas dans le DataFrame.")

    return df

# Calcul de l'âge de l'avion
print("Calcul de l'âge de l'avion...")
date_actuelle = datetime.now()
maitre['age_avion'] = (date_actuelle - maitre['debut_service']).dt.days

# Déplacement de la colonne 'age_avion'
print("Déplacement de la colonne 'age_avion'...")
maitre = deplacer_colonne(maitre, 'age_avion', 3)

# Calcul des taux d'usure
print("Calcul des taux d'usure...")
colonnes_time_en_air = [colonne for colonne in maitre.columns if colonne.startswith("time_en_air_")]
for colonne_time_en_air in colonnes_time_en_air:
    date = colonne_time_en_air.split("_")[-1]
    if maitre[colonne_time_en_air].iloc[0] > 0:
        colonne_evolution_usure = f"evolution_usure_{date}"
        taux_usure = maitre[colonne_evolution_usure] / maitre[colonne_time_en_air]
        colonne_taux_usure = f"taux_usure_{date}"
        maitre[colonne_taux_usure] = taux_usure

# Réorganisation des colonnes
print("Réorganisation des colonnes...")
columns = maitre.columns
date_column_tuples = [(re.search(r'\d{4}-\d{2}-\d{2}', col).group(), col) for col in columns[8:] if re.search(r'\d{4}-\d{2}-\d{2}', col)]
sorted_date_column_tuples = sorted(date_column_tuples, key=lambda x: x[0])
sorted_columns = [col[1] for col in sorted_date_column_tuples]
desired_order = ['type_model', 'ref_aero', 'debut_service', 'age_avion', 'last_maint', 'end_maint', 'ref_compo', 'categorie', 'cout', 'lifespan']
sorted_columns = desired_order + sorted_columns
maitre = maitre[sorted_columns]

# Enregistrement des données dans un fichier CSV
csv_filename = "data/extract/maitre.csv"
maitre.to_csv(csv_filename, index=False)
print(f"Résultats enregistrés dans {csv_filename}")



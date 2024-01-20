import warnings
import pandas as pd
import mysql.connector
import moduleProcessing
import moduleConnection
from dotenv import load_dotenv
import os # Pour la gestion des fichiers et des répertoires
import requests # Pour effectuer des requêtes HTTP
from datetime import datetime, timedelta # Pour manipuler les dates
import json
import ast

warnings.filterwarnings("ignore", category=UserWarning)

'''
Explications :
La fonction file_download_batch prend en entrée :
    - start_date : Date de début
    - path : URL où les fichiers sont stockés
    - file_name : Nom du fichier à télécharger
    - storage_folder : Dossier de stockage local

Etapes du script :
    - Téléchargement des fichiers depuis un chemin de base, 
    - Les fichiers sont stockés localement, 
    - Affichage d'un message en cas de réussite ou d'échec pour chaque téléchargement. 

Exemple :
file_download(start_date = '2023-10-29', path = 'https://sc-e.fr/docs/', file_name = 'logs_vols_AAAA-MM-JJ.csv', storage_folder = 'incoming')
'''
def file_download_batch(start_date, path, file_name, storage_folder):
    print()
    print(f"💾 Téléchargement des fichiers {file_name}.")

    # Vérification de l'existance du dossier
    if not os.path.exists(storage_folder):
        # Si le dossier est absent, on le crée
        os.makedirs(storage_folder)

    # Conversion de la date de début
    # La date est contenue dans le nom du fichier au format 'AAAA-MM-JJ') 
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    
    # Conversion de la date de fin (remplacez 'YYYY-MM-DD' par la date de fin souhaitée)
    end_date = datetime.now()

    # Création d'une liste pour stocker les chemins des fichiers téléchargés
    downloaded_files = []

    # Établissement de la connexion à la base de données
    env_path = os.path.join(os.path.dirname(__file__), 'env', 'adm.env')
    moduleConnection.load_environment(env_path) 
    
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")  
    
    connection = moduleConnection.get_db_connection(db_host, db_user, db_password, db_name)
    cursor = connection.cursor()

    # Tant que la date actuelle est supérieure ou égale à la date de début
    while start_date <= end_date:
        # Construction du nom du fichier complet en remplaçant 'AAAA-MM-JJ' par la date formatée
        # La date contenue dans start_date est formatée en une chaîne de caractères au format 'AAAA-MM-JJ'. 
        # Explications : si start_date est le 29 octobre 2023, alors date_format deviendra la chaîne de caractères "2023-10-29". 
        # Utilisation de strftime pour formater la date selon le modèle spécifié.        
        date_format = start_date.strftime('%Y-%m-%d')
        # Substitution de la date formatée à la place de 'AAAA-MM-JJ' dans le nom du fichier. 
        # Explications : si file_name est "logs_vols_AAAA-MM-JJ.csv" , alors file_name_complete deviendra "logs_vols_2023-10-29.csv". 
        file_name_complete = file_name.replace('AAAA-MM-JJ', date_format)
        # Construction de l'URL complète en combinant l'URL et le nom du fichier complet
        url = os.path.join(path, file_name_complete)
        # Téléchargement du fichier depuis l'URL
        response = requests.get(url)

        # Vérification de la réponse du serveur (status_code == 200 signifie que la requête a réussi)        
        if response.status_code == 200:
            # Création du chemin complet du fichier de destination en combinant le dossier de stockage et le nom du fichier complet
            path_destination = os.path.join(storage_folder, file_name_complete)

            # Enregistrement du fichier téléchargé localement dans le dossier de stockage
            # Ouverture du fichier en mode write binary (wb) 
            with open(path_destination, 'wb') as fichier_destination:
                fichier_destination.write(response.content)
            
            if os.path.exists(path_destination):
                # Vérification de l'existence du fichier dans la base de données
                if not file_in_database(cursor, path_destination):
                    process_and_insert_data(cursor, path_destination, connection)
                else:
                    # Si le fichier existe mais n'est pas intégré
                    if file_not_integrated(cursor, path_destination):
                        print(f"ℹ️ Tentative de réintégration pour la journée {path_destination}.")
                        filename_without_extension = os.path.splitext(file_name_complete)[0]
                        delete_rows(connection, table_name = 'files', column_name = 'id', value_to_delete = filename_without_extension)
                        process_and_insert_data(cursor, path_destination, connection)
                    else:
                        print(f"ℹ️ Le fichier {path_destination} est déjà intégré dans la base de données.")
        
        # Passage à la date suivante en ajoutant un jour
        start_date = start_date + timedelta(days=1)

    connection.commit()
    cursor.close()
    connection.close()

'''
Explications :
La fonction process_and_insert_data prend en entrée :
    - cursor : L'objet cursor permet d'exécuter des commandes SQL dans la base de données.
    - file_path : Chemin complet du fichier à traiter
    - connection : objet de connexion à la base de données MySQL

Etapes du script :
    - Lecture des données d'un fichier CSV
    - Stockage dans un dataframe
    - Préparation des données
    - Insertion dans la base de données avec SQL

Exemple :
process_and_insert_data(cursor, path_destination, connection)
'''

def process_and_insert_data(cursor, file_path, connection):
    # Lecture du fichier CSV
    df = pd.read_csv(file_path)

    # Traitement des fichiers 'logs_vols'.
    if 'logs_vols' in file_path:
        # Extraction du nom du fichier sans extension.
        file_name_with_extension = os.path.basename(file_path)
        file_name, file_extension = os.path.splitext(file_name_with_extension)

        # Insertion des informations du fichier dans la table 'files'.
        table_name = "files"
        insert_file_query = f"INSERT INTO {table_name} (id, integre) VALUES (%s, %s)"
        data_to_insert_file = [(file_name, 0)]
        cursor.executemany(insert_file_query, data_to_insert_file)

        # Traitement des données du fichier logs_vols
        df = moduleProcessing.create_column_id(df, 'logs_vols', 'jour_vol', 'id')
        df = moduleProcessing.split_column_json(df, column_name='sensor_data',column_copy='copy')
        df = moduleProcessing.rename_column(df, original_column='aero_linked', renamed_column='ref_aero')
        df = moduleProcessing.change_column_type(df, original_column='jour_vol', new_type='datetime64[ns]')
        df = moduleProcessing.convert_date(df, original_column='jour_vol')
        
        # Conversion des types de données pour certaines colonnes.
        df['time_en_air'] = df['time_en_air'].astype(float)
        df['temp_C'] = df['temp_C'].astype(float)
        df['pressure_hPa'] = df['pressure_hPa'].astype(float)
        df['vibrations_ms2'] = df['vibrations_ms2'].astype(float)
        df['etat_voyant'] = df['etat_voyant'].astype(int)
        df = df.where(pd.notna(df), None)

        # Mise à jour de l'état d'intégration du fichier dans la table 'files'.
        update_file_query = f"UPDATE {table_name} SET integre = 1 WHERE id = %s AND integre = 0"
        cursor.execute(update_file_query, (file_name,))

        # Insertion des données traitées dans la table 'logs_vols'.
        table_name = "logs_vols"
        insert_query = f"INSERT INTO {table_name} (id, ref_vol, ref_aero, jour_vol, time_en_air, sensor_data, etat_voyant, temp_C, pressure_hPa, vibrations_ms2) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        data_to_insert = df.values.tolist()
        cursor.executemany(insert_query, data_to_insert)

    # Traitement des fichiers 'degradations'.
    elif 'degradations' in file_path:
        # Extraction du nom du fichier sans extension.
        file_name_with_extension = os.path.basename(file_path)
        file_name, file_extension = os.path.splitext(file_name_with_extension)

        # Insertion des informations du fichier dans la table 'files'.
        table_name = "files"
        insert_file_query = f"INSERT INTO {table_name} (id, integre) VALUES (%s, %s)"
        data_to_insert_file = [(file_name, 0)]
        cursor.executemany(insert_file_query, data_to_insert_file)

        # Traitement des données spécifiques aux fichiers de dégradations.
        #df['need_replacement'] = df['need_replacement'].replace({False: 0, True: 1}, inplace=True)
        df['id'] = file_name
        target_column_series = df.pop('id')
        df.insert(0, "id", target_column_series)
        #df = moduleProcessing.create_column_id(df, prefix='degradations', original_column='measure_day', target_column='id')
        df = moduleProcessing.drop_duplicates(df, original_column='compo_concerned', inplace=True)
        df = moduleProcessing.rename_column(df, original_column='linked_aero', renamed_column='ref_aero')
        df = moduleProcessing.change_column_type(df, original_column='measure_day', new_type='datetime64[ns]')
        df = moduleProcessing.convert_date(df, original_column='measure_day')
        df = moduleProcessing.rounded_column(df, original_column='usure_nouvelle', rounded_value='sup')

        # Conversion des types de données pour certaines colonnes.
        df['usure_nouvelle'] = df['usure_nouvelle'].astype(float)
        df = df.where(pd.notna(df), None)

        # Mise à jour de l'état d'intégration du fichier dans la table 'files'.
        update_file_query = f"UPDATE {table_name} SET integre = 1 WHERE id = %s AND integre = 0"
        cursor.execute(update_file_query, (file_name,))

        # Insertion des données traitées dans la table 'degradations'.
        table_name = "degradations"
        insert_query = f"INSERT INTO {table_name} (id, ref_deg, ref_aero, compo_concerned, usure_nouvelle, measure_day, need_replacement) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        data_to_insert = df.values.tolist()     

        cursor.executemany(insert_query, data_to_insert)

    # Traitement des fichiers 'aeronefs'.
    elif 'aeronefs' in file_path:
        # Extraction du nom du fichier sans extension.
        file_name_with_extension = os.path.basename(file_path)
        file_name, file_extension = os.path.splitext(file_name_with_extension)

        # Insertion des informations du fichier dans la table 'files'.
        table_name_integrate = "files"
        insert_file_query = f"INSERT INTO {table_name_integrate} (id, integre) VALUES (%s, %s)"
        data_to_insert_file = [(file_name, 0)]
        cursor.executemany(insert_file_query, data_to_insert_file)

        # Traitement des données spécifiques aux fichiers 'aeronefs'.
        df = moduleProcessing.create_column(df, created_column='img', position_column=2, default_value='none.png')
        df = moduleProcessing.change_column_type(df, original_column='debut_service', new_type='datetime64[ns]')
        df = moduleProcessing.convert_date(df, original_column='debut_service')
        df = moduleProcessing.change_column_type(df, original_column='last_maint', new_type='datetime64[ns]')
        df = moduleProcessing.convert_date(df, original_column='last_maint')
        df = moduleProcessing.change_column_type(df, original_column='end_maint', new_type='datetime64[ns]')
        df = moduleProcessing.convert_date(df, original_column='end_maint')
        df = moduleProcessing.fillna_column(df, original_column='end_maint', type_NaN='NaT')

        # Conversion des types de données pour certaines colonnes.
        df = df.where(pd.notna(df), None)

        table_name = "aeronefs"

        # Parcours du fichier .csv
        for index, row in df.iterrows():
            ref_aero = row['ref_aero']

            # On vérifie si l'enregistrement existe
            query = f"SELECT * FROM {table_name} WHERE ref_aero = '{ref_aero}'"
            existing = pd.read_sql(query, connection)

            if not existing.empty:
                # Mise à jour de l'enregistrement
                update_query = ", ".join([f"{col} = %s" for col in df.columns])
                values = [row[col] for col in df.columns]
                cursor.execute(f"UPDATE {table_name} SET {update_query} WHERE ref_aero = %s", values + [ref_aero])
                print(f"Mise à jour de l'appareil existant = {ref_aero}")
            else:
                # Insértion du nouvel enregistrement
                insert_query = f"INSERT INTO {table_name} (ref_aero, type_model, img, debut_service, last_maint, en_maintenance, end_maint) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                values = [row[col] for col in df.columns]
                cursor.execute(insert_query, values)
                print(f"Insertion d'un nouvel appareil = {ref_aero}")

        # Mise à jour de l'état d'intégration du fichier dans la table 'files'.
        update_file_query = f"UPDATE {table_name_integrate} SET integre = 1 WHERE id = %s AND integre = 0"
        cursor.execute(update_file_query, (file_name,))

    # Traitement des fichiers 'composants'.
    elif 'composants' in file_path:
        # Extraction du nom du fichier sans extension.
        file_name_with_extension = os.path.basename(file_path)
        file_name, file_extension = os.path.splitext(file_name_with_extension)

        # Insertion des informations du fichier dans la table 'files'.
        table_name_integrate = "files"
        insert_file_query = f"INSERT INTO {table_name_integrate} (id, integre) VALUES (%s, %s)"
        data_to_insert_file = [(file_name, 0)]
        cursor.executemany(insert_file_query, data_to_insert_file)

        # Traitement des données spécifiques aux fichiers 'composants'.
        df = moduleProcessing.rename_column(df, original_column= 'aero', renamed_column = 'ref_aero')
        df = moduleProcessing.rename_column(df, original_column= 'desc', renamed_column = 'descr')
        df = moduleProcessing.rounded_column(df, original_column= 'taux_usure_actuel', rounded_value = 'sup')

        # Conversion des types de données pour certaines colonnes.
        df = df.where(pd.notna(df), None)

        table_name = "composants"

        # Parcours du fichier .csv
        for index, row in df.iterrows():
            ref_compo = row['ref_compo']

            # On vérifie si l'enregistrement existe
            query = f"SELECT * FROM {table_name} WHERE ref_compo = '{ref_compo}'"
            existing = pd.read_sql(query, connection)

            if not existing.empty:
                # Mise à jour de l'enregistrement
                update_query = ", ".join([f"{col} = %s" for col in df.columns])
                values = [row[col] for col in df.columns]
                cursor.execute(f"UPDATE {table_name} SET {update_query} WHERE ref_compo = %s", values + [ref_compo])
                print(f"Mise à jour du composant existant = {ref_compo}")
            else:
                # Insertion du nouvel enregistrement
                insert_query = f"INSERT INTO {table_name} (ref_compo, categorie, ref_aero, descr, lifespan, taux_usure_actuel, cout) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                values = [row[col] for col in df.columns]
                cursor.execute(insert_query, values)
                print(f"Insertion du nouveau composant = {ref_compo}")

        # Mise à jour de l'état d'intégration du fichier dans la table 'files'.
        update_file_query = f"UPDATE {table_name_integrate} SET integre = 1 WHERE id = %s AND integre = 0"
        cursor.execute(update_file_query, (file_name,))


    # Validation de l'insertion des données dans la base de données.
    connection.commit()

'''
Explications :
La fonction file_in_database prend en entrée :
    - connection : objet de connexion à la base de données MySQL
    - file_path : Chemin complet du fichier à vérifier

Etapes du script :
    - Connnexion à la base de données
    - Comptage du nombre de ligne  
    - Retourne True si le fichier existe dans la base de données, False sinon.

Exemple :
file_in_database(cursor, path_destination)
'''

def file_in_database(cursor, file_path):
    # Extraction du nom du fichier (avec extension) à partir du chemin complet du fichier.
    filename_with_extension = os.path.basename(file_path)

    # Séparation du nom de fichier de son extension. 
    # La fonction os.path.splitext renvoie un tuple (nom du fichier, extension),
    # On prend le premier élément de ce tuple ([0]) pour obtenir le nom sans l'extension.
    filename_without_extension = os.path.splitext(filename_with_extension)[0]

    # Création de la requête SQL. 
    # %s est un paramètre qui sera remplacé par 'filename_without_extension'. 
    # Cette requête compte le nombre d'entrées dans la table 'files' 
    # où l'identifiant correspond au nom du fichier.
    query = "SELECT COUNT(*) FROM files WHERE id = %s"

    # Exécution de la requête SQL. Le nom du fichier (sans extension) est passé
    # comme paramètre pour remplacer %s dans la requête.
    cursor.execute(query, (filename_without_extension,))

    # Récupération du résultat de la requête. 
    # fetchone() renvoie le premier enregistrement de la requête, 
    # et [0] récupère le premier champ de cet enregistrement,
    # qui est le nombre de fois que le fichier apparaît dans la base de données.
    count = cursor.fetchone()[0]

    # Si count est supérieur à 0, cela signifie que le fichier existe déjà dans la base de données.
    # La fonction retourne donc True. 
    # Si count est 0, le fichier n'existe pas dans la base de données. La fonction retourne False.
    return count > 0

'''
Explications :
La fonction delete_rows prend en entrée :
    - connection : objet de connexion à la base de données MySQL
    - file_path : Chemin complet du fichier à vérifier

Etapes du script :
    - Connnexion à la base de données
    - Comptage du nombre de ligne  
    - Retourne True si le fichier n'est pas intégré, False sinon.

Exemple :
file_not_integrated(cursor, path_destination)
'''

# Importation du module os pour travailler avec des chemins de fichiers
import os

def file_not_integrated(cursor, file_path):
    # Extraction du nom de fichier avec l'extension à partir du chemin complet
    filename_with_extension = os.path.basename(file_path)

    # Extraction du nom de fichier sans l'extension
    filename_without_extension = os.path.splitext(filename_with_extension)[0]

    # Préparation de la requête SQL pour vérifier l'intégration du fichier.
    # La requête compte le nombre de fois où le fichier apparaît dans la table 'files'
    # avec un statut 'integre' égal à 0 (non intégré).
    query = "SELECT COUNT(*) FROM files WHERE id = %s AND integre = 0"

    # Exécution de la requête SQL avec le nom du fichier comme paramètre.
    cursor.execute(query, (filename_without_extension,))

    # Récupération du résultat de la requête (nombre de lignes correspondant au fichier).
    count = cursor.fetchone()[0]

    # Si count est supérieur à 0 alors le fichier n'est pas encore intégré.
    # La fonction retourne alors True. Sinon, elle retourne False.
    return count > 0

'''
Explications :
La fonction delete_rows prend en entrée :
    - connection : objet de connexion à la base de données MySQL
    - table_name : Nom de la table où les lignes seront supprimées.
    - column_name : Nom de la colonne utilisée pour la suppression.
    - value_to_delete : Valeur dans la colonne spécifiée qui détermine quelles lignes seront supprimées.

Etapes du script :
    - Connnexion à la base de données
    - Suppression de la ligne en fonction des arguments  
    - Affichage d'un message en cas de réussite ou d'échec 

Exemple :
delete_rows(connection, table_name = 'files', column_name = 'id', value_to_delete = filename_without_extension)
'''

def delete_rows(connection, table_name, column_name, value_to_delete):
    try:
        # Création d'un curseur à partir des informations de connexion
        cursor = connection.cursor()

        # Définition de la requête SQL pour supprimer les lignes. 
        # %s est un paramètre qui sera remplacé par la valeur 'value_to_delete'.
        query = f"DELETE FROM {table_name} WHERE {column_name} = %s"

        # Exécution de la requête SQL avec le paramètre 'value_to_delete'
        # value_to_delete, permet la création d'un tuple avec un seul élément
        cursor.execute(query, (value_to_delete,))

        # Validation de  la transaction pour que les modifications soient enregistrées dans la base de données
        connection.commit()

        # Message de succès
        print(f"Lignes supprimées avec succès de la table {table_name} où {column_name} = {value_to_delete}.")
    
    except mysql.connector.Error as error:
        # Message d'échec en cas de problème
        print(f"Erreur lors de la suppression des données : {error}")

    finally:
        # Fermeture de l'objet cursor (libération des ressources du serveur)
        cursor.close()


'''
Explications :
La fonction file_download prend en entrée :
    - path : URL où le fichier est stocké
    - file_name : Nom du fichier à télécharger
    - storage_folder : Dossier de stockage local

Etapes du script :
    - Téléchargement du fichier depuis un chemin de base, 
    - Le fichier est stocké localement, 
    - Affichage d'un message en cas de réussite ou d'échec pour chaque téléchargement. 

Exemple :
file_download(start_date = '2023-10-29', path = 'https://sc-e.fr/docs/', file_name = 'logs_vols_AAAA-MM-JJ.csv', storage_folder = 'incoming')
'''

def file_download(path, file_name, storage_folder):
    try:
        # Vérification de l'existance du dossier
        if not os.path.exists(storage_folder):
            os.makedirs(storage_folder)

        # Chemin complet du fichier de destination
        chemin_complet = os.path.join(storage_folder, file_name)

        # Téléchargement du fichier depuis l'URL
        reponse = requests.get(path)
        
        # Vérification de la réponse du serveur (status_code == 200 signifie que la requête a réussi)
        if reponse.status_code == 200:
            # Enregistrement du fichier téléchargé localement dans le dossier de stockage
            # Ouverture du fichier en mode write binary (wb) 
            with open(chemin_complet, 'wb') as fichier:
                fichier.write(reponse.content)
            # Message de succès
            print(f"✅ Fichier {file_name} téléchargé avec succès.")
        else:
            # Message d'échec
            print(f"❌ Échec du téléchargement du fichier {file_name}.")
    except Exception as e:
        # Message d'erreur
        print(f"❌ Une erreur s'est produite : {str(e)}")

'''
Explications :
La fonction concatenate_CSV prend en entrée :
    - start_date : Date de début du traitement
    - storage_folder : Dossier de stockage
    - file_name : Modèle de nom de fichier
    - outgoing_folder : Dossier de sortie
    - outgoing_file : Nom du fichier de sortie

Etapes du script :
    - Lecture des fichiers .CSV stockés localement
    - Concaténation des fichiers dans un même dataframe
    - Affichage d'un message en cas de réussite ou d'échec pour chaque téléchargement.

Exemple : concatenate_CSV(start_date = '2023-10-29', storage_folder = 'incoming', file_name = 'logs_vols_AAAA-MM-JJ.CSV', outgoing_folder = 'preprocessing', outgoing_file = 'logs_vols')    
'''

import os  # Pour la gestion des fichiers et des répertoires
import pandas as pd  # Pour travailler avec les dataframes
from datetime import datetime, timedelta  # Pour manipuler les dates

def concatenate_csv(start_date, outgoing_folder, file_name, storage_folder, outgoing_file):
    print()
    print(f"💾 Concaténation des fichiers {file_name}.")

    # Conversion de la date de début
    # La date est contenue dans le nom du fichier au format 'AAAA-MM-JJ') 
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    
    # Création d'un DataFrame vide
    final_dataframe = pd.DataFrame()
    
    # Tant que la date actuelle est supérieure ou égale à la date de début
    while start_date <= datetime.now():
        # Construction du nom du fichier complet en remplaçant 'AAAA-MM-JJ' par la date formatée
        # La date contenue dans start_date est formatée en une chaîne de caractères au format 'AAAA-MM-JJ'. 
        # Explications : si start_date est le 29 octobre 2023, alors date_format deviendra la chaîne de caractères "2023-10-29". 
        # Utilisation de strftime pour formater la date selon le modèle spécifié.
        date_format = start_date.strftime('%Y-%m-%d')
        # Substitution de la date formatée à la place de 'AAAA-MM-JJ' dans le nom du fichier. 
        # Explications : si file_name est "logs_vols_AAAA-MM-JJ.cssv" , alors file_name_complete deviendra "logs_vols_2023-10-29.csv". 
        file_name_complete = file_name.replace('AAAA-MM-JJ', date_format)
        
        # Construire le outgoing_folder complet du fichier en combinant le dossier de stockage
        # et le nom du fichier complet
        # Construction du chemin avec le dossier de stockage et le nom du fichier complet
        outgoing_folder_file = os.path.join(storage_folder, file_name_complete)
        
        # Vérification de l'existance du fichier
        if os.path.isfile(outgoing_folder_file):
            # Lecture du fichier CSV
            dataframe = pd.read_csv(outgoing_folder_file)
            
            # Vérification du type de fichier en comparant avec la variable 'outgoing_file'
            if outgoing_file in file_name_complete:
                # Concaténation du dataFrame lu avec le DataFrame final
                final_dataframe = pd.concat([final_dataframe, dataframe], ignore_index=True)
                print(f"✅ {file_name_complete} concaténé avec succès.")
            else:
                print(f"❌ {file_name_complete} n'est pas du type {outgoing_file}.")
        
        # Passage à la date suivante en ajoutant un jour
        start_date = start_date + timedelta(days=1)

    # Exportation du dataFrame final dans un fichier CSV
    file_name_sortie = f'{outgoing_file}_FULL.csv'
    outgoing_folder_sortie = os.path.join(outgoing_folder, file_name_sortie)

    # Vérification de l'existance du dossier
    if not os.path.exists(outgoing_folder):
        # Si le dossier est absent, on le crée
        os.makedirs(outgoing_folder)

    final_dataframe.to_csv(outgoing_folder_sortie, index=False)
    print(f"DataFrame final exporté dans {file_name_sortie}.")
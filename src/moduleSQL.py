import os
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd

'''
Explications :
La fonction delete_records prend en entrée :
    - connection : objet de connexion à la base de données MySQL
    - sql_folder : Dossier où les requêtes sont stockées
    - scripts : Liste des scripts SQL
    - mysql_path : Variable d'environnement pour récupérer le chemin de MySQL
    - db_host : Adresse du serveur MySQL
    - db_user : Utilisateur de la base de données MySQL
    - db_password : Mot de passe de la base de données MySQL
    - db_name : Nom de la base de données MySQL

Etapes du script :
    - Connexion à la base de données,
    - vérification du contenu de la base de données 
    - Parcours et exécution des scripts SQL à l'existence de la date en paramètres

Exemples :
create_tables(connection, sql_folder, scripts, mysql_path, db_host, db_user, db_password, db_name)
'''
def create_tables(connection, sql_folder, scripts, mysql_path, db_host, db_user, db_password, db_name):
    # Création d'un curseur pour exécuter des requêtes SQL
    cursor = connection.cursor()
    try:
        # Exécution d'une requête pour obtenir la liste des tables existantes dans la base de données
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        # Si la base de données est vide (aucune table existante)
        if len(tables) == 0:
            # Parcourt de la liste des scripts SQL à exécuter
            for script in scripts:
                script_path = os.path.join(sql_folder, script)
                script_path_abs = os.path.abspath(script_path)

                # Vérification de l'existence du script SQL
                if os.path.exists(script_path_abs):
                    print(f"Exécution du script SQL: {script_path_abs}")
                    # Construction d'une commande pour l'exécution du script SQL à l'aide de la ligne de commande MySQL
                    command = f"mysql -h {db_host} -u {db_user} -p{db_password} {db_name} < \"{script_path_abs}\""
                    # Exécution de la commande en utilisant subprocess.run
                    subprocess.run(command, shell=True)
                else:
                    print(f"Erreur : Le fichier '{script_path_abs}' n'existe pas.")
        else:
            # Si des tables existent déjà dans la base de données, on affiche un message d'erreur
            print("La base de données n'est pas vide.")
    finally:
        # Fermeture du curseur
        cursor.close()

'''
Explications :
La fonction delete_records prend en entrée :
    - connection : objet de connexion à la base de données MySQL
    - table : Nom de la table où la suppression doit-être exécutée
    - prefix : Préfixe de l'id utilisé
    - date : Date unique ou intervalle

Etapes du script :
    - Connexion à la base de données, 
    - Parcours de la date en paramètres, 
    - Construction de la requête de suppression en fonction des paramètres
    - Exécution de la requête 

Exemples :
delete_records(connection, 'files', 'logs_vols', '2023-11-08')
delete_records(connection, 'files', 'logs_vols', '2023-11-07:2023-11-10')
delete_records(connection, 'files', 'logs_vols', '2023-11-09', '2023-11-11', '2023-11-13')
delete_records(connection, 'files', 'logs_vols', '2023-11-09', '2023-11-07:2023-11-10')
'''
def delete_records(connection, table, prefix, *dates):
    try:
        # Création d'un curseur pour exécuter des requêtes SQL
        cursor = connection.cursor()

        # Parcours de chaque date ou période de dates passées en argument
        for date_range in dates:
            if ':' in date_range:
                # Si la date_range contient un ':', cela signifie que c'est une période de dates
                # Par exemple, '2022-01-01:2022-01-05'
                start_date, end_date = date_range.split(':')
                # Construction d'une requête SQL pour supprimer les enregistrements dans la période spécifiée
                query = f"DELETE FROM {table} WHERE id LIKE '{prefix}_%' AND id BETWEEN '{prefix}_{start_date}' AND '{prefix}_{end_date}'"
            else:
                # Si la date_range ne contient pas de ':', cela signifie que c'est une date spécifique
                # Par exemple, '2022-01-01'
                # Construstion d'une requête SQL pour supprimer l'enregistrement de cette date spécifique
                query = f"DELETE FROM {table} WHERE id = '{prefix}_{date_range}'"

            # Exécution de la requête SQL
            cursor.execute(query)
            # Validation des changements dans la base de données
            connection.commit()

    except mysql.connector.Error as err:
        # Affiche d'un message en cas d'erreur
        print(f"Erreur MySQL : {err}")


def export_data(connection, table_name):
    query = f"SELECT * FROM {table_name}"
    data = pd.read_sql(query, connection)
    return data

def export_data_date(connection, table_name, date):
    query = f"SELECT * FROM {table_name} WHERE id like '%{date}'"
    data = pd.read_sql(query, connection)
    return data
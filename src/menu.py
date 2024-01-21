import argparse
import moduleSQL
import moduleConnection
import os
import datetime
import modulePreprocessing
import pandas as pd

# Menu de gestion de la base de données
def afficher_menu():
    print("Menu:")
    print("1. Suppression LogsVols")
    print("2. Suppression Degrations")
    print("3. Intégration de journées manquantes")
    print("4. Génération de CSV")
    print("0. Quitter")

    choix = input("Choisissez une option : ")

    return choix

if __name__ == "__main__":
    while True:
        choix = afficher_menu()

        if choix == "1":
            # Message d'informations / Saisie de la date
            print("Suppression de journées logs_vols déjà intégrées")
            print("Format : '2023-11-09', '2023-11-07:2023-11-10'")
            date = input('Veuillez saisir une date : ')

            # Chemin relatif vers le fichier .env depuis le dossier src
            env_path = os.path.join(os.path.dirname(__file__), 'env', 'adm.env')
            moduleConnection.load_environment(env_path)

            # Récupération des variables d'environnement
            db_host = os.getenv("DB_HOST")
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")
            db_name = os.getenv("DB_NAME")

            # Connexion à la base de données
            connection = moduleConnection.get_db_connection(db_host, db_user, db_password, db_name)
            
            # Suppression des enregistrements
            moduleSQL.delete_records(connection, 'files', 'logs_vols', date)

        elif choix == "2":
            # Message d'informations / Saisie de la date
            print("Suppression de journées degradations déjà intégrées")
            print("Format : '2023-11-09', '2023-11-07:2023-11-10'")
            date = input('Veuillez saisir une date : ')

            # Chemin relatif vers le fichier .env depuis le dossier src
            env_path = os.path.join(os.path.dirname(__file__), 'env', 'adm.env')
            moduleConnection.load_environment(env_path)

            # Récupération des variables d'environnement
            db_host = os.getenv("DB_HOST")
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")
            db_name = os.getenv("DB_NAME")

            # Connexion à la base de données
            connection = moduleConnection.get_db_connection(db_host, db_user, db_password, db_name)

            # Suppression des enregistrements
            moduleSQL.delete_records(connection, 'files', 'degradations', date)

        elif choix == "3":
            # Message d'informations / Saisie de la date
            print("Réintégration de journées à partir de la date saisie")
            print("Format : '2023-11-09'")
            
            # Saisie d'une date au format YYYY-MM-DD
            start_date = input("Veuillez saisir une date : ")

            try:
                # Récupération et traitement des .csv
                # Téléchargement des fichiers logs_vols_AAAA-MM-JJ.csv
                modulePreprocessing.file_download_batch(start_date = start_date, path = 'https://sc-e.fr/docs/', file_name = 'logs_vols_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
                # Téléchargement des fichiers degradations_AAAA-MM-JJ.csv
                modulePreprocessing.file_download_batch(start_date = start_date, path = 'https://sc-e.fr/docs/', file_name = 'degradations_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
                # Téléchargement des fichiers aeronefs_AAAA-MM-JJ.csv
                modulePreprocessing.file_download_batch(start_date = start_date, path = 'https://sc-e.fr/docs/', file_name = 'aeronefs_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
                # Téléchargement des fichiers composants_AAAA-MM-JJ.csv
                modulePreprocessing.file_download_batch(start_date = start_date, path = 'https://sc-e.fr/docs/', file_name = 'composants_AAAA-MM-JJ.csv', storage_folder='data/incoming')
            except ValueError:
                print("Format de date incorrect. Veuillez saisir une date au format 'YYYY-MM-DD'.")

        elif choix == "4":
            # Message d'informations / Saisie de la date
            print("Génération des tables logs_vols et dégradations au format CSV")

            # Fonction pour exporter les données vers un fichier CSV
            def export_to_csv(data, table_name, output_folder):
                # Création du dossier si non existant
                if not os.path.exists(output_folder):
                    os.makedirs(output_folder)

                file_name = os.path.join(output_folder, f"{table_name}_{datetime.date.today()}.csv")
                data.to_csv(file_name, index=False)
                print(f"Données de la table {table_name} exportées vers {file_name}")

            output_folder = "data/extract"

            # Message d'informations
            print("Export de données des tables logs_vols et degradations vers des fichiers CSV")
            print("Options disponibles :")
            print("- Saisir 'FULL' pour exporter la totalité des deux tables")
            print("- Saisir 'TODAY' pour exporter les données d'aujourd'hui")
            
            user_input = input("Veuillez saisir une option : ")
            
            # Chemin relatif vers le fichier .env depuis le dossier src
            env_path = os.path.join(os.path.dirname(__file__), 'env', 'adm.env')
            moduleConnection.load_environment(env_path)
            
            # Récupération des variables d'environnement
            db_host = os.getenv("DB_HOST")
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")
            db_name = os.getenv("DB_NAME")
            
            # Connexion à la base de données
            connection = moduleConnection.get_db_connection(db_host, db_user, db_password, db_name)
            
            # Export des données en fonction de l'option choisie
            if user_input == 'FULL':
                logs_vols_data = moduleSQL.export_data(connection, 'logs_vols')
                degradations_data = moduleSQL.export_data(connection, 'degradations')
                export_to_csv(logs_vols_data, 'logs_vols', output_folder)
                export_to_csv(degradations_data, 'degradations', output_folder)
            elif user_input == 'TODAY':
                today = datetime.date.today()
                logs_vols_data = moduleSQL.export_data_date(connection, 'logs_vols', today)
                degradations_data = moduleSQL.export_data_date(connection, 'degradations', today)
                export_to_csv(logs_vols_data, 'logs_vols', output_folder)
                export_to_csv(degradations_data, 'degradations', output_folder)
            else:
                pass

            connection.close()     

        elif choix == "0":
            print("Au revoir !")
            break
        else:
            print("Option invalide. Veuillez choisir une option valide.")

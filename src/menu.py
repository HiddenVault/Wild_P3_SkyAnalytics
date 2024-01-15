import argparse
import moduleSQL
import moduleConnection
import os

# Menu de gestion de la base de données
def afficher_menu():
    print("Menu:")
    print("1. Suppression LogsVols")
    print("2. Suppression Degrations")
    print("3. Quitter")

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
            print("Au revoir !")
            break
        else:
            print("Option invalide. Veuillez choisir une option valide.")

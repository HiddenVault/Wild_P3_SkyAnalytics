import mysql.connector
from dotenv import load_dotenv
import os
import pymysql

# Chemin relatif vers le fichier .env depuis le dossier src
env_path = os.path.join(os.path.dirname(__file__), 'env', 'adm.env')

# Chargement des variables d'environnement depuis le fichier .env
load_dotenv(env_path)

# Récupération des variables d'environnement
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")

# Etablissement d'une connexion à la base de données MySQL avec les variables de connexion
connection = pymysql.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)

# Création d'un curseur pour exécuter des commandes SQL avec pymysql
cursor = connection.cursor()

# Vérification de l'existance de la base de données
cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
result = cursor.fetchone()

# Création de la base de données si elle n'existe pas
if not result:
    cursor.execute(f"CREATE DATABASE {db_name}")
    print(f"La base de données '{db_name}' a été créée avec succès.")
else:
    print(f"La base de données '{db_name}' existe déjà.")

# Fermeture du curseur et connexion pymysql
cursor.close()
connection.close()


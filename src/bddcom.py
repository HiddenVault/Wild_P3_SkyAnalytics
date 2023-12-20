import mysql.connector
from dotenv import load_dotenv
import os
import subprocess

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
connection = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)

# Création d'un curseur pour exécuter des commandes SQL avec mysql.connector
cursor = connection.cursor()

# Vérification de l'existence de tables (en comptant leur nombre)
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()

if len(tables) == 0:
    sql_folder = os.path.join(os.path.dirname(__file__), 'sql')
    scripts = ['init.sql', 'aeronefs.sql', 'composants.sql']

    for script in scripts:
        script_path = os.path.join(sql_folder, script)
        # Conversion du chemin relatif en chemin absolu
        script_path_abs = os.path.abspath(script_path) 

        if os.path.exists(script_path_abs):
            print(f"Exécution du script SQL: {script_path_abs}") 
            # L'emplacement du dossier de mysql doit-être renseigné dans les variables d'environnement de l'OS.
            # On peut aussi le déclarer par l'intermédiaire d'une variable.
            # Exemple : mysql_path = "C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin\\mysql.exe"
            command = f"mysql -h {db_host} -u {db_user} -p{db_password} {db_name} < \"{script_path_abs}\""
            subprocess.run(command, shell=True)
        else:
            print(f"Erreur : Le fichier '{script_path_abs}' n'existe pas.")

# Fermeture du curseur et de la connexion
print("La base de données n'est pas vide.")            
cursor.close()
connection.close()

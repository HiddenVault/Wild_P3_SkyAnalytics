import os
from moduleConnection import load_environment, get_mysql_path, get_db_connection, close_db_connection
from moduleSQL import create_tables

# Chemin relatif vers le fichier .env depuis le dossier src
env_path = os.path.join(os.path.dirname(__file__), 'env', 'adm.env')
load_environment(env_path)

# Récupération des variables d'environnement
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")

# Connexion à la base de données
connection = get_db_connection(db_host, db_user, db_password, db_name)

# Chemins et scripts SQL
mysql_path = get_mysql_path()
sql_folder = os.path.join(os.path.dirname(__file__), 'sql')
scripts = ['init.sql', 'aeronefs.sql', 'composants.sql']

# Exécution des scripts SQL
create_tables(connection, sql_folder, scripts, mysql_path, db_host, db_user, db_password, db_name)

# Récupération et traitement des .csv

# Fermeture de la connexion
close_db_connection(connection)

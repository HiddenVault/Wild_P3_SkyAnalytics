import warnings
import os
from moduleConnection import load_environment, get_mysql_path, get_db_connection, close_db_connection
from moduleSQL import create_tables
from modulePreprocessing import file_download_batch
import schedule
import concurrent.futures
import time

warnings.filterwarnings("ignore", category=UserWarning)

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

# Fermeture de la connexion
close_db_connection(connection)

# Planificateurs de tâches
def schedule_tasks_test():
    # Récupération et traitement des .csv
    # Téléchargement des fichiers logs_vols_AAAA-MM-JJ.csv
    schedule.every(5).seconds.do(file_download_batch, start_date = '2023-11-01', path = 'http://hiddenvault.fr/P3_SkyAnalytics/docs/', file_name = 'logs_vols_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
    # Téléchargement des fichiers degradations_AAAA-MM-JJ.csv
    schedule.every(1).seconds.do(file_download_batch, start_date = '2023-11-01', path = 'http://hiddenvault.fr/P3_SkyAnalytics/docs/', file_name = 'degradations_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
    # Téléchargement des fichiers aeronefs_AAAA-MM-JJ.csv
    schedule.every(5).seconds.do(file_download_batch, start_date = '2023-11-01', path = 'http://hiddenvault.fr/P3_SkyAnalytics/docs/', file_name = 'aeronefs_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
    # Téléchargement des fichiers composants_AAAA-MM-JJ.csv
    schedule.every(10).seconds.do(file_download_batch, start_date = '2023-11-01', path = 'http://hiddenvault.fr/P3_SkyAnalytics/docs/', file_name = 'composants_AAAA-MM-JJ.csv', storage_folder='data/incoming')

def schedule_tasks():
    # Récupération et traitement des .csv
    # Téléchargement des fichiers logs_vols_AAAA-MM-JJ.csv
    schedule.every(60).seconds.do(file_download_batch, start_date = '2024-01-11', path = 'https://sc-e.fr/docs/', file_name = 'logs_vols_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
    # Téléchargement des fichiers degradations_AAAA-MM-JJ.csv
    schedule.every(10).seconds.do(file_download_batch, start_date = '2024-01-11', path = 'https://sc-e.fr/docs/', file_name = 'degradations_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
    # Téléchargement des fichiers aeronefs_AAAA-MM-JJ.csv
    schedule.every(60).seconds.do(file_download_batch, start_date = '2024-01-11', path = 'https://sc-e.fr/docs/', file_name = 'aeronefs_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
    # Téléchargement des fichiers composants_AAAA-MM-JJ.csv
    schedule.every(60).seconds.do(file_download_batch, start_date = '2024-01-11', path = 'https://sc-e.fr/docs/', file_name = 'composants_AAAA-MM-JJ.csv', storage_folder='data/incoming')


# Appel de la fonction pour exécuter les tâches planifiées
#schedule_tasks_test()
schedule_tasks()


# Création un ThreadPoolExecutor (pool de threads) pour exécuter les tâches planifiées en parallèle
with concurrent.futures.ThreadPoolExecutor() as executor:
    while True:
        # Exécution des tâches planifiées en parallèle
        # Liste des tâches planifiées à partir de l'objet 'schedule'
        scheduled_jobs = schedule.get_jobs()

        # Parcours de chaque tâche planifiée dans la liste
        for job in scheduled_jobs:
            # Vérification de la tâche à exécuter à un instant donné
            if job.should_run:
                # Stockage de la tâche actuelle dans la variable 'current_job'
                current_job = job
                # Exécution de la tâche avec la méthode 'run' de la tâche
                job.run()
                # Réinitialisation de la variable 'current_job' après l'exécution de la tâche
                current_job = None
                # Temps d'attente entre chaque exécution en secondes (On évite la surchage du processeur)
                time.sleep(1)

import mysql.connector
from dotenv import load_dotenv
import os

def load_environment(env_path):
    load_dotenv(env_path)

def get_mysql_path():
    mysql_path = os.environ.get('MYSQL_PATH')
    if mysql_path is None:
        print("La variable d'environnement MYSQL_PATH n'est pas définie.")
        return None
    else:
        print("Chemin MySQL trouvé:", mysql_path)
        return mysql_path

def get_db_connection(db_host, db_user, db_password, db_name):
    connection = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )
    return connection

def close_db_connection(connection):
    connection.close()

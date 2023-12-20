import os
import subprocess

def create_tables(connection, sql_folder, scripts, mysql_path, db_host, db_user, db_password, db_name):
    cursor = connection.cursor()
    try:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        if len(tables) == 0:
            for script in scripts:
                script_path = os.path.join(sql_folder, script)
                script_path_abs = os.path.abspath(script_path)

                if os.path.exists(script_path_abs):
                    print(f"Exécution du script SQL: {script_path_abs}")
                    command = f"mysql -h {db_host} -u {db_user} -p{db_password} {db_name} < \"{script_path_abs}\""
                    subprocess.run(command, shell=True)
                else:
                    print(f"Erreur : Le fichier '{script_path_abs}' n'existe pas.")
        else:
            print("La base de données n'est pas vide.")
    finally:
        cursor.close()

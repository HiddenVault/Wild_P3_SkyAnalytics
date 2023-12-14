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

import os  # Pour la gestion des fichiers et des répertoires
import requests  # Pour effectuer des requêtes HTTP
from datetime import datetime, timedelta  # Pour manipuler les dates

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
    
    # Tant que la date actuelle est supérieure ou égale à la date de début
    while start_date <= datetime.now():
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
            
            # Message de succès
            print(f"✅ Fichier {file_name_complete} téléchargé avec succès.")
        else:
            # Message d'échec en cas de problème lors du téléchargement
            print(f"❌ Échec du téléchargement du fichier {file_name_complete}.")
        
        # Passage à la date suivante en ajoutant un jour
        start_date = start_date + timedelta(days=1)

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


'''
Explications :
La fonction file_download prend en entrée :
    - start_date : Date de début
    - path : URL où les fichiers sont stockés
    - file_name : Nom du fichier à télécharger
    - storage_folder : Dossier de stockage local

Etapes du script :
    - Téléchargement des fichiers depuis un chemin de base, 
    - Les fichiers sont stockés localement, 
    - Affiche d'un message en cas de réussite ou d'échec pour chaque téléchargement. 

Exemple :
file_download(start_date = '2023-10-29', path = 'https://sc-e.fr/docs/', file_name = 'logs_vols_AAAA-MM-JJ.csv', storage_folder = 'incoming')
'''

import os  # Pour la gestion des fichiers et des répertoires
import requests  # Pour effectuer des requêtes HTTP
from datetime import datetime, timedelta  # Pour manipuler les dates

def file_download(start_date, path, file_name, storage_folder):
    print()
    print(f"💾 Téléchargement des fichiers {file_name}.")
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
        # Explications : si file_name est "logs_vols_AAAA-MM-JJ.csv" , alors file_name_complet deviendra "logs_vols_2023-10-29.csv". 
        file_name_complet = file_name.replace('AAAA-MM-JJ', date_format)
        
        # Construction de l'URL complète en combinant l'URL et le nom du fichier complet
        url = os.path.join(path, file_name_complet)
        
        # Téléchargement du fichier depuis l'URL
        response = requests.get(url)
        
        # Vérification de la réponse du serveur (status_code == 200 signifie que la requête a réussi)
        if response.status_code == 200:
            # Création du chemin complet du fichier de destination en combinant le dossier de stockage et le nom du fichier complet
            path_destination = os.path.join(storage_folder, file_name_complet)
            
            # Enregistrement du fichier téléchargé localement dans le dossier de stockage
            # Ouverture du fichier en mode write binary (wb) 
            with open(path_destination, 'wb') as fichier_destination:
                fichier_destination.write(response.content)
            
            # Affichage d'un message de succès
            print(f"✅ Fichier {file_name_complet} téléchargé avec succès.")
        else:
            # Affichage d'un message d'échec en cas de problème lors du téléchargement
            print(f"❌ Échec du téléchargement du fichier {file_name_complet}.")
        
        # Passage à la date suivante en ajoutant un jour
        start_date = start_date + timedelta(days=1)

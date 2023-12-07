**Scripts**

[TOC]

# Analyse

## analyse_dataset.py

```python
# Importation des modules nécessaires
import moduleOS
import moduleCSV
import moduleDownload
import moduleDataframe
import moduleAnalyseHTML
import time

# Dictionnaire avec les noms des fichiers, leurs emplacements, leur type de séparateur et le nombre de lignes à générer dans les fichiers csv et html
files_dict = {
    'title.akas.tsv': ('./data/sources', '\t', 1000, 10, 11, 12),
    'complementaire_tmdb_full.csv': ('./data/sources', ',', 1000, 13, 14, 15),
    'title.ratings.tsv': ('./data/sources', '\t', 500, 16, 17, 18),
    'title.principals.tsv': ('./data/sources', '\t', 800, 19, 20, 21),
    'title.episode.tsv': ('./data/sources', '\t', 300, 22, 23, 24),
    'title.crew.tsv': ('./data/sources', '\t', 700, 25, 26, 27),
    'title.basics.tsv': ('./data/sources', '\t', 1200, 28, 29, 30),
    'name.basics.tsv': ('./data/sources', '\t', 900, 31, 32, 33),
}

# Préfixe pour les noms de fichiers HTML et CSV
file_prefix = 'A_'

# Début du chronomètre pour mesurer le temps de génération du rapport
start_time = time.time()

# Parcours du dictionnaire de fichiers
for file_name, (path, separator, nrows_value, first_rows, sample_rows, last_rows) in files_dict.items():
    # Téléchargement ou lecture du contenu du fichier
    content = moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)

    # Création du dossier des fichiers .csv
    csv_directory = './data/analyse/'
    moduleOS.create_csv_directory(csv_directory)

    if content is not None:
        # Création d'un DataFrame à partir du contenu du fichier
        df = moduleDataframe.create_dataframe(content, separator, nrows_value)
        
        if df is not None:
            # Création des noms des fichiers avec le préfixe
            csv_file_name = f'{file_prefix}{file_name}.csv'
            html_file_name = f'{file_prefix}{file_name}.html'
            html_file_name_with_prefix = html_file_name

            # Création des fichiers CSV
            moduleCSV.create_csv_files(df, csv_directory, csv_file_name, first_rows, sample_rows, last_rows, nrows_value)

            # Déclaration de local_file_path pour définir le dossier de sauvegarde
            local_file_path = f'./data/analyse/{html_file_name}'

            # Création des fichiers HTML
            moduleAnalyseHTML.create_html_file(df, html_file_name, nrows_value, start_time, files_dict, local_file_path, file_prefix='A_')

            # Obtention des informations du DataFrame
            moduleDataframe.get_dataframe_info(df)

            # Téléchargement du fichier depuis une URL ou lecture depuis un chemin local
            moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)

            # Création d'un DataFrame à partir du contenu du fichier
            moduleDataframe.create_dataframe(content, separator, nrows_value)
```

**Explications :**

1. Le code commence par importer les modules nécessaires pour le traitement des fichiers, la manipulation de DataFrames, le téléchargement, etc.
2. Un dictionnaire (`files_dict`) est défini, spécifiant les noms des fichiers, leurs emplacements, les séparateurs, et le nombre de lignes à générer dans les fichiers CSV et HTML.
3. Un préfixe pour les noms de fichiers HTML et CSV est défini (`file_prefix`).
4. Un chronomètre est démarré pour mesurer le temps de génération du rapport.
5. Le code parcourt le dictionnaire de fichiers, télécharge ou lit le contenu, crée des DataFrames, génère des fichiers CSV, HTML, et obtient des informations sur les DataFrames.
6. Les modules tels que `moduleOS`, `moduleCSV`, `moduleDownload`, `moduleDataframe`, et `moduleAnalyseHTML` sont utilisés pour effectuer différentes opérations sur les fichiers et les données.

# Préparation



## preparation.complementaire_tmdb_full.csv.py

```python
import moduleOS
import moduleCSV
import moduleDownload
import moduleOS
import moduleDataframe
import modulePreparationHTML
import time
import pandas as pd
import numpy as np
import re

# Dictionnaire avec les noms des fichiers, leurs emplacements, leur type de séparateur et le nombre de lignes
files_dict = {
   'complementaire_tmdb_full.csv': ('./data/sources', ',', -1, 13, 14, 15)
}

# Préfixe pour les fichiers HTML et CSV
file_prefix = 'P_'

# Début du chronomètre
import time
start_time = time.time()

# Parcours du dictionnaire de fichiers
for file_name, (path, separator, nrows_value, first_rows, sample_rows, last_rows) in files_dict.items():
    # Téléchargement ou lecture du contenu du fichier
    content = moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)
    
    # Utilisez la fonction pour créer le répertoire des fichiers .csv
    csv_directory = './data/preparation'
    moduleOS.create_csv_directory(csv_directory)
    
    if content is not None:
        # Création d'un DataFrame original à partir du contenu du fichier
        df_original = moduleDataframe.create_dataframe(content, separator, nrows_value)

        if df_original is not None:
            # Création d'une copie du DataFrame original pour les manipulations
            df_copy = df_original.copy()

            # Remplacement des retours à la ligne indésirables dans les champs de texte
            df_copy = df_copy.applymap(lambda x: x.replace('\n', '') if isinstance(x, str) else x)

            # Remplacemenent de toutes les occurrences de "NaN" par None dans le DataFrame
            df_copy = df_copy.replace(np.nan, None)

            # Renommage de la colonne 'imdb_id' en 'tconst'
            df_copy = df_copy.rename(columns={'imdb_id': 'tconst'})

            # Fonction pour ajouter un préfixe à la colonne 'poster_path'
            def add_prefix(path):
                if path is not None:
                    return 'https://image.tmdb.org/t/p/original' + str(path)
                return None

            df_copy['poster_path'] = df_copy['poster_path'].apply(add_prefix)

            # Suppression des colonnes indiquées
            columns_to_drop = ['adult', 'genres','homepage','id','original_language','original_title','release_date','title','runtime','status','production_countries','spoken_languages','production_companies_name','production_companies_country','backdrop_path','video','vote_average','vote_count']
            df_copy = df_copy.drop(columns=columns_to_drop)

            # Créez les noms des fichiers avec le préfixe
            csv_file_name = f'{file_prefix}{file_name}.csv'
            html_file_name = f'{file_prefix}{file_name}.html'
            html_file_name_with_prefix = html_file_name

            # Fonction pour créer des fichiers CSV
            moduleCSV.create_csv_files(df_copy, csv_directory, csv_file_name, first_rows, sample_rows, last_rows, nrows_value)
            
            local_file_path = f'./data/preparation/{html_file_name}'  # Déclaration de local_file_path
            
            # Fonction pour créer un fichier HTML à partir du DataFrame
            modulePreparationHTML.create_html_file(df_copy, html_file_name, nrows_value, start_time, files_dict, local_file_path, file_prefix='P_')

            # Fonction pour obtenir les informations du DataFrame
            moduleDataframe.get_dataframe_info(df_copy)

            # Fonction pour télécharger un fichier depuis une URL ou lire depuis un chemin local
            moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)

            # Fonction pour créer un DataFrame à partir du contenu du fichier
            moduleDataframe.create_dataframe(content, separator, nrows_value)

```

**Explications :**

1. Le script commence par importer différents modules nécessaires et définir des variables, telles que le dictionnaire `files_dict` qui contient les détails des fichiers à traiter et le préfixe pour les fichiers HTML et CSV.
2. Ensuite, il parcourt chaque fichier spécifié dans le dictionnaire et effectue une série d'opérations, y compris le téléchargement ou la lecture du fichier, la création d'un répertoire pour les fichiers CSV, la manipulation du DataFrame, la création de fichiers CSV, et enfin la création d'un fichier HTML avec des informations détaillées.

## preparation.name.basics.csv.py

```python
import moduleOS
import moduleCSV
import moduleDownload
import moduleOS
import moduleDataframe
import modulePreparationHTML
import time
import pandas as pd

# Dictionnaire avec les noms des fichiers, leurs emplacements, leur type de séparateur et le nombre de lignes
files_dict = {
    'name.basics.tsv': ('./data/sources', '\t', -1, 31, 32, 33)
}

# Préfixe pour les fichiers HTML et CSV
file_prefix = 'P_'

# Début du chronomètre
import time
start_time = time.time()

# Parcours du dictionnaire de fichiers
for file_name, (path, separator, nrows_value, first_rows, sample_rows, last_rows) in files_dict.items():
    # Téléchargement ou lecture du contenu du fichier
    content = moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)
    
    # Appel de la fonction pour créer le répertoire des fichiers .csv
    csv_directory = './data/preparation'
    moduleOS.create_csv_directory(csv_directory)
    
    if content is not None:
        # Création d'un DataFrame original à partir du contenu du fichier
        df_original = moduleDataframe.create_dataframe(content, separator, nrows_value)  
        
        if df_original is not None:
            # Création d'une copie du DataFrame original pour les manipulations
            df_copy = df_original.copy()

            # Conversion de la colonne 'birthYear' en numérique en gérant les erreurs
            df_copy['birthYear'] = pd.to_numeric(df_copy['birthYear'], errors='coerce')
            
            # Remplacement des valeurs NaN par 0
            df_copy['birthYear'].fillna(0, inplace=True)
            
            # Filtrage des dates de naissance supérieures à 1920 ou égales à 0
            df_copy = df_copy[(df_copy['birthYear'] >= 1920)]
            #df_copy = df_copy[(df_copy['birthYear'] >= 1920) | (df_copy['birthYear'] == 0)] # Trop gourmand en ressources

            # Suppression des colonnes indiquées
            columns_to_drop = ['birthYear', 'deathYear']
            df_copy = df_copy.drop(columns=columns_to_drop)

            # Création des colonnes de valeurs dummies pour 'primaryProfession'
            dummies = df_copy['primaryProfession'].str.get_dummies(sep=',')
            df_copy = pd.concat([df_copy, dummies], axis=1)

            # Renommage de la colonne 'knownForTitles' en 'tconst'
            df_copy = df_copy.rename(columns={'knownForTitles': 'tconst'})

            # Renommage de la colonne 'nconst' en 'nconst_nb'
            df_copy = df_copy.rename(columns={'nconst': 'nconst_nb'})

            # Division de la colonne 'tconst' en listes de valeurs
            df_copy['tconst'] = df_copy['tconst'].str.split(',')
            df_copy = df_copy.explode('tconst')

            # Supprimer la colonne 'primaryProfession'
            columns_to_drop = ['primaryProfession']
            df_copy = df_copy.drop(columns=columns_to_drop)

            # Création des noms des fichiers avec le préfixe
            csv_file_name = f'{file_prefix}{file_name}.csv'
            html_file_name = f'{file_prefix}{file_name}.html'

            # Appel de la fonction pour créer des fichiers CSV
            moduleCSV.create_csv_files(df_copy, csv_directory, csv_file_name, first_rows, sample_rows, last_rows, nrows_value)
            
            local_file_path = f'./data/preparation/{html_file_name}'  # Déclaration de local_file_path
            
            # Appel de la fonction pour créer un fichier HTML à partir du DataFrame
            modulePreparationHTML.create_html_file(df_copy, html_file_name, nrows_value, start_time, files_dict, local_file_path, file_prefix='P_')

            # Appel de la fonction pour obtenir les informations du DataFrame
            moduleDataframe.get_dataframe_info(df_copy)

            # Appel de la fonction pour télécharger un fichier depuis une URL ou lire depuis un chemin local
            moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)

            # Appel de la fonction pour créer un DataFrame à partir du contenu du fichier
            moduleDataframe.create_dataframe(content, separator, nrows_value)

```

**Explications :**

1. Le script commence par importer différents modules nécessaires et définir des variables, telles que le dictionnaire `files_dict` qui contient les détails des fichiers à traiter et le préfixe pour les fichiers HTML et CSV.
2. Ensuite, il parcourt chaque fichier spécifié dans le dictionnaire et effectue une série d'opérations, y compris le téléchargement ou la lecture du fichier, la création d'un répertoire pour les fichiers CSV, la manipulation du DataFrame, la création de fichiers CSV, et enfin la création d'un fichier HTML avec des informations détaillées.

## preparation.title.akas.csv.py

```python
import moduleOS
import moduleCSV
import moduleDownload
import moduleOS
import moduleDataframe
import modulePreparationHTML
import pandas as pd

# Dictionnaire avec les noms des fichiers, leurs emplacements, leur type de séparateur et le nombre de lignes
files_dict = {
    'title.akas.tsv': ('./data/sources', '\t', -1, 100, 100, 100)
}

# Préfixe pour les fichiers HTML et CSV
file_prefix = 'P_'

# Début du chronomètre
import time
start_time = time.time()

# Parcours du dictionnaire de fichiers
for file_name, (path, separator, nrows_value, first_rows, sample_rows, last_rows) in files_dict.items():
    # Téléchargement ou lecture du contenu du fichier
    content = moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)
    
    # Appel de la fonction pour créer le répertoire des fichiers .csv
    csv_directory = './data/preparation'
    moduleOS.create_csv_directory(csv_directory)
    
    if content is not None:
        # Création d'un DataFrame original à partir du contenu du fichier
        df_original = moduleDataframe.create_dataframe(content, separator, nrows_value)  
        
        if df_original is not None:
            # Création d'une copie du DataFrame original pour les manipulations
            df_copy = df_original.copy()

            # Filtrer les lignes avec "isOriginalTitle" égale à 1
            df_copy = df_copy[df_copy['isOriginalTitle'] == '1']

            # Suppression des colonnes indiquées
            columns_to_drop = ['ordering', 'attributes', 'types', 'language','region','isOriginalTitle']
            df_copy = df_copy.drop(columns=columns_to_drop)

            # Renommage de la colonne 'titleId' en 'tconst'
            df_copy = df_copy.rename(columns={'titleId': 'tconst'})        

            # Réinitialiser les index si nécessaire
            # df_copy.reset_index(drop=True, inplace=True)    

            # Création des noms des fichiers avec le préfixe
            csv_file_name = f'{file_prefix}{file_name}.csv'
            html_file_name = f'{file_prefix}{file_name}.html'

            # Appel de la fonction pour créer des fichiers CSV
            moduleCSV.create_csv_files(df_copy, csv_directory, csv_file_name, first_rows, sample_rows, last_rows, nrows_value)
            
            local_file_path = f'./data/preparation/{html_file_name}'  # Déclaration de local_file_path
            
            # Appel de la fonction pour créer un fichier HTML à partir du DataFrame
            modulePreparationHTML.create_html_file(df_copy, html_file_name, nrows_value, start_time, files_dict, local_file_path, file_prefix='P_')

            # Appel de la fonction pour obtenir les informations du DataFrame
            moduleDataframe.get_dataframe_info(df_copy)

            # Appel de la fonction pour télécharger un fichier depuis une URL ou lire depuis un chemin local
            moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)

            # Appel de la fonction pour créer un DataFrame à partir du contenu du fichier
            moduleDataframe.create_dataframe(content, separator, nrows_value)

```

**Explications :**

1. Le script commence par importer différents modules nécessaires et définir des variables, telles que le dictionnaire `files_dict` qui contient les détails des fichiers à traiter et le préfixe pour les fichiers HTML et CSV.
2. Ensuite, il parcourt chaque fichier spécifié dans le dictionnaire et effectue une série d'opérations, y compris le téléchargement ou la lecture du fichier, la création d'un répertoire pour les fichiers CSV, la manipulation du DataFrame, la création de fichiers CSV, et enfin la création d'un fichier HTML avec des informations détaillées.

## preparation.title.basics.csv.py

```python
import moduleOS
import moduleCSV
import moduleDownload
import moduleOS
import moduleDataframe
import modulePreparationHTML
import time
import pandas as pd
import numpy as np

# Dictionnaire avec les noms des fichiers, leurs emplacements, leur type de séparateur et le nombre de lignes
files_dict = {
    'title.basics.tsv': ('./data/sources', '\t', -1, 28, 29, 30)
}

# Préfixe pour les fichiers HTML et CSV
file_prefix = 'P_'

# Début du chronomètre
import time
start_time = time.time()

# Parcours du dictionnaire de fichiers
for file_name, (path, separator, nrows_value, first_rows, sample_rows, last_rows) in files_dict.items():
    # Téléchargement ou lecture du contenu du fichier
    content = moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)
    
    # Appel de la fonction pour créer le répertoire des fichiers .csv
    csv_directory = './data/preparation'
    moduleOS.create_csv_directory(csv_directory)
    
    if content is not None:
        # Création d'un DataFrame original à partir du contenu du fichier
        df_original = moduleDataframe.create_dataframe(content, separator, nrows_value)  
        
        if df_original is not None:
            # Création d'une copie du DataFrame original pour les manipulations
            df_copy = df_original.copy()

            # Remplacement de toutes les occurrences de "\\N" par NaN dans le DataFrame en utilisant un caractère d'échappement
            df_copy = df_copy.replace({'\\N': np.nan})

            # Création de colonnes de valeurs dummies pour 'genres'
            dummies = df_copy['genres'].str.get_dummies(sep=',')
            df_copy = pd.concat([df_copy, dummies], axis=1)

            # Suppression des lignes où 'Short', 'Talk-Show', 'Reality-TV', 'News', 'Game-Show' sont égales à 1
            columns_to_drop = ['Short', 'Talk-Show', 'Reality-TV', 'News', 'Game-Show']
            df_copy = df_copy.drop(columns=columns_to_drop)

            # Filtrage sur titleType = movie
            df_copy = df_copy.loc[df_copy['titleType'] == 'movie']

            # Suppression des lignes où 'startYear' n'est pas compris entre 2019 (inclus) et 2024 (non inclus)
            df_copy['startYear'] = pd.to_numeric(df_copy['startYear'], errors='coerce').astype('Int64')
            df_copy['startYear'] = df_copy['startYear'].astype(int)
            df_copy = df_copy[(df_copy['startYear'] >= 2019) & (df_copy['startYear'] < 2024)]

            # Suppression des lignes où 'runtimeMinutes' est inférieur à 60
            df_copy['runtimeMinutes'] = df_copy['runtimeMinutes'].astype(int)
            df_copy = df_copy[df_copy['runtimeMinutes'] >= 60]

            # Suppression des colonnes 'originalTitle', 'isAdult', 'genres', 'endYear'
            columns_to_drop = ['originalTitle', 'isAdult', 'genres', 'endYear']
            df_copy = df_copy.drop(columns=columns_to_drop)

            # Réinitialiser les index si nécessaire
            # df_copy.reset_index(drop=True, inplace=True)    

            # Création des noms des fichiers avec le préfixe
            csv_file_name = f'{file_prefix}{file_name}.csv'
            html_file_name = f'{file_prefix}{file_name}.html'

            # Appel de la fonction pour créer des fichiers CSV
            moduleCSV.create_csv_files(df_copy, csv_directory, csv_file_name, first_rows, sample_rows, last_rows, nrows_value)

            local_file_path = f'./data/preparation/{html_file_name}'  # Déclaration de local_file_path

            # Appel de la fonction pour créer un fichier HTML à partir du DataFrame
            modulePreparationHTML.create_html_file(df_copy, html_file_name, nrows_value, start_time, files_dict, local_file_path, file_prefix='P_')

            # Appel de la fonction pour obtenir les informations du DataFrame
            moduleDataframe.get_dataframe_info(df_copy)

            # Appel de la fonction pour télécharger un fichier depuis une URL ou lire depuis un chemin local
            moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)

            # Appel de la fonction pour créer un DataFrame à partir du contenu du fichier
            moduleDataframe.create_dataframe(content, separator, nrows_value)

```

**Explications :**

1. Le script commence par importer différents modules nécessaires et définir des variables, telles que le dictionnaire `files_dict` qui contient les détails des fichiers à traiter et le préfixe pour les fichiers HTML et CSV.
2. Ensuite, il parcourt chaque fichier spécifié dans le dictionnaire et effectue une série d'opérations, y compris le téléchargement ou la lecture du fichier, la création d'un répertoire pour les fichiers CSV, la manipulation du DataFrame, la création de fichiers CSV, et enfin la création d'un fichier HTML avec des informations détaillées.

## preparation.title.crew.csv.py

```python
import moduleOS
import moduleCSV
import moduleDownload
import moduleOS
import moduleDataframe
import modulePreparationHTML
import time
import pandas as pd
import numpy as np
import ast

# Dictionnaire avec les noms des fichiers, leurs emplacements, leur type de séparateur et le nombre de lignes
files_dict = {
    'title.crew.tsv': ('./data/sources', '\t', -1, 25, 26, 27)
}

# Préfixe pour les fichiers HTML et CSV
file_prefix = 'P_'

# Début du chronomètre
import time
start_time = time.time()

# Parcours du dictionnaire de fichiers
for file_name, (path, separator, nrows_value, first_rows, sample_rows, last_rows) in files_dict.items():
    # Téléchargement ou lecture du contenu du fichier
    content = moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)
    
    # Appel de la fonction pour créer le répertoire des fichiers .csv
    csv_directory = './data/preparation'
    moduleOS.create_csv_directory(csv_directory)
    
    if content is not None:
        # Création d'un DataFrame original à partir du contenu du fichier
        df_original = moduleDataframe.create_dataframe(content, separator, nrows_value)  
        
        if df_original is not None:
            # Création d'une copie du DataFrame original pour les manipulations
            df_copy = df_original.copy()

            # Remplacement de toutes les occurrences de "\\N" par NaN dans le DataFrame en utilisant un caractère d'échappement
            df_copy = df_copy.replace('\\N', None)

            # Division des colonnes 'directors' et 'writers' en listes de valeurs
            df_copy['directors'] = df_copy['directors'].str.split(',')
            df_copy['writers'] = df_copy['writers'].str.split(',')

            # Explosion des listes créées, c'est-à-dire transformation des listes en lignes séparées pour chaque valeur
            df_copy = df_copy.explode('directors')
            df_copy = df_copy.explode('writers')

            # Réinitialiser les index si nécessaire
            # df_copy.reset_index(drop=True, inplace=True)    

            # Création des noms des fichiers avec le préfixe
            csv_file_name = f'{file_prefix}{file_name}.csv'
            html_file_name = f'{file_prefix}{file_name}.html'

            # Appel de la fonction pour créer des fichiers CSV
            moduleCSV.create_csv_files(df_copy, csv_directory, csv_file_name, first_rows, sample_rows, last_rows, nrows_value)

            local_file_path = f'./data/preparation/{html_file_name}'  # Déclaration de local_file_path

            # Appel de la fonction pour créer un fichier HTML à partir du DataFrame
            modulePreparationHTML.create_html_file(df_copy, html_file_name, nrows_value, start_time, files_dict, local_file_path, file_prefix='P_')

            # Appel de la fonction pour obtenir les informations du DataFrame
            moduleDataframe.get_dataframe_info(df_copy)

            # Appel de la fonction pour télécharger un fichier depuis une URL ou lire depuis un chemin local
            moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)

            # Appel de la fonction pour créer un DataFrame à partir du contenu du fichier
            moduleDataframe.create_dataframe(content, separator, nrows_value)

```

**Explications :**

1. Le script commence par importer différents modules nécessaires et définir des variables, telles que le dictionnaire `files_dict` qui contient les détails des fichiers à traiter et le préfixe pour les fichiers HTML et CSV.
2. Ensuite, il parcourt chaque fichier spécifié dans le dictionnaire et effectue une série d'opérations, y compris le téléchargement ou la lecture du fichier, la création d'un répertoire pour les fichiers CSV, la manipulation du DataFrame, la création de fichiers CSV, et enfin la création d'un fichier HTML avec des informations détaillées.

## preparation.title.episode.csv.py

```python
import moduleOS
import moduleCSV
import moduleDownload
import moduleOS
import moduleDataframe
import modulePreparationHTML
import time
import pandas as pd
import numpy as np

# Dictionnaire avec les noms des fichiers, leurs emplacements, leur type de séparateur et le nombre de lignes
files_dict = {
    'title.episode.tsv': ('./data/sources', '\t', -1, 22, 23, 24)
}

# Préfixe pour les fichiers HTML et CSV
file_prefix = 'P_'

# Début du chronomètre
import time
start_time = time.time()

# Parcours du dictionnaire de fichiers
for file_name, (path, separator, nrows_value, first_rows, sample_rows, last_rows) in files_dict.items():
    # Téléchargement ou lecture du contenu du fichier
    content = moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)
    
    # Appel de la fonction pour créer le répertoire des fichiers .csv
    csv_directory = './data/preparation'
    moduleOS.create_csv_directory(csv_directory)
    
    if content is not None:
        # Création d'un DataFrame original à partir du contenu du fichier
        df_original = moduleDataframe.create_dataframe(content, separator, nrows_value)  
        
        if df_original is not None:
            # Création d'une copie du DataFrame original pour les manipulations
            df_copy = df_original.copy()

            # Suppression des colonnes 'parentTconst', 'seasonNumber', 'episodeNumber'
            columns_to_drop = ['parentTconst', 'seasonNumber', 'episodeNumber']
            df_copy = df_copy.drop(columns=columns_to_drop)

            # Réinitialiser les index si nécessaire
            # df_copy.reset_index(drop=True, inplace=True)    

            # Création des noms des fichiers avec le préfixe
            csv_file_name = f'{file_prefix}{file_name}.csv'
            html_file_name = f'{file_prefix}{file_name}.html'

            # Appel de la fonction pour créer des fichiers CSV
            moduleCSV.create_csv_files(df_copy, csv_directory, csv_file_name, first_rows, sample_rows, last_rows, nrows_value)

            local_file_path = f'./data/preparation/{html_file_name}'  # Déclaration de local_file_path

            # Appel de la fonction pour créer un fichier HTML à partir du DataFrame
            modulePreparationHTML.create_html_file(df_copy, html_file_name, nrows_value, start_time, files_dict, local_file_path, file_prefix='P_')

            # Appel de la fonction pour obtenir les informations du DataFrame
            moduleDataframe.get_dataframe_info(df_copy)

            # Appel de la fonction pour télécharger un fichier depuis une URL ou lire depuis un chemin local
            moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)

            # Appel de la fonction pour créer un DataFrame à partir du contenu du fichier
            moduleDataframe.create_dataframe(content, separator, nrows_value)

```

**Explications :**

1. Le script commence par importer différents modules nécessaires et définir des variables, telles que le dictionnaire `files_dict` qui contient les détails des fichiers à traiter et le préfixe pour les fichiers HTML et CSV.
2. Ensuite, il parcourt chaque fichier spécifié dans le dictionnaire et effectue une série d'opérations, y compris le téléchargement ou la lecture du fichier, la création d'un répertoire pour les fichiers CSV, la manipulation du DataFrame, la création de fichiers CSV, et enfin la création d'un fichier HTML avec des informations détaillées.

## preparation.title.ratings.csv.py

```python
import moduleOS
import moduleCSV
import moduleDownload
import moduleOS
import moduleDataframe
import modulePreparationHTML
import time
import pandas as pd
import numpy as np

# Dictionnaire avec les noms des fichiers, leurs emplacements, leur type de séparateur et le nombre de lignes
files_dict = {
    'title.ratings.tsv': ('./data/sources', '\t', -1, 22, 23, 24)
}

# Préfixe pour les fichiers HTML et CSV
file_prefix = 'P_'

# Début du chronomètre
import time
start_time = time.time()

# Parcours du dictionnaire de fichiers
for file_name, (path, separator, nrows_value, first_rows, sample_rows, last_rows) in files_dict.items():
    # Téléchargement ou lecture du contenu du fichier
    content = moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)
    
    # Appel de la fonction pour créer le répertoire des fichiers .csv
    csv_directory = './data/preparation'
    moduleOS.create_csv_directory(csv_directory)
    
    if content is not None:
        # Création d'un DataFrame original à partir du contenu du fichier
        df_original = moduleDataframe.create_dataframe(content, separator, nrows_value)  
        
        if df_original is not None:
            # Création d'une copie du DataFrame original pour les manipulations
            df_copy = df_original.copy()

            # Réinitialiser les index si nécessaire
            # df_copy.reset_index(drop=True, inplace=True)    

            # Création des noms des fichiers avec le préfixe
            csv_file_name = f'{file_prefix}{file_name}.csv'
            html_file_name = f'{file_prefix}{file_name}.html'

            # Appel de la fonction pour créer des fichiers CSV
            moduleCSV.create_csv_files(df_copy, csv_directory, csv_file_name, first_rows, sample_rows, last_rows, nrows_value)

            local_file_path = f'./data/preparation/{html_file_name}'  # Déclaration de local_file_path

            # Appel de la fonction pour créer un fichier HTML à partir du DataFrame
            modulePreparationHTML.create_html_file(df_copy, html_file_name, nrows_value, start_time, files_dict, local_file_path, file_prefix='P_')

            # Appel de la fonction pour obtenir les informations du DataFrame
            moduleDataframe.get_dataframe_info(df_copy)

            # Appel de la fonction pour télécharger un fichier depuis une URL ou lire depuis un chemin local
            moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)

            # Appel de la fonction pour créer un DataFrame à partir du contenu du fichier
            moduleDataframe.create_dataframe(content, separator, nrows_value)

```

**Explications :**

1. Le script commence par importer différents modules nécessaires et définir des variables, telles que le dictionnaire `files_dict` qui contient les détails des fichiers à traiter et le préfixe pour les fichiers HTML et CSV.
2. Ensuite, il parcourt chaque fichier spécifié dans le dictionnaire et effectue une série d'opérations, y compris le téléchargement ou la lecture du fichier, la création d'un répertoire pour les fichiers CSV, la manipulation du DataFrame, la création de fichiers CSV, et enfin la création d'un fichier HTML avec des informations détaillées.

## preparation.titles.principals.csv.py

```python
import moduleOS
import moduleCSV
import moduleDownload
import moduleOS
import moduleDataframe
import modulePreparationHTML
import time
import pandas as pd
import numpy as np

# Dictionnaire avec les noms des fichiers, leurs emplacements, leur type de séparateur et le nombre de lignes
files_dict = {
    'title.principals.tsv': ('./data/sources', '\t', -1, 19, 20, 21)
}

# Préfixe pour les fichiers HTML et CSV
file_prefix = 'P_'

# Début du chronomètre
import time
start_time = time.time()

# Parcours du dictionnaire de fichiers
for file_name, (path, separator, nrows_value, first_rows, sample_rows, last_rows) in files_dict.items():
    # Téléchargement ou lecture du contenu du fichier
    content = moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)
    
    # Appel de la fonction pour créer le répertoire des fichiers .csv
    csv_directory = './data/preparation'
    moduleOS.create_csv_directory(csv_directory)
    
    if content is not None:
        # Création d'un DataFrame original à partir du contenu du fichier
        df_original = moduleDataframe.create_dataframe(content, separator, nrows_value)  
        
        if df_original is not None:
            # Création d'une copie du DataFrame original pour les manipulations
            df_copy = df_original.copy()

            # Remplacement de toutes les occurrences de "\\N" par NaN dans le DataFrame en utilisant un caractère d'échappement
            df_copy = df_copy.replace('\\N', None)

            # Suppression des colonnes 'ordering', 'job', 'characters'
            columns_to_drop = ['ordering', 'job', 'characters']
            df_copy = df_copy.drop(columns=columns_to_drop)

            # Renommage de la colonne 'nconst' en 'nconst_tp'
            df_copy = df_copy.rename(columns={'nconst': 'nconst_tp'})

            # Réinitialiser les index si nécessaire
            # df_copy.reset_index(drop=True, inplace=True)    

            # Création des noms des fichiers avec le préfixe
            csv_file_name = f'{file_prefix}{file_name}.csv'
            html_file_name = f'{file_prefix}{file_name}.html'

            # Appel de la fonction pour créer des fichiers CSV
            moduleCSV.create_csv_files(df_copy, csv_directory, csv_file_name, first_rows, sample_rows, last_rows, nrows_value)

            local_file_path = f'./data/preparation/{html_file_name}'  # Déclaration de local_file_path

            # Appel de la fonction pour créer un fichier HTML à partir du DataFrame
            modulePreparationHTML.create_html_file(df_copy, html_file_name, nrows_value, start_time, files_dict, local_file_path, file_prefix='P_')

            # Appel de la fonction pour obtenir les informations du DataFrame
            moduleDataframe.get_dataframe_info(df_copy)

            # Appel de la fonction pour télécharger un fichier depuis une URL ou lire depuis un chemin local
            moduleDownload.download_or_read_file(file_name, path, separator, nrows_value)

            # Appel de la fonction pour créer un DataFrame à partir du contenu du fichier
            moduleDataframe.create_dataframe(content, separator, nrows_value)

```

**Explications :**

1. Le script commence par importer différents modules nécessaires et définir des variables, telles que le dictionnaire `files_dict` qui contient les détails des fichiers à traiter et le préfixe pour les fichiers HTML et CSV.
2. Ensuite, il parcourt chaque fichier spécifié dans le dictionnaire et effectue une série d'opérations, y compris le téléchargement ou la lecture du fichier, la création d'un répertoire pour les fichiers CSV, la manipulation du DataFrame, la création de fichiers CSV, et enfin la création d'un fichier HTML avec des informations détaillées.

# Fusion

## fusion.dataframes.py

```python
# Importation des modules nécessaires
import os
import pandas as pd

# Dictionnaire avec les noms des fichiers, leurs emplacements, leur type de séparateur et le nombre de lignes
files_dict = {
    'P_title.basics.tsv.csv_explore.csv': ('./data/preparation', ',', -1),
    'P_name.basics.tsv.csv_explore.csv': ('./data/preparation', ',', -1),
    'P_title.principals.tsv.csv_explore.csv': ('./data/preparation', ',', -1),
    'P_title.crew.tsv.csv_explore.csv': ('./data/preparation', ',', -1),
    'P_complementaire_tmdb_full.csv.csv_explore.csv': ('./data/preparation', ',', -1),
    'P_title.akas.tsv.csv_explore.csv': ('./data/preparation', ',', -1),
    'P_title.ratings.tsv.csv_explore.csv': ('./data/preparation', ',', -1),
    # 'P_title.episode.tsv.csv_explore.csv': ('./data/preparation', ',', -1)
}

# Préfixe pour le fichier CSV
file_prefix = 'F_'

merged_data = None  # Initialisation du DataFrame fusionné à zéro.

# Fusion des DataFrames en utilisant la colonne tconst
for file_name, (directory, separator, lines, *_) in files_dict.items():
    file_path = os.path.join(directory, file_name)
    try:
        if os.path.exists(file_path):
            if lines == -1:
                # Lecture du fichier CSV avec toutes les lignes si le nombre est à -1
                df = pd.read_csv(file_path, sep=separator)
            else:
                # Lecture du fichier CSV avec le nombre de lignes spécifié s'il est différent de -1
                df = pd.read_csv(file_path, sep=separator, nrows=lines)
            
            if merged_data is None:
                # Si c'est le premier fichier, on l'assigne directement à merged_data
                merged_data = df
                print(f"Merging {file_name}...")
            else:
                # Si ce n'est pas le premier fichier, on le fusionne avec merged_data en utilisant la colonne 'tconst'
                print(f"Merging {file_name}...")
                merged_data = merged_data.merge(df, on='tconst', how='inner')
                print(merged_data.head())
    except Exception as e:
        print(f"Erreur lors de la lecture de {file_name}: {e}")

# Fichier de sortie
output_filename = f"{file_prefix}merged_data.csv"

# Chemin complet du fichier de sortie en utilisant os.path.join
output_file_path = os.path.join(directory, output_filename)

# Enregistrement du DataFrame fusionné dans un fichier CSV
print(f"Le fichier CSV est en cours de génération...")
merged_data.to_csv(output_file_path, index=False)

print(f"Le fichier CSV a été enregistré sous : {output_file_path}")

```

**Explications :**

1. Le code commence par définir un dictionnaire (`files_dict`) contenant les noms des fichiers, leurs emplacements, le type de séparateur, et le nombre de lignes à lire (-1 pour tout lire).
2. Un préfixe pour le fichier de sortie (`file_prefix`) est défini.
3. Un DataFrame (`merged_data`) est initialisé à `None` pour stocker les données fusionnées.
4. Les fichiers CSV sont lus un par un, et s'ils existent, ils sont fusionnés dans le DataFrame `merged_data` en utilisant la colonne 'tconst'.
5. Le DataFrame fusionné est ensuite enregistré dans un fichier CSV avec un nom généré à partir du préfixe défini.
6. Des messages sont affichés pour informer l'utilisateur de l'avancement du processus.

## fusion.affinement.py

```python
# Importer la bibliothèque pandas sous l'alias 'pd'
import pandas as pd

# Spécifier le chemin du fichier CSV à lire
link = "./data/preparation/F_merged_data.csv"

# Ouvrir le fichier CSV en tant que DataFrame en spécifiant le séparateur (',') et l'encodage ('UTF-8')
# Utiliser low_memory=False pour éviter les avertissements liés à la mémoire lors de la lecture de grands fichiers
df = pd.read_csv(link, sep=',', encoding='UTF-8', low_memory=False)

# Sélectionner les colonnes pour chaque catégorie et ajouter les préfixes correspondants
selected_columns_ge = ['Action', 'Adult', 'Adventure', 'Animation', 'Biography', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Family', 'Fantasy', 'Film-Noir', 'History', 'Horror', 'Music', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Sport', 'Thriller', 'War', 'Western']
selected_columns_ti = ['titleType', 'primaryTitle', 'startYear', 'runtimeMinutes', 'poster_path', 'tagline', 'overview']
selected_columns_pe = ['primaryName', 'nconst_tp', 'nconst_nb', 'directors', 'writers']
selected_columns_pk = ['category', 'actor', 'actress', 'animation_department', 'art_department', 'art_director', 'assistant', 'assistant_director', 'camera_department', 'casting_department', 'casting_director', 'cinematographer', 'composer', 'costume_department', 'costume_designer', 'director', 'editor', 'editorial_department', 'electrical_department', 'executive', 'legal', 'location_management', 'make_up_department', 'manager', 'miscellaneous', 'music_artist', 'music_department', 'podcaster', 'producer', 'production_department', 'production_designer', 'production_manager', 'publicist', 'script_department', 'set_decorator', 'sound_department', 'soundtrack', 'special_effects', 'stunts', 'talent_agent', 'transportation_department', 'visual_effects', 'writer']
selected_columns_ra = ['popularity', 'title', 'averageRating', 'numVotes']

# Préfixes à ajouter aux noms de colonnes correspondants à chaque catégorie
prefix_ge = 'GE_'
prefix_ti = 'TI_'
prefix_pe = 'PE_'
prefix_pk = 'PK_'
prefix_ra = 'RA_'

# Renommer les colonnes en ajoutant le préfixe correspondant en fonction de la catégorie de la colonne
df.rename(columns=lambda x: prefix_ge + x if x in selected_columns_ge else
                          prefix_ti + x if x in selected_columns_ti else
                          prefix_pe + x if x in selected_columns_pe else
                          prefix_pk + x if x in selected_columns_pk else
                          prefix_ra + x if x in selected_columns_ra else x, inplace=True)

# Enregistrer le DataFrame final en tant que fichier CSV
df.to_csv('./data/preparation/F2_merged_data.csv', index=False, encoding='UTF-8')

```

**Explications :**

1. Le code commence par importer la bibliothèque pandas sous l'alias 'pd'.
2. Il spécifie le chemin du fichier CSV à lire (`link`) et ouvre le fichier en tant que DataFrame (`df`) en utilisant `pd.read_csv()`.
3. Les colonnes pour chaque catégorie sont sélectionnées dans des listes distinctes (`selected_columns_ge`, `selected_columns_ti`, etc.).
4. Des préfixes correspondants sont définis pour chaque catégorie (`prefix_ge`, `prefix_ti`, etc.).
5. Les colonnes du DataFrame sont renommées en ajoutant les préfixes appropriés en fonction de leur catégorie.
6. Enfin, le DataFrame modifié est enregistré en tant que fichier CSV sous un nouveau nom (`F2_merged_data.csv`) avec l'encodage 'UTF-8'.

# Modules

## moduleAnalyseHTML.py

```python
# Importation du module moduleDataframe pour obtenir des informations sur le DataFrame
import moduleDataframe
# Importation de la bibliothèque time pour mesurer le temps de génération du rapport
import time

# Fonction pour créer un fichier HTML à partir du DataFrame
def create_html_file(df, file_name, nrows_value, start_time, files_dict, local_file_path, file_prefix=''):
    # Importation des modules nécessaires
    import psutil
    import os
    import moduleFTP

    # Obtention des informations sur le DataFrame
    info_output = moduleDataframe.get_dataframe_info(df)

    # Si des informations sont disponibles, les formatter pour les inclure dans le fichier HTML
    if info_output is not None:
        info_output = info_output.replace('\n', '<br>')
    else:
        info_output = "Aucune information disponible"
    
    # Enlever le préfixe du nom du fichier et l'extension ".html"
    file_name = file_name.replace(file_prefix, '').replace('.html', '')

    # Chemin absolu du répertoire du script Python
    script_directory = os.path.dirname(os.path.abspath(__file__))

    # Chemin absolu du fichier CSS en utilisant le répertoire du script
    css_file_path = os.path.join(script_directory, "style.css")

    # Lecture du contenu du fichier CSS
    css_content = ""
    with open(css_file_path, 'r') as css_file:
        css_content = css_file.read()

    # Récupération du nombre de premières lignes, lignes au hasard et dernières lignes à afficher
    first_rows = files_dict[file_name][3]  # x premières lignes
    sample_rows = files_dict[file_name][4]  # x lignes au hasard
    last_rows = files_dict[file_name][5]  # x dernières lignes

    # Structure du fichier HTML
    html_content = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <title>{file_name}</title>
        <style>
        {css_content}  # Intégrer le contenu CSS lu depuis le fichier
        </style>
    </head>
    <body>
        <h2>{file_name}</h2>
        <p>L'analyse porte sur les <b>{nrows_value}</b> premières lignes du dataframe</p>
        <!-- Informations sur les ressources -->
        <div>
            <h3>Informations sur les ressources :</h3>
            <p>Temps de génération : {round(time.time() - start_time)} secondes</p>
            <p>Utilisation CPU : {psutil.cpu_percent(interval=None, percpu=True)} %</p>
            <p>Utilisation mémoire : {round((psutil.virtual_memory().used) / (1024 * 1024))} Go</p>
            <p>Processeur : {psutil.cpu_freq().current} MHz</p>
        </div>  
        <!-- Informations sur le dataframe -->
        <div>
            <h3>Informations sur le DataFrame ( df.info() ):</h3> 
            <table>
                {info_output}
            </table>
        </div>  
        <div>            
            <h3>Noms des colonnes ( df.columns ) :</h3> 
            <table>
                <tr><td>{', '.join(df.columns)}</td></tr>
            </table>
        </div>  
        <div>            
            <h3>Types de données des colonnes ( df.dtypes ):</h3> 
            <table>
                {df.dtypes.to_frame().to_html(classes='table table-bordered', header=False)}
            </table>
        </div>  
        <div>            
            <h3>Statistiques descriptives pour les colonnes numériques ( df.describe() ):</h3> 
            <table>
                {df.describe().to_html(classes='table table-bordered', header=False)}
            </table>
        </div>  
        <div>            
            <h3>Valeurs manquantes dans le Dataframe ( df.isnull().sum() ) :</h3>
            <table>
                {df.isnull().sum().to_frame().to_html(classes='table table-bordered', header=False)}
            </table>
        </div>  
        <div>
            <h3>{first_rows} premières lignes :</h3>
            <table> 
                {df.head(first_rows).to_html(index=False, escape=False, classes='table table-bordered')}
            </table>
            <h3>{sample_rows} lignes au hasard :</h3>
            <table> 
                {df.sample(sample_rows).to_html(index=False, escape=False, classes='table table-bordered')}
            </table>
            <h3>{last_rows} dernières lignes :</h3>
            <table> 
                {df.tail(last_rows).to_html(index=False, escape=False, classes='table table-bordered')}
            </table>
        </div>
    </body>
    </html>
    """

    # Écriture du contenu HTML dans le fichier
    with open(local_file_path, 'w', encoding='UTF-8') as f:
        f.write(html_content)
    
    print(f"Exporté en HTML : {local_file_path}")

```

**Explications :**

1. Le code utilise la bibliothèque `psutil` pour obtenir des informations sur l'utilisation des ressources du système (CPU, mémoire, etc.).
2. Il utilise également le module `moduleDataframe` pour obtenir des informations sur le DataFrame, notamment le résumé des données, les noms des colonnes, les types de données, les statistiques descriptives, et le nombre de valeurs manquantes.
3. La fonction `create_html_file` prend un DataFrame, un nom de fichier, le nombre de lignes à inclure dans le rapport, le temps de début de génération, un dictionnaire spécifiant les paramètres pour chaque fichier, le chemin local pour sauvegarder le fichier HTML, et un préfixe optionnel pour le nom de fichier. Elle génère un fichier HTML complet avec des informations sur le DataFrame et l'utilisation des ressources système.

## moduleCSV.py

```python
# Importation du module moduleOS pour gérer les opérations sur le système de fichiers
import moduleOS
# Importation du module os pour des opérations liées au système d'exploitation
import os

# Fonction pour créer différents fichiers CSV à partir d'un DataFrame
def create_csv_files(df, csv_directory, file_name, first_rows, sample_rows, last_rows, nrows_value):
    # Importation du module moduleOS pour créer le répertoire CSV si nécessaire
    moduleOS.create_directory(csv_directory)

    # Si first_rows est spécifié, créer un fichier CSV avec les x premières lignes
    if first_rows > 0:
        csv_file_path_explore = os.path.join(csv_directory, f'{file_name}_explore.csv')
        df.head(nrows_value).to_csv(csv_file_path_explore, index=False, encoding='UTF-8')
        print(f"Exporté en CSV : {csv_file_path_explore}")

    # Si first_rows est spécifié, créer un fichier CSV avec les x premières lignes
    if first_rows > 0:
        csv_file_path_head = os.path.join(csv_directory, f'{file_name}_head.csv')
        df.head(first_rows).to_csv(csv_file_path_head, index=False, encoding='UTF-8')
        print(f"Exporté en CSV : {csv_file_path_head}")

    # Si sample_rows est spécifié, créer un fichier CSV avec un échantillon aléatoire de 50% des données
    if sample_rows > 0:
        csv_file_path_sample = os.path.join(csv_directory, f'{file_name}_sample.csv')
        df.sample(frac=0.50).to_csv(csv_file_path_sample, index=False, encoding='UTF-8')
        print(f"Exporté en CSV : {csv_file_path_sample}")

    # Si sample_rows est spécifié, créer un fichier CSV avec un échantillon aléatoire de 10% des données
    if sample_rows > 0:
        csv_file_path_big_sample = os.path.join(csv_directory, f'{file_name}_big_sample.csv')
        df.sample(frac=0.10).to_csv(csv_file_path_big_sample, index=False, encoding='UTF-8')
        print(f"Exporté en CSV : {csv_file_path_big_sample}")

    # Si last_rows est spécifié, créer un fichier CSV avec les x dernières lignes
    if last_rows > 0:
        csv_file_path_tail = os.path.join(csv_directory, f'{file_name}_tail.csv')
        df.tail(last_rows).to_csv(csv_file_path_tail, index=False, encoding='UTF-8')
        print(f"Exporté en CSV : {csv_file_path_tail}")

```

**Explications :**

1. La fonction `create_csv_files` prend en paramètre un DataFrame (`df`), un répertoire de destination pour les fichiers CSV (`csv_directory`), un nom de fichier (`file_name`), le nombre de premières lignes à inclure dans certains fichiers CSV (`first_rows`), le nombre de lignes à inclure dans un échantillon aléatoire (`sample_rows`), le nombre de dernières lignes à inclure (`last_rows`), et un nombre de lignes dans le DataFrame (`nrows_value`).
2. La fonction utilise le module `moduleOS` pour créer le répertoire de destination CSV si nécessaire.
3. Elle crée plusieurs fichiers CSV selon les spécifications (exploration, premières lignes, échantillons aléatoires, dernières lignes) en utilisant la méthode `to_csv` de pandas.
4. Les noms de fichiers sont générés en fonction des paramètres passés à la fonction, et chaque opération de création de fichier est suivie d'une impression indiquant le succès de l'opération et le chemin du fichier généré.

## moduleDataframe.py

```python
# Importation du module sys pour accéder à des fonctionnalités spécifiques du système
import sys
# Importation de la bibliothèque pandas sous l'alias 'pd'
import pandas as pd
# Importation de la classe StringIO du module io pour traiter les chaînes de caractères comme des fichiers
from io import StringIO

# Fonction pour obtenir des informations sur un DataFrame
def get_dataframe_info(df):
    # Sauvegarde de la sortie standard actuelle
    original_stdout = sys.stdout
    # Redirection de la sortie standard vers un objet StringIO pour capturer les informations
    sys.stdout = StringIO()
    # Appel à la méthode info() du DataFrame pour obtenir des informations sur sa structure
    df.info()
    # Récupération de la sortie capturée dans la variable info_output
    info_output = sys.stdout.getvalue()
    # Restauration de la sortie standard d'origine
    sys.stdout = original_stdout
    # Si aucune information n'a été capturée, renvoyer un message indiquant l'absence d'informations
    if info_output.strip() == '':
        return "Aucune information disponible"
    # Sinon, renvoyer les informations capturées
    return info_output

# Fonction pour créer un DataFrame à partir d'un contenu CSV
def create_dataframe(content, separator, nrows_value):
    try:
        # Si nrows_value est -1, lire l'intégralité du contenu CSV, sinon lire seulement le nombre de lignes spécifiées dans nrows_value
        if nrows_value == -1:
            df = pd.read_csv(StringIO(content), sep=separator, low_memory=False, encoding='UTF-8')
        else:
            df = pd.read_csv(StringIO(content), sep=separator, nrows=nrows_value, low_memory=False, encoding='UTF-8')
        # Renvoyer le DataFrame créé
        return df
    # Gestion des erreurs liées à la lecture du fichier CSV
    except pd.errors.ParserError as e:
        # Affichage d'un message d'erreur en cas d'échec de la lecture
        print(f"Erreur lors de la lecture du fichier: {e}")
        # Renvoyer None pour indiquer un échec de la création du DataFrame
        return None

```

**Explications :**

1. **get_dataframe_info(df):**
   - La fonction `get_dataframe_info` prend un DataFrame (`df`) en entrée.
   - Elle utilise la méthode `info()` de pandas pour obtenir des informations détaillées sur le DataFrame, telles que les types de données, les valeurs non nulles, la consommation de mémoire, etc.
   - Les informations sont capturées en redirigeant la sortie standard vers un objet `StringIO`, puis en restaurant la sortie standard d'origine.
   - Si aucune information n'est disponible, la fonction retourne un message indiquant cela.
2. **create_dataframe(content, separator, nrows_value):**
   - La fonction `create_dataframe` prend en entrée une chaîne de caractères CSV (`content`), un séparateur de colonnes (`separator`), et le nombre de lignes à lire (`nrows_value`).
   - Elle utilise la bibliothèque pandas pour créer un DataFrame à partir de la chaîne de caractères CSV.
   - En cas d'erreur lors de la lecture du fichier CSV, la fonction imprime l'erreur et retourne `None`.
   - Si la lecture réussit, elle retourne le DataFrame créé.

## moduleDownload.py

```python
# Fonction pour télécharger un fichier depuis une URL ou lire un fichier localement
def download_or_read_file(file_name, path, separator, nrows_value):
    # Importation du module requests pour effectuer des requêtes HTTP
    import requests
    # Importation de la classe StringIO du module io pour traiter les chaînes de caractères comme des fichiers
    from io import StringIO

    # Vérification si le chemin commence par 'http' (URL)
    if path.startswith('http'):
        # Si le chemin est une URL, téléchargez le fichier depuis le site web
        file_url = path + file_name
        # Effectuer une requête HTTP pour obtenir le contenu du fichier
        response = requests.get(file_url)
        # Décoder le contenu de la réponse en utilisant l'encodage UTF-8 
        # Utile si le fichier est hébergé sur un serveur utilisant l'encodage 8859-1 par exemple
        content = response.content.decode('utf-8')
    else:
        # Si le chemin est en local, on lit le fichier à partir du chemin local
        local_file_path = f'{path}/{file_name}'
        # Ouverture du fichier en mode lecture avec l'encodage UTF-8
        with open(local_file_path, 'r', encoding='UTF-8') as file:
            # Lire le contenu du fichier
            content = file.read()
    
    # Retourner le contenu du fichier
    return content

```

**Explications :**

1. La fonction `download_or_read_file` prend en paramètre le nom du fichier (`file_name`), le chemin local ou l'URL du fichier (`path`), un séparateur (`separator`), et le nombre de lignes à lire (`nrows_value`).

2. Si le chemin commence par 'http', la fonction considère que c'est une URL et utilise la bibliothèque `requests` pour télécharger le contenu du fichier depuis le site web. 

   Si le chemin n'est pas une URL, la fonction lit le fichier localement en utilisant la bibliothèque intégrée `open`.

3. La fonction renvoie le contenu du fichier sous forme de chaîne de caractères, prêt à être utilisé pour la création d'un DataFrame ou d'autres opérations de traitement des données.

## moduleFTP.py

```python
# Fonction pour transférer un fichier vers le serveur FTP
def upload_file_to_ftp(local_file_path, remote_file_path, ftp):
    # Ouvrir le fichier local en mode lecture binaire ('rb')
    with open(local_file_path, 'rb') as local_file:
        # Utiliser la méthode storbinary() de l'objet FTP pour transférer le fichier
        # 'STOR' est une commande FTP qui signifie "Store a file on the remote machine."
        # Elle est suivie du chemin distant et du fichier local à stocker
        ftp.storbinary('STOR ' + remote_file_path, local_file)

```

**Explications  :**

1. La fonction `upload_file_to_ftp` prend en paramètre le chemin local du fichier à transférer (`local_file_path`), le chemin distant où le fichier sera stocké sur le serveur FTP (`remote_file_path`), et un objet FTP préalablement configuré (`ftp`).

2. La fonction utilise la commande FTP `STOR` pour transférer le fichier vers le serveur FTP. 

   La méthode `storbinary` est utilisée pour effectuer cette opération. 

   Le mode 'rb' est utilisé lors de l'ouverture du fichier local pour s'assurer que le transfert est effectué en mode binaire.

3. Cette fonction est utile lorsque l'on a un fichier local que l'on souhaite le transférer vers un serveur FTP distant. 

   Elle facilite le processus de mise à jour de fichiers sur le serveur distant à partir d'une machine locale.

## moduleOS.py

```python
# Fonction pour créer un répertoire
def create_directory(directory_path):
    # Importation du module os pour effectuer des opérations sur le système de fichiers
    import os
    # Utilisation de la fonction makedirs de os pour créer le répertoire
    # exist_ok=True permet de ne pas générer d'erreur si le répertoire existe déjà
    os.makedirs(directory_path, exist_ok=True)

# Fonction pour créer un répertoire pour stocker des fichiers CSV
def create_csv_directory(directory_path):
    # Importation du module os pour effectuer des opérations sur le système de fichiers
    import os
    """
    Crée un répertoire pour stocker les fichiers .csv s'il n'existe pas.
    Args:
        directory_path (str): Le chemin du répertoire.
    """
    # Utilisation de la fonction makedirs de os pour créer le répertoire
    # exist_ok=True permet de ne pas générer d'erreur si le répertoire existe déjà
    os.makedirs(directory_path, exist_ok=True)
```

**Explications :**

1. La fonction `create_directory` prend en paramètre le chemin du répertoire à créer (`directory_path`).
2. La fonction utilise la fonction `makedirs` du module `os` pour créer le répertoire spécifié. L'argument `exist_ok=True` permet d'ignorer les erreurs si le répertoire existe déjà.
3. La fonction `create_csv_directory` fait la même chose que `create_directory`.

## modulePreparationHTML.py

```python
import moduleDataframe  # Importation du module pour obtenir des informations sur le DataFrame
import time
import os
import psutil
import moduleFTP  # Importation du module FTP (peut être utilisé pour le transfert vers un serveur FTP)

# Fonction pour créer un fichier HTML à partir du DataFrame
def create_html_file(df, file_name, nrows_value, start_time, files_dict, local_file_path, file_prefix=''):
    # Obtention des informations sur le DataFrame
    info_output = moduleDataframe.get_dataframe_info(df)

    # Si des informations sont disponibles, remplacer les sauts de ligne par des balises <br>
    if info_output is not None:
        info_output = info_output.replace('\n', '<br>')
    else:
        info_output = "Aucune information disponible"

    # Suppression du préfixe du nom du fichier et de l'extension .html
    file_name = file_name.replace(file_prefix, '').replace('.html', '')

    # Chemin absolu du répertoire du script Python
    script_directory = os.path.dirname(os.path.abspath(__file__))

    # Chemin absolu du fichier CSS en utilisant le répertoire du script
    css_file_path = os.path.join(script_directory, "style.css")

    # Lecture du contenu du fichier CSS
    css_content = ""
    with open(css_file_path, 'r') as css_file:
        css_content = css_file.read()

    # Récupération des valeurs spécifiées dans le dictionnaire de fichiers
    first_rows = files_dict[file_name][3]  # x premières lignes
    sample_rows = files_dict[file_name][4]  # x lignes au hasard
    last_rows = files_dict[file_name][5]  # x dernières lignes

    # Structure du fichier HTML
    html_content = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <title>{file_name}</title>
        <style>
        {css_content}  # Intégrer le contenu CSS lu depuis le fichier
        </style>
    </head>
    <body>
        <h2>{file_name}</h2>
        <p>L'analyse porte sur les <b>{nrows_value}</b> premières lignes du dataframe</p>
        <!-- Informations sur les ressources -->
        <div>
            <h3>Informations sur les ressources :</h3>
            <p>Temps de génération : {round(time.time() - start_time)} secondes</p>
            <p>Utilisation CPU : {psutil.cpu_percent(interval=None, percpu=True)} %</p>
            <p>Utilisation mémoire : {round((psutil.virtual_memory().used) / (1024 * 1024))} Go</p>
            <p>Processeur : {psutil.cpu_freq().current} MHz</p>
        </div>  
        <!-- Informations sur le dataframe -->
        <div>
            <h3>Informations sur le DataFrame ( df.info() ):</h3> 
            <table>
                {info_output}
            </table>
        </div>  
        <div>            
            <h3>Noms des colonnes ( df.columns ) :</h3> 
            <table>
                <tr><td>{', '.join(df.columns)}</td></tr>
            </table>
        </div>  
        <div>            
            <h3>Types de données des colonnes ( df.dtypes ):</h3> 
            <table>
                {df.dtypes.to_frame().to_html(classes='table table-bordered', header=False)}
            </table>
        </div>  
        <div>            
            <h3>Statistiques descriptives pour les colonnes numériques ( df.describe() ):</h3> 
            <table>
                {df.describe().to_html(classes='table table-bordered', header=False)}
            </table>
        </div>  
        <div>            
            <h3>Valeurs manquantes dans le Dataframe ( df.isnull().sum() ) :</h3>
            <table>
                {df.isnull().sum().to_frame().to_html(classes='table table-bordered', header=False)}
            </table>
        </div>  
        <div>
            <h3>{first_rows} premières lignes :</h3>
            <table> 
                {df.head(first_rows).to_html(index=False, escape=False, classes='table table-bordered')}
            </table>
            <h3>{sample_rows} lignes au hasard :</h3>
            <table> 
                {df.sample(frac=0.10).to_html(index=False, escape=False, classes='table table-bordered')}
            </table>
            <h3>{last_rows} dernières lignes :</h3>
            <table> 
                {df.tail(last_rows).to_html(index=False, escape=False, classes='table table-bordered')}
            </table>
        </div>
    </body>
    </html>
    """

    # Écriture du contenu HTML dans le fichier
    with open(local_file_path, 'w', encoding='UTF-8') as f:
        f.write(html_content)
    
    # Affichage du chemin du fichier HTML exporté
    print(f"Exporté en HTML : {local_file_path}")

```

**Explications :**

1. La fonction `create_html_file` prend un DataFrame (`df`), le nom du fichier (`file_name`), le nombre de lignes à analyser (`nrows_value`), le temps de début de la génération (`start_time`), un dictionnaire de fichiers (`files_dict`), le chemin local pour enregistrer le fichier HTML (`local_file_path`), et un préfixe (`file_prefix`).
2. Elle utilise différentes bibliothèques telles que `psutil` pour obtenir des informations système, `os` pour les opérations sur le système

# Utilitaires

## utiliitaire.export.code.py

```python
import os

# Dossier contenant les fichiers
dossier_scripts = "./src"

# Liste des noms de fichiers
noms_de_fichiers = ["preparation.title.basics.csv.py","moduleCSV.py"]

# Ouvrir un fichier de sortie en mode écriture
with open("exported_scripts.txt", "w") as fichier_export:

    # Parcours de la liste des noms de fichiers
    for nom_fichier in noms_de_fichiers:
        chemin_complet = os.path.join(dossier_scripts, nom_fichier)

        try:
            # Ouvrerture du fichier en mode lecture
            with open(chemin_complet, "r") as fichier_source:
                # Lecture du contenu du fichier
                contenu = fichier_source.read()

            # Écriture du nom du script dans le fichier de sortie
            fichier_export.write(f"Nom du script : {nom_fichier}\n")
            
            # Écriture du contenu du script dans le fichier de sortie
            fichier_export.write("Contenu du script :\n")
            fichier_export.write(contenu)
            fichier_export.write("\n\n")

        except FileNotFoundError:
            print(f"Le fichier {nom_fichier} n'a pas été trouvé dans le dossier {dossier_scripts}.")

print("Exportation terminée.")

```

**Explications  :**

1. Le script commence par définir le chemin du dossier contenant les fichiers (`dossier_scripts`) et la liste des noms de fichiers à traiter (`noms_de_fichiers`).
2. Il ouvre un fichier de sortie (`exported_scripts.txt`) en mode écriture, pour stocker les informations extraites des scripts.
3. Ensuite, pour chaque nom de fichier dans la liste, il construit le chemin complet en utilisant `os.path.join` .
4. Dans un bloc `try-except`, le script tente d'ouvrir chaque fichier, lit son contenu, puis écrit le nom du script suivi de son contenu dans le fichier de sortie.
5. Si un fichier n'est pas trouvé (lève une exception `FileNotFoundError`), le script affiche un message indiquant que le fichier n'a pas été trouvé dans le dossier spécifié.
6. Le script affiche "Exportation terminée" une fois que toutes les opérations sont effectuées avec succès.

## utilitaire.preparation.batch.py

```python
import subprocess

# Dictionnaire définissant les scripts à traiter avec leurs propriétés associées
files_to_process = {
    "./preparation.complementaire_tmdb_full.csv.py": {"process": True, "execution_mode": "run", "order": 1},
    "./preparation.name.basics.csv.py": {"process": True, "execution_mode": "run", "order": 2},
    "./preparation.title.akas.csv.py": {"process": True, "execution_mode": "run", "order": 3},
    "./preparation.title.basics.csv.py": {"process": True, "execution_mode": "run", "order": 4},
    "./preparation.title.crew.csv.py": {"process": True, "execution_mode": "run", "order": 5},
    "./preparation.title.episode.csv.py": {"process": True, "execution_mode": "run", "order": 6},
    "./preparation.titles.principals.csv.py": {"process": True, "execution_mode": "run", "order": 7},
    "./preparation.title.ratings.csv.py": {"process": True, "execution_mode": "run", "order": 8},
    "./fusion.dataframes.py": {"process": True, "execution_mode": "run", "order": 9}
}

# Triage des scripts en fonction de leur numéro d'ordre.
sorted_scripts = sorted(files_to_process.items(), key=lambda x: x[1]["order"])

# Parcours des scripts triés et exécution en fonction de leurs propriétés.
for script_name, script_info in sorted_scripts:
    if script_info["process"]:
        try:
            # Affiche un message indiquant le début de l'exécution du script.
            print(f"Début de l'exécution du script {script_name} en mode '{script_info['execution_mode']}'.")
            
            # Vérifie le mode d'exécution et exécute le script en conséquence.
            if script_info["execution_mode"] == "run":
                # Exécution du script en utilisant subprocess.run.
                subprocess.run(["python", script_name])
            elif script_info["execution_mode"] == "Popen":
                # Exécution du script en utilisant subprocess.Popen.
                subprocess.Popen(["python", script_name])
            
            # Affiche un message indiquant la fin de l'exécution du script.
            print(f"Fin de l'exécution du script {script_name}.")
        
        except subprocess.CalledProcessError as e:
            # Gestion des erreurs lors de l'exécution des scripts.
            print(f"Erreur lors de l'exécution de {script_name}: {e}")
    
    else:
        # Si le script ne doit pas être traité, affiche un message correspondant.
        print(f"Le script {script_name} ne doit pas être traité.")

```

**Explications  :**

1. Le script définit un dictionnaire (`files_to_process`) contenant des informations sur les scripts à traiter, notamment s'ils doivent être traités, le mode d'exécution, et leur numéro d'ordre.
2. Les scripts sont triés en fonction de leur ordre d'exécution.
3. Ensuite, le script parcourt les scripts triés et exécute chaque script selon ses propriétés. On peut utiliser `subprocess.run` ou `subprocess.Popen` pour lancer les scripts Python.
4. Le script affiche des messages sur le début et la fin de l'exécution de chaque script.
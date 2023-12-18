import pandas as pd
import math
import json
import warnings

'''
Explications :
La fonction create_column_id prend en entrée :
    - df : Nom du dataframe utilisé
    - prefix : Préfixe utilisé précédant la date
    - original_column : Nom de la colonne ou se trouve la date
    - target_column : Nom de la colonne à créer

Etapes du script :
    - Conversion de la colonne original_column en format date
    - Extraction de la date dans original_column et concaténation avec prefix pour la création de l'identifiant
    - Enregistrement de l'identifiant dans la colonne target_column
    - Déplacement de la nouvelle au début du dataframe

Exemple : create_column_id(df, prefix= 'degradations', original_column = 'measure_day', target_column = 'id')
'''
def create_column_id(df, prefix, original_column, target_column): 
    print()
    print(f"🛫 Création de la colonne en cours", end='')
    # print(df.head(5))
    # Conversion de la colonne en format datetime
    df[original_column] = pd.to_datetime(df[original_column], errors='coerce')
    print(f"\n✅ Conversion de la colonne {original_column} en format Date/Time", end='')

    # Extraction de la date au format YYYY-MM-DD et concaténation avec le préfixe
    df[target_column] = prefix + '_' + df[original_column].dt.strftime('%Y-%m-%d')
    print(f"\n✅ Concaténation de la date et du préfixe", end='')

    # Déplacement de target_column en première position
    target_column_series = df.pop(target_column)
    df.insert(0, target_column, target_column_series)
    print(f"\n✅ Réorganisaton des colonnes", end='')
    print(f"\n🛬 Création terminée")
    # print(df.head(5))

    return df

'''
Explications :
La fonction split_column_json prend en entrée :
    - df : Nom du dataframe utilisé
    - column_name : Nom de la colonne à diviser

Etapes du script :
    - Vérification de l'existence de la colonne
    - Division de la colonne "sensor_data" en colonnes distinctes avec pd.json_normalize().

Exemple : split_column_json(df, column_name = 'sensor_data')
'''
def split_column_json(df, column_name):
    warnings.simplefilter('ignore')
    
    # Vérification de l'existence de la colonne
    if column_name not in df.columns:
        print(f"\n❌ La colonne '{column_name}' n'existe pas.", end='')
        return df
    print(f"\n🛫 Division de la colonne {column_name} en cours", end='')
    # print(df.head(5))

    def extract_sensor_data(sensor_data):
        try:
            # Chargement des données JSON après remplacement des guillemets
            data = json.loads(sensor_data.replace("'", "\""))
            # 1. Extraction des températures, pressions et vibrations du dictionnaire JSON
            # 2. Suppression des unités '°C', 'hPa', 'm/s²'
            # 3. Conversion en nombre
            temp = int(data.get('temp').replace('°C', ''))
            pressure = int(data.get('pressure').replace('hPa', ''))
            vibrations = float(data.get('vibrations').replace('m/s²', ''))
            return temp, pressure, vibrations
        except Exception as e:
            print(f"\n❌ Erreur lors de l'extraction des données : {e}", end='')
            # Autant de none que de colonnes
            return None, None, None

    print(f"\n✅ Traitement des colonnes de températures, pressions et vibrations", end='')

    # Appliquer la fonction extract_sensor_data à la colonne sensor_data
    df[[column_name]] = df[[column_name]].applymap(extract_sensor_data)

    # Division de sensor_data en colonnes distinctes
    df[['temp_C', 'pressure_hPa', 'vibrations_ms2']] = pd.DataFrame(df[column_name].tolist(), index=df.index)
    print(f"\n✅ Division de sensor_data en colonnes distinctes", end='')

    # Suppression de la colonne
    print(f"\n✅ Suppression de la colonne sensor_data", end='')
    df = df.drop([column_name], axis=1)

    print(f"\n🛬 Division terminée")
    # print(df.head(5))

    return df

'''
Explications :
La fonction create_column prend en entrée :
    - df : Nom du dataframe utilisé
    - created_column : Nom de la colonne à créer
    - position_column : Position de la colonne

Etapes du script :
    - Création de la nouvelle colonne
    - Placement de la colonne 

Exemple : create_column(df, created_column= 'img', position_column = 2, default_value = 'none.png')
'''
def create_column(df, created_column, position_column, default_value):
    print(f"🛫 Création de la colonne en cours", end='')
    # print(df.head(5))

    # Vérification de la validité de la position
    if position_column < 0 or position_column > len(df.columns):
        raise ValueError("❌ Position de colonne invalide.")
    
    # Création d'une nouvelle colonne
    new_column = pd.Series()

    # On va utiliser 'pop' pour manipuler la nouvelle colonne
    if default_value is not None:
        # La nouvelle colonne créée aura une valeur par défaut
        df[created_column] = default_value
        print(f"\n✅ Assignation de la valeur par défaut ({default_value})", end='')
    else:
        # Sinon ce sera une valeur NaN
        print(f"\n✅ Assignation de la valeur NaN", end='')
        df[created_column] = pd.Series()

    # Réorganisation des colonnes
    columns = list(df.columns)
    columns.pop()
    columns.insert(position_column, created_column)
    df = df[columns]
    print(f"\n✅ Réorganisaton des colonnes", end='')

    print(f"\n🛬 Création terminée")
    # print(df.head(5))

    return df

'''
Explications :
La fonction create_column prend en entrée :
    - df : Nom du dataframe utilisé
    - original_column : Nom de la colonne à renommer.
    - renamed_column : Nouveau nom

Etapes du script :
    - Renommage de la colonne 

Exemple : rename_column(df, original_column= 'aero_linked', renamed_column = 'ref_aero')
'''
def rename_column(df, original_column, renamed_column):
    print(f"\n🛫 Renommage de la colonne en cours", end='')
    # print(df.head(5))

    # Vérification de l'existence de la colonne
    if original_column not in df.columns:
        raise ValueError(f"❌ La colonne '{original_column}' n'existe pas.")

    # Renommage de la colonne
    df.rename(columns={original_column: renamed_column}, inplace=True)

    print(f"\n✅ Renommage de la colonne", end='')

    print(f"\n🛬 Renommage terminé")
    # print(df.head(5))

    return df

'''
Explications :
La fonction rounded_column prend en entrée :
    - df : Nom du dataframe utilisé
    - original_column : Nom de la colonne à arrondir.
    - rounded_value : Choix de l'arrondi

Etapes du script :
    - Renommage de la colonne 

Exemple : rounded_column(df, original_column= 'taux_usure_actuel', rounded_value = 'sup')
'''
def rounded_column(df, original_column, rounded_value):
    print(f"\n🛫 Arrondissement des valeurs numériques en cours")
    # print(df.head(5))

    # Vérification de l'argument 'arrondi'
    if rounded_value not in ['inf', 'sup']:
        raise ValueError("❌ L'argument 'rounded_value' doit être 'inf' ou 'sup'.")

    # Vérification de l'existence de la colonne
    if original_column not in df.columns:
        raise ValueError(f"❌ La colonne '{original_column}' n'existe pas.")

    # Application de l'arrondi à la colonne
    if rounded_value == 'inf':
        df[original_column] = df[original_column].apply(lambda x: math.floor(x * 100) / 100)
        print(f"✅ Valeur arrondie à l'inférieur", end='')
    elif rounded_value == 'sup':
        df[original_column] = df[original_column].apply(lambda x: math.ceil(x * 100) / 100)
        print(f"✅ Valeur arrondie au supérieur", end='')
    
    print(f"\n🛬 Arrondissement terminé")
    # print(df.head(5))
    return df

'''
Explications :
La fonction drop_duplicates prend en entrée :
    - df : Nom du dataframe utilisé
    - original_column : Nom de la colonne à traiter.

Etapes du script :
    - Suppression des lignes dupliquées dans une colonne du dataframe 

Exemple : drop_duplicates(df, original_column = 'compo_concerned', inplace = 'true')
'''
def drop_duplicates(df, original_column, inplace):
    print(f"\n🛫 Suppression des lignes dupliquées en cours", end='')
    print(f"✅ Nombre de lignes : {len(df)}")
    # print(df.head(5))

    # Vérification de l'existence de la colonne
    if original_column not in df.columns:
        raise ValueError(f"❌ La colonne '{original_column}' n'existe pas.")

    # Suppression des lignes dupliquées
    if inplace:
        df.drop_duplicates(subset=[original_column], inplace=True)
        print(f"\n✅ Suppression des lignes dupliquées avec inplace = {inplace}", end='')
        print(f"\n🛬 Suppression terminée", end='')
        print(f"\n✅ Nombre de lignes : {len(df)}")
        # print(df.head(5))    
        return df
    else:
        print(f"\n✅ Suppression des lignes dupliquées avec inplace = {inplace}", end='')
        print(f"\n🛬 Suppression terminée", end='')
        print(f"\n✅ Nombre de lignes : {len(df)}")
        # print(df.head(5))  
        return df.drop_duplicates(subset=[original_column])

'''
Explications :
La fonction change_column_type prend en entrée :
    - df : Nom du dataframe utilisé
    - original_column : Nom de la colonne à traiter
    - new_type : Nouveau type de la colonne

Etapes du script :
    - Changement du type d'une colonne par un autre 

Exemple : change_column_type(df, original_column = 'compo_concerned', new_type = 'datetime64')
'''
def change_column_type(df, original_column, new_type):
    print(f"\n🛫 Changement du type de la colonne en cours", end='')
    # print(df.head(5))
    
    # Vérification de l'existence de la colonne
    if original_column not in df.columns:
        raise ValueError(f"❌ La colonne '{original_column}' n'existe pas.")

    # Changement du type de la colonne
    df[original_column] = df[original_column].astype(new_type)
    print(f"\n✅ Changement du type {df[original_column].dtype} de la colonne {original_column} par {new_type}", end='')
    print(f"\n🛬 Changement du type terminé")
    # print(df.head(5)) 

    return df

'''
Explications :
La fonction fillna_column prend en entrée :
    - df : Nom du dataframe utilisé
    - original_column : Nom de la colonne à traiter
    - type_NaN : type de valeur à utiliser pour les valeurs manquantes ('NaN' ou 'NaT')

Etapes du script :
    - Remplissage d'une colonne par  

Exemple : fillna_column(df, original_column = 'compo_concerned', type_NaN = 'NaT')
'''
def fillna_column(df, original_column, type_NaN='NaN'):
    print(f"\n🛫 Remplissage de colonne avec des valeurs {type_NaN}", end='')
    # print(df.head(5))

    # Vérification de l'existance de la colonne
    if original_column not in df.columns:
        raise ValueError(f"❌ La colonne '{original_column}' n'existe pas.")

    # Remplir les valeurs manquantes en fonction du type spécifié
    if type_NaN == 'NaN':
        df[original_column].fillna('NaN', inplace=True)
        print(f"\n✅ Remplissage de la colonne {original_column} avec {type_NaN}", end='')
    elif type_NaN == 'NaT':
        df[original_column].fillna(pd.NaT, inplace=True)
        print(f"\n✅ Remplissage de la colonne {original_column} avec {type_NaN}", end='')
    
    print(f"\n🛬 Remplissage terminé")
    # print(df.head(5))

    return df
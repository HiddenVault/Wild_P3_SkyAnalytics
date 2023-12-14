import pandas as pd
import json
import warnings

'''
Explications :
La fonction create_column_id prend en entrée :
    - df : Nom du dataframe utilisé
    - prefix : Préfixe utilisé précédant la date
    - origin_column : Nom de la colonne ou se trouve la date
    - target_column : Nom de la colonne à créer

Etapes du script :
    - Conversion de la colonne origin_column en format date
    - Extraction de la date dans origin_column et concaténation avec prefix pour la création de l'identifiant
    - Enregistrement de l'identifiant dans la colonne target_column
    - Déplacement de la nouvelle au début du dataframe

Exemple : create_column_id(df, prefix= 'degradations', origin_column = 'measure_day', target_column = 'id')
'''


def create_column_id(df, prefix, origin_column, target_column): 
    print()
    print(f"\n💾 Traitement du dataframe en cours")
    print(df.head(5))
    # Conversion de la colonne en format datetime
    df[origin_column] = pd.to_datetime(df[origin_column], errors='coerce')
    print(f"\n✅ Conversion de la colonne en format Date/Time", end='')

    # Extraction de la date au format YYYY-MM-DD et concaténation avec le préfixe
    df[target_column] = prefix + '_' + df[origin_column].dt.strftime('%Y-%m-%d')
    print(f"\n✅ Concaténation de la date et du préfixe", end='')

    # Déplacement de target_column en première position
    target_column_series = df.pop(target_column)
    df.insert(0, target_column, target_column_series)
    print(f"\n✅ Réorganisaton des colonnes")

    print(f"\n💾 Traitement du dataframe terminé")
    print(df.head(5))

    return df

'''
Explications :
La fonction split_column_json prend en entrée :
    - df : Nom du dataframe utilisé
    - column_name : Nom de la colonne à diviser

Etapes du script :
    - Vérification de l'existance de la colonne
    - Division de la colonne "sensor_data" en colonnes distinctes avec pd.json_normalize().

Exemple : split_column_json(df, column_name = 'sensor_data')
'''
def split_column_json(df, column_name):
    warnings.simplefilter('ignore')
    
    # Vérification de l'existance de la colonne
    if column_name not in df.columns:
        print(f"❌ La colonne '{column_name}' n'existe pas.")
        return df
    print(f"\n💾 Traitement du dataframe en cours")
    print(df.head(5))

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
            print(f"❌ Erreur lors de l'extraction des données : {e}")
            # Autant de none que de colonnes
            return None, None, None

    print(f"\n✅ Traitement des colonnes de températures, pressions et vibrations", end='')

    # Appliquer la fonction extract_sensor_data à la colonne sensor_data
    df[[column_name]] = df[[column_name]].applymap(extract_sensor_data)

    # Division de sensor_data en colonnes distinctes
    df[['temp_C', 'pressure_hPa', 'vibrations_ms2']] = pd.DataFrame(df[column_name].tolist(), index=df.index)
    print(f"\n✅ Division de sensor_data en colonnes distinctes", end='')

    # Suppression de la colonne
    print(f"\n✅ Suppression de la colonne sensor_data")
    df = df.drop([column_name], axis=1)

    print(f"\n💾 Traitement du dataframe terminé")
    print(df.head(5))

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
    print(f"\n💾 Traitement du dataframe en cours")
    print(df.head(5))

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
    print(f"\n✅ Réorganisaton des colonnes")

    print(f"\n💾 Traitement du dataframe terminé")
    print(df.head(5))

    return df

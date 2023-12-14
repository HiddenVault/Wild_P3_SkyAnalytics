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
    print(f"💾 Traitement du dataframe en cours")
    print(df.head(5))
    # Conversion de la colonne en format datetime
    df[origin_column] = pd.to_datetime(df[origin_column], errors='coerce')

    # Extraction de la datee au format YYYY-MM-DD et concaténation avec le préfixe
    df[target_column] = prefix + '_' + df[origin_column].dt.strftime('%Y-%m-%d')

    # Déplacement de target_column en première position
    target_column_series = df.pop(target_column)
    df.insert(0, target_column, target_column_series)

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
    print(f"💾 Traitement du dataframe en cours")
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

    # Appliquer la fonction extract_sensor_data à la colonne sensor_data
    df[[column_name]] = df[[column_name]].applymap(extract_sensor_data)

    # Division de sensor_data en colonnes distinctes
    df[['temp_C', 'pressure_hPa', 'vibrations_ms2']] = pd.DataFrame(df[column_name].tolist(), index=df.index)

    # Suppression de la colonne
    print(f"\nℹ️ Suppression de la colonne sensor_data")
    df = df.drop([column_name], axis=1)

    print(f"\n💾 Traitement du dataframe terminé")
    print(df.head(5))

    return df
















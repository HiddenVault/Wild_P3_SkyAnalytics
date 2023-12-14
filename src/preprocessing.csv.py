
import modulePreprocessing

# Téléchargement des fichiers logs_vols_AAAA-MM-JJ.csv
modulePreprocessing.file_download(start_date = '2023-11-15', path = 'https://sc-e.fr/docs/', file_name = 'logs_vols_AAAA-MM-JJ.csv', storage_folder = 'incoming')

# Téléchargement des fichiers degradations_AAAA-MM-JJ.csv
modulePreprocessing.file_download(start_date = '2023-11-15', path = 'https://sc-e.fr/docs/', file_name = 'degradations_AAAA-MM-JJ.csv', storage_folder = 'incoming')

# Concatenation des fichiers logs_vols_AAAA-MM-JJ.csv
modulePreprocessing.concatenate_csv(start_date = '2023-11-15', storage_folder = 'incoming', file_name = 'logs_vols_AAAA-MM-JJ.csv', outgoing_folder = 'preprocessing', outgoing_file = 'logs_vols')

# Concatenation des fichiers degradations_AAAA-MM-JJ.csv
modulePreprocessing.concatenate_csv(start_date = '2023-11-15', storage_folder = 'incoming', file_name = 'degradations_AAAA-MM-JJ.csv', outgoing_folder = 'preprocessing', outgoing_file = 'degradations')


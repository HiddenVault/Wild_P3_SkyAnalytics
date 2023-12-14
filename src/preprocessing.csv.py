import modulePreprocessing

# Téléchargement des fichiers logs_vols_AAAA-MM-JJ.csv
modulePreprocessing.file_download(start_date = '2023-11-01', path = 'https://sc-e.fr/docs/', file_name = 'logs_vols_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')

# Téléchargement des fichiers degradations_AAAA-MM-JJ.csv
modulePreprocessing.file_download(start_date = '2023-11-01', path = 'https://sc-e.fr/docs/', file_name = 'degradations_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')

# Concatenation des fichiers logs_vols_AAAA-MM-JJ.csv
modulePreprocessing.concatenate_csv(start_date = '2023-11-01', storage_folder = 'data/incoming', file_name = 'logs_vols_AAAA-MM-JJ.csv', outgoing_folder = 'data/preprocessing', outgoing_file = 'logs_vols')

# Concatenation des fichiers degradations_AAAA-MM-JJ.csv
modulePreprocessing.concatenate_csv(start_date = '2023-11-01', storage_folder = 'data/incoming', file_name = 'degradations_AAAA-MM-JJ.csv', outgoing_folder = 'data/preprocessing', outgoing_file = 'degradations')



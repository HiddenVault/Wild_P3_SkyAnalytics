import modulePreprocessing

# Téléchargement du fichier composants.csv
#modulePreprocessing.file_download(path = 'https://sc-e.fr/docs/', file_name = 'composants.csv', storage_folder = 'data/incoming')

# Téléchargement du fichier aeronefs.csv
#modulePreprocessing.file_download(path = 'https://sc-e.fr/docs/', file_name = 'aeronefs.csv', storage_folder = 'data/incoming')

# Téléchargement des fichiers logs_vols_AAAA-MM-JJ.csv
# modulePreprocessing.file_download_batch(start_date = '2023-11-01', path = 'https://sc-e.fr/docs/', file_name = 'logs_vols_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
#modulePreprocessing.file_download_batch(start_date = '2023-11-01', path = 'http://hiddenvault.fr/P3_SkyAnalytics/docs/', file_name = 'logs_vols_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')

# Téléchargement des fichiers degradations_AAAA-MM-JJ.csv
#modulePreprocessing.file_download_batch(start_date = '2023-11-01', path = 'https://sc-e.fr/docs/', file_name = 'degradations_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
#modulePreprocessing.file_download_batch(start_date = '2023-11-01', path = 'http://hiddenvault.fr/P3_SkyAnalytics/docs/', file_name = 'degradations_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')

# Téléchargement des fichiers aeronefs_AAAA-MM-JJ.csv
#modulePreprocessing.file_download_batch(start_date = '2023-11-01', path = 'https://sc-e.fr/docs/', file_name = 'degradations_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
#modulePreprocessing.file_download_batch(start_date = '2023-11-01', path = 'http://hiddenvault.fr/P3_SkyAnalytics/docs/', file_name = 'aeronefs_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')

# Téléchargement des fichiers composants_AAAA-MM-JJ.csv
#modulePreprocessing.file_download_batch(start_date = '2023-11-01', path = 'https://sc-e.fr/docs/', file_name = 'composants_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')
modulePreprocessing.file_download_batch(start_date = '2023-11-01', path = 'http://hiddenvault.fr/P3_SkyAnalytics/docs/', file_name = 'composants_AAAA-MM-JJ.csv', storage_folder = 'data/incoming')

# Concaténation des fichiers logs_vols_AAAA-MM-JJ.csv
#modulePreprocessing.concatenate_csv(start_date = '2023-11-01', storage_folder = 'data/incoming', file_name = 'logs_vols_AAAA-MM-JJ.csv', outgoing_folder = 'data/preprocessing', outgoing_file = 'logs_vols')
#modulePreprocessing.concatenate_csv(start_date = '2023-11-01', storage_folder = 'http://hiddenvault.fr/P3_SkyAnalytics/docs/', file_name = 'logs_vols_AAAA-MM-JJ.csv', outgoing_folder = 'data/preprocessing', outgoing_file = 'logs_vols')

# Concaténation des fichiers degradations_AAAA-MM-JJ.csv
#modulePreprocessing.concatenate_csv(start_date = '2023-11-01', storage_folder = 'http://hiddenvault.fr/P3_SkyAnalytics/docs/', file_name = 'degradations_AAAA-MM-JJ.csv', outgoing_folder = 'data/preprocessing', outgoing_file = 'degradations')
#modulePreprocessing.concatenate_csv(start_date = '2023-11-01', storage_folder = 'http://hiddenvault.fr/P3_SkyAnalytics/docs/', file_name = 'degradations_AAAA-MM-JJ.csv', outgoing_folder = 'data/preprocessing', outgoing_file = 'degradations')





import modulePreprocessing

# Téléchargement des fichiers logs_vols_AAAA-MM-JJ.csv
modulePreprocessing.file_download(start_date = '2023-10-29', path = 'https://sc-e.fr/docs/', file_name = 'logs_vols_AAAA-MM-JJ.csv', storage_folder = 'incoming')

# Télécharement des fichiers 
modulePreprocessing.file_download(start_date = '2023-10-29', path = 'https://sc-e.fr/docs/', file_name = 'degradations_AAAA-MM-JJ.csv', storage_folder = 'incoming')


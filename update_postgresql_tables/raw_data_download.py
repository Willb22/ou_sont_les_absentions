import requests
import csv
import gzip
from datetime import datetime
import os
import sys

def allow_imports():
    current_directory = os.path.dirname(__file__)
    parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
    if parent_directory not in sys.path:
        sys.path.append(parent_directory)
allow_imports()
from db_connections import Connectdb, log_memory_after, database_name, query_aws_table
from config import configurations, logging, now

current_directory = os.path.dirname(__file__)
project_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

#url = 'https://public.opendatasoft.com/explore/dataset/election-presidentielle-2017-resultats-par-bureaux-de-vote-tour-1/export/'
#url_csv_file = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/election-presidentielle-2017-resultats-par-bureaux-de-vote-tour-1/exports/csv?lang=fr&amp;timezone=Europe%2FBerlin&amp;use_labels=true&amp;delimiter=%3B"
#url_opendatasoft_2017 = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/election-presidentielle-2017-resultats-par-bureaux-de-vote-tour-1/exports/csv"
url_opendatasoft_2017 = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/election-presidentielle-2017-resultats-par-bureaux-de-vote-tour-1/exports/csv?lang=fr&use_labels=true&delimiter=%3B"

url_opendatasoft_2022 = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/elections-france-presidentielles-2022-1er-tour-par-bureau-de-vote/exports/csv?lang=fr&timezone=Europe%2FParis&use_labels=true&delimiter=%3B"
url_geo_coords = "http://data.cquest.org/elections/bureaux_de_vote/geo_bureaux_de_vote.csv.gz" # returns gzip

url_datagouv_france2017 = "https://static.data.gouv.fr/resources/election-presidentielle-des-23-avril-et-7-mai-2017-resultats-definitifs-du-1er-tour-par-bureaux-de-vote/20170427-100955/PR17_BVot_T1_FE.txt"
url_datagouv_france2022 = "https://static.data.gouv.fr/resources/election-presidentielle-des-10-et-24-avril-2022-resultats-definitifs-du-1er-tour/20220414-152542/resultats-par-niveau-burvot-t1-france-entiere.txt"


current_directory = os.path.dirname(__file__)
project_directory = os.path.abspath(os.path.join(current_directory, os.pardir))

path_geo_coords = f'{project_directory}/raw/geo_bureaux_de_vote.csv'

path_datagouv_france2017 = f'{project_directory}/raw/france_2017/PR17_BVot_T1_FE.txt'
path_opendatasoft_france2017 = f'{project_directory}/raw/france_2017/election-presidentielle-2017-resultats-par-bureaux-de-vote-tour-1.csv'

path_datagouv_france2022 = f'{project_directory}/raw/france_2022/resultats-par-niveau-burvot-t1-france-entiere.txt'
path_opendatasoft_france2022 = f'{project_directory}/raw/france_2022/elections-france-presidentielles-2022-1er-tour-par-bureau-de-vote.csv'


def download_csv_file(url_csv_file, destination_filename, compressed_content=False):
    if compressed_content:
        response = requests.get(url_csv_file)
        if response.status_code == 200:
            logging.info(log_memory_after(f'get response for FILE {destination_filename}'))
            path_gzip = f'geo_bureaux_de_vote_{now}.csv.gz'
            with open(path_gzip, "wb") as f:
                f.write(response.content)
                logging.info(log_memory_after(f'AFTER write csv.gz for FILE {destination_filename}'))
            with gzip.open(path_gzip, 'rt', newline='', encoding='utf_8') as csv_file:
                csv_data = csv_file.read()
                logging.info(log_memory_after(f'AFTER open csv.gz for FILE {destination_filename}'))
                #logging.info(f'type for var csv_data is {type(csv_data)}')

            with open(destination_filename, 'wt') as out_file:
                out_file.write(csv_data)
                logging.info(log_memory_after(f'AFTER write final csv for FILE {destination_filename}'))
    else:
        session = requests.Session()
        r = session.get(url_csv_file, stream=True)
        r.raise_for_status()
        with open(destination_filename, 'wt') as out_file:
            for chunk in r.iter_content(chunk_size=10240000, decode_unicode=True): # chunk_size is a number of bytes
                out_file.write(str(chunk))
                #logging.info(log_memory_after(f'increment inside chunk loop for FILE {destination_filename}'))

            logging.info(log_memory_after(f'AFTER write final csv for FILE {destination_filename}'))
    print(f"CSV file successfully downloaded and saved as {destination_filename}")

# download_csv_file(url_geo_coords, f'{current_directory}/geo_coords_{now}.csv', compressed_content=True)
# download_csv_file(url_datagouv_france2017, f'{current_directory}/datagouv_2017_{now}.csv')
# download_csv_file(url_datagouv_france2022, f'{current_directory}/datagouv_2022_{now}.csv')
# download_csv_file(url_opendatasoft_2017, f'{current_directory}/opendatasoft_2017_{now}.csv')
# download_csv_file(url_opendatasoft_2022, f'{current_directory}/opendatasoft_2022_{now}.csv')



download_csv_file(url_geo_coords, path_geo_coords, compressed_content=True)
#download_csv_file(url_datagouv_france2017, path_datagouv_france2017)
#download_csv_file(url_datagouv_france2022, path_datagouv_france2022)
#download_csv_file(url_opendatasoft_2017, path_opendatasoft_france2017)
#download_csv_file(url_opendatasoft_2022, path_opendatasoft_france2022)
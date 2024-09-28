import requests
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

url_geo_coords = configurations['raw_data_sources']['url_geo_coords']
url_opendatasoft_2017 = configurations['raw_data_sources']['france2017']['url_opendatasoft_2017']
url_opendatasoft_2022 = configurations['raw_data_sources']['france2022']['url_opendatasoft_2022']
url_datagouv_france2017 = configurations['raw_data_sources']['france2017']['url_datagouv_france2017']
url_datagouv_france2022 = configurations['raw_data_sources']['france2022']['url_datagouv_france2022']

path_geo_coords = f"{project_directory}{configurations['raw_data_sources']['path_geo_coords']}"
path_datagouv_france2017 = f"{project_directory}{configurations['raw_data_sources']['france2017']['path_datagouv_france2017']}"
path_opendatasoft_france2017 = f"{project_directory}{configurations['raw_data_sources']['france2017']['path_opendatasoft_france2017']}"
path_datagouv_france2022 = f"{project_directory}{configurations['raw_data_sources']['france2022']['path_datagouv_france2022']}"
path_opendatasoft_france2022 = f"{project_directory}{configurations['raw_data_sources']['france2022']['path_opendatasoft_france2022']}"

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

download_csv_file(url_geo_coords, path_geo_coords, compressed_content=True)
download_csv_file(url_datagouv_france2017, path_datagouv_france2017)
download_csv_file(url_datagouv_france2022, path_datagouv_france2022)
download_csv_file(url_opendatasoft_2017, path_opendatasoft_france2017)
download_csv_file(url_opendatasoft_2022, path_opendatasoft_france2022)
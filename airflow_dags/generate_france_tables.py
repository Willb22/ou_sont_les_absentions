import pandas as pd
import numpy as nd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.models.baseoperator import chain
import dask.dataframe as dd
from sqlalchemy import MetaData, String, Integer, Float
import os, sys
from datetime import datetime


def allow_imports():
    current_directory = os.path.dirname(__file__)
    parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
    if parent_directory not in sys.path:
        sys.path.append(parent_directory)
allow_imports()
from db_connections import Connectdb, log_memory_after, database_name, table_connection
from config import configurations, logging
from process_data.feed_data_to_postgresql import Table_inserts, Process_france2017, Process_france2022
from process_data.raw_data_download import download_csv_file,download_geo_coord
current_directory = os.path.dirname(__file__)
project_directory = os.path.abspath(os.path.join(current_directory, os.pardir))

path_geo_coords = f"{project_directory}{configurations['raw_data_sources']['path_geo_coords']}"
path_datagouv_france2017 = f"{project_directory}{configurations['raw_data_sources']['france2017']['path_datagouv_france2017']}"
path_opendatasoft_france2017 = f"{project_directory}{configurations['raw_data_sources']['france2017']['path_opendatasoft_france2017']}"
path_datagouv_france2022 = f"{project_directory}{configurations['raw_data_sources']['france2022']['path_datagouv_france2022']}"
path_opendatasoft_france2022 = f"{project_directory}{configurations['raw_data_sources']['france2022']['path_opendatasoft_france2022']}"

dask_read_block_size = configurations['ram_memory_settings']['dask_read_block_size']



with DAG(dag_id="ousontlesabstentions_france", start_date=datetime(2025, 3, 19), schedule="0 0 * * *") as dag:

    # Tasks are represented as operators
    #hello = BashOperator(task_id="hello", bash_command="echo hello")
    @task(task_id="task_download_geo_coord")
    def download_geo_coord():
        download_csv_file(url_geo_coords, path_geo_coords, compressed_content=True)

    @task(task_id="task_download_datagouv_france2017")
    def download_datagouv_france2017():
        download_csv_file(url_datagouv_france2017, path_datagouv_france2017)

    @task(task_id="task_download_datagouv_france2022")
    def download_datagouv_france2022():
        download_csv_file(url_datagouv_france2022, path_datagouv_france2022)

    @task(task_id="task_download_opendatasoft_france2017")
    def download_opendatasoft_france2017():
        download_csv_file(url_opendatasoft_france2017, path_opendatasoft_france2017)

    @task(task_id="task_download_opendatasoft_france2022")
    def download_opendatasoft_france2022():
        download_csv_file(url_opendatasoft_france2022, path_opendatasoft_france2022)

    @task(task_id="task_france_2017_update_table")
    def insert_france2017():
        process_france2017 = Process_france2017(path_opendatasoft_france2017)
        df_france2017 = process_france2017.dask_dataframe()
        process_france2017.insert_to_db(df_france2017)

    @task(task_id="task_france_2022_update_table")
    def insert_france2022():
        process_france2022 = Process_france2022(path_opendatasoft_france2022)
        df_france2022 = process_france2022.dask_dataframe()
        process_france2022.insert_to_db(df_france2022)

    geo_coord = download_geo_coord()
    chain([download_datagouv_france2017(), download_opendatasoft_france2017(), geo_coord] ,insert_france2017())
    chain([download_datagouv_france2022(), download_opendatasoft_france2022(), geo_coord] ,insert_france2022())


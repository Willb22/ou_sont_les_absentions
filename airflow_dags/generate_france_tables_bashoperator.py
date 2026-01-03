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


# def allow_imports():
#     current_directory = os.path.dirname(__file__)
#     parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
#     if parent_directory not in sys.path:
#         sys.path.append(parent_directory)
# allow_imports()
#from db_connections import Connectdb, log_memory_after, database_name, table_connection
# from config import configurations, logging
# from process_data.feed_data_to_postgresql import Table_inserts, Process_france2017, Process_france2022
# from process_data.raw_data_download import download_csv_file,download_geo_coord
# current_directory = os.path.dirname(__file__)
# project_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
#
# path_geo_coords = f"{project_directory}{configurations['raw_data_sources']['path_geo_coords']}"
# path_datagouv_france2017 = f"{project_directory}{configurations['raw_data_sources']['france2017']['path_datagouv_france2017']}"
# path_opendatasoft_france2017 = f"{project_directory}{configurations['raw_data_sources']['france2017']['path_opendatasoft_france2017']}"
# path_datagouv_france2022 = f"{project_directory}{configurations['raw_data_sources']['france2022']['path_datagouv_france2022']}"
# path_opendatasoft_france2022 = f"{project_directory}{configurations['raw_data_sources']['france2022']['path_opendatasoft_france2022']}"
#
# dask_read_block_size = configurations['ram_memory_settings']['dask_read_block_size']



with DAG(dag_id="ousontlesabstentions_france_bash", start_date=datetime(2025, 3, 19), schedule="0 0 * * *") as dag:

    # Tasks are represented as operators
    #hello = BashOperator(task_id="hello", bash_command="echo hello")

    op_download_geo_coord = BashOperator(
        task_id="task_download_geo_coord",
        bash_command="docker compose run -d datafeed --stage download_geo_coord",
    )

    op_download_datagouv_france2017 = BashOperator(
        task_id="task_download_datagouv_france2017",
        bash_command="docker compose run -d datafeed --stage download_datagouv_france2017",
    )

    op_download_datagouv_france2022 = BashOperator(
        task_id="task_download_datagouv_france2022",
        bash_command="docker compose run -d datafeed --stage download_datagouv_france2022",
    )

    op_download_opendatasoft_france2017 = BashOperator(
        task_id="task_download_opendatasoft_france2017",
        bash_command="docker compose run -d datafeed --stage download_opendatasoft_france2017",
    )

    op_download_opendatasoft_france2022 = BashOperator(
        task_id="task_download_opendatasoft_france2022",
        bash_command="docker compose run -d datafeed --stage download_opendatasoft_france2022",
    )

    op_insert_france2017 = BashOperator(
        task_id="task_insert_france2017",
        bash_command="docker compose run -d datafeed --stage france2017",
    )

    op_insert_france2022 = BashOperator(
        task_id="task_insert_france2022",
        bash_command="docker compose run -d datafeed --stage france2022",
    )

    chain([op_download_datagouv_france2017, op_download_opendatasoft_france2017, op_download_geo_coord] ,op_insert_france2017)
    chain([op_download_datagouv_france2022, op_download_opendatasoft_france2022, op_download_geo_coord] ,op_insert_france2022)


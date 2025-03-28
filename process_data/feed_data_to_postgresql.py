import pandas as pd
import numpy as nd
import dask.dataframe as dd
from sqlalchemy import MetaData, Table, Column, String, Integer, Float
from sqlalchemy.orm import sessionmaker
import os, sys

def allow_imports():
    current_directory = os.path.dirname(__file__)
    parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
    if parent_directory not in sys.path:
        sys.path.append(parent_directory)
allow_imports()
from db_connections import Connectdb, log_memory_after, database_name, query_aws_table
from config import configurations, logging

current_directory = os.path.dirname(__file__)
project_directory = os.path.abspath(os.path.join(current_directory, os.pardir))

path_geo_coords = f"{project_directory}{configurations['raw_data_sources']['path_geo_coords']}"
path_datagouv_france2017 = f"{project_directory}{configurations['raw_data_sources']['france2017']['path_datagouv_france2017']}"
path_opendatasoft_france2017 = f"{project_directory}{configurations['raw_data_sources']['france2017']['path_opendatasoft_france2017']}"
path_datagouv_france2022 = f"{project_directory}{configurations['raw_data_sources']['france2022']['path_datagouv_france2022']}"
path_opendatasoft_france2022 = f"{project_directory}{configurations['raw_data_sources']['france2022']['path_opendatasoft_france2022']}"

dask_read_block_size = configurations['ram_memory_settings']['dask_read_block_size']

class Table_inserts(Connectdb):
    def __init__(self):
        super().__init__(database_name=database_name, query_aws_table=query_aws_table)
        self.table_name = ''
        self.opendatasoft_cols_to_read = list()
        self.opendatasoft_col_types = dict()
        self.path_opendatasoft = ''
        self.dask_read_block_size = None
        self.path_geo_coords = path_geo_coords


    def create_denomination_complete(self, df):
        df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ')'
        # cols = ['Libellé du département', 'Code du département']
        # df['dénomination complète'] = df[cols].apply(lambda row : '{} ({})'.format(*cols), axis=1)
        return df

    def default_read(self, path_read, path_write):
        if hasattr(self, 'path_opendatasoft'):
            if path_read == self.path_opendatasoft:
                df = pd.read_csv(path_read, sep=';', lineterminator='\r', low_memory=False)
            else:
                df = pd.read_csv(path_read)
        df = df.dropna()
        df.to_csv(path_write)

    def paris_geo_coords(self):
        geo = pd.read_csv(self.path_geo_coords)
        postal = geo['code_postal'].fillna('00')
        is_paris = postal.apply(lambda x: True if x.startswith('75') else False)
        geo_paris = geo[is_paris]
        geo_paris['col_merge'] = geo_paris['circonscription_code'] + '_' + geo_paris['code_postal'].apply(
            lambda x: str(x)[-2:]) + geo_paris['code'].apply(lambda x: str(x)[-2:])
        return geo_paris


    def process_raw_opendatasoft(self):
        header_chunk = pd.read_csv(self.path_opendatasoft, index_col=False, nrows=0, sep=';').columns
        all_csv_cols = header_chunk.tolist()
        print(f'header chunk read from {self.path_opendatasoft} are {header_chunk}')
        col_indices_to_read = [all_csv_cols.index(col) for col in self.opendatasoft_cols_to_read]

        df = dd.read_csv(self.path_opendatasoft, sep=';', lineterminator='\r', usecols=col_indices_to_read,
                         blocksize=self.dask_read_block_size,
                         dtype=self.opendatasoft_col_types)
        df = df.dropna()
        df = df[self.opendatasoft_cols_to_read]
        nested_coordinates_label = self.opendatasoft_cols_to_read[0]
        df['latitude'] = df[nested_coordinates_label].apply(lambda x: float(x.split(',')[0]) if type(x) is str else x,
                                                 meta=df[nested_coordinates_label])
        df['longitude'] = df[nested_coordinates_label].apply(lambda x: float(x.split(',')[1]) if type(x) is str else x,
                                                  meta=df[nested_coordinates_label])
        df = df.drop(nested_coordinates_label, axis=1)
        df['Code du département'] = df['Code du département'].apply(lambda x: str(x)[1:], meta=df[
            'Code du département'])  # truncate unwanted '\n'
        df = self.ammend_pourcentage_abs_col(df)

        return df

    def ammend_pourcentage_abs_col(self, df):
        df['% Abs/Ins'] = df['% Abs/Ins'].apply(lambda x: x.replace(',', '.') if type(x) == str else x,
                                                meta=df['% Abs/Ins'])
        df = df.rename(columns={'% Abs/Ins': 'Pourcentage_Abstentions'})
        return df

    def insert_to_db(self, df):
        conn, cursor = self.connect_driver()
        dbExists = self.check_database_exists(conn, cursor)
        if dbExists is False:
            self.create_db(cursor)
        conn_orm, db, uri = self.connect_orm()
        logging.info(log_memory_after('retrieve dataframe before SQL'))
        metadata_obj = MetaData()
        metadata_obj.create_all(db)
        logging.info(log_memory_after('SQL ORM configs '))
        col_types = dict()
        col_types['Code du département'] = String
        col_types['Libellé du département'] = String
        col_types['Libellé de la commune'] = String
        col_types['Inscrits'] = Integer
        col_types['Abstentions'] = Integer
        col_types['Pourcentage_Abstentions'] = Float
        col_types['latitude'] = Float
        col_types['longitude'] = Float
        col_types['dénomination complète'] = String
        col_types['Adresse complète'] = String
        for i in range(df.npartitions):
            partition = df.get_partition(i)
            if i == 0:
                partition.to_sql(self.table_name, uri=uri, if_exists='replace', index=False,
                                 dtype=col_types)
            if i > 0:
                partition.to_sql(self.table_name, uri=uri, if_exists='append', index=False,
                                 dtype=col_types)
        logging.info(log_memory_after(f'sql insertion {self.table_name}'))

class Process_france2017(Table_inserts):
    def __init__(self, path_opendatasoft):
        super().__init__()
        self.path_datagouv_france2017 = path_datagouv_france2017
        self.path_opendatasoft = path_opendatasoft
        self.dask_read_block_size = dask_read_block_size
        self.table_name = 'france_pres_2017'
        self.opendatasoft_col_types = {'Coordonnées': 'object', 'Code du département': 'object', 'Département': 'object',
                      'Libellé de la commune': 'object', 'Abstentions': 'object', 'Inscrits': 'object',
                      '% Abs/Ins': 'object',
                      'lib_du_b_vote': 'object', 'Code Postal': 'float64'} #, # code postal as float to avoid ValueError
        self.opendatasoft_cols_to_read = ['Coordonnées', 'Code du département', 'Département',
                        'Commune', 'Inscrits', 'Abstentions', '% Abs/Ins', 'Adresse', 'Code Postal']

    def paris_datagouv(self):
        dict_dtype = {'Code du département':'object', 'Code du b.vote':'object', 'Code de la circonscription':'object'}
        df_datagouv_france2017 = pd.read_csv(self.path_datagouv_france2017, sep=';', decimal=',', dtype=dict_dtype, index_col=False)
        logging.info(log_memory_after('read datagouv csv 2017'))
        df_paris = df_datagouv_france2017[df_datagouv_france2017['Libellé du département'] == 'Paris']
        df_paris['Code du b.vote'] = df_paris['Code du b.vote'].apply(lambda x: str(x))
        df_paris['Code du département'] = df_paris['Code du département'].apply(lambda x: str(x))
        df_paris['col_merge'] = df_paris['Code du département'].apply(str) + '-' + df_paris[
            'Code de la circonscription'].apply(str).apply(lambda x: '0' + x if len(x) < 2 else x) + '_' + df_paris['Code du b.vote'].apply(str).apply(
                                    lambda x: '0' + x if len(x) < 4 else x)
        return df_paris

    def join_for_paris(self):
        df_paris = self.paris_datagouv()
        geo_paris = self.paris_geo_coords()
        df_merged = pd.merge(geo_paris, df_paris, left_on='col_merge', right_on='col_merge')
        return df_merged

    def add_paris(self, df):
        paris_with_coords = self.join_for_paris()
        keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département',
                        'Libellé de la commune', '% Abs/Ins', 'Inscrits', 'Abstentions', 'geo_adresse']
        paris_keep_columns = paris_with_coords[keep_columns]
        renamed_cols = {'geo_adresse': 'Adresse complète'}
        paris_keep_columns.rename(columns=renamed_cols, inplace=True)
        paris_keep_columns['Code du département'] = paris_keep_columns['Code du département'].apply(lambda x: str(x))
        paris_keep_columns = self.create_denomination_complete(paris_keep_columns)
        dask_paris = dd.from_pandas(paris_keep_columns, npartitions=5)
        dask_paris = self.ammend_pourcentage_abs_col(dask_paris)
        df = dd.concat([df, dask_paris])
        return df

    def create_adresse_complete(self, df):
        df['Adresse complète'] = df['Adresse'].map(str) + ' ' + df['Libellé de la commune'].map(str) + ' ' + df[
            'Code Postal'].map(int).map(str)
        df = df.drop(['Adresse', 'Code Postal'], axis=1)
        return df

    def harmonize_opendatasoft_columns(self, df):
        renamed_cols = {'Commune': 'Libellé de la commune', 'Département': 'Libellé du département'}
        df = df.rename(columns=renamed_cols)
        df = self.create_denomination_complete(df)
        return df

    def dask_dataframe(self):
        df = self.process_raw_opendatasoft()
        df = self.harmonize_opendatasoft_columns(df)
        df = self.create_adresse_complete(df)
        df = self.add_paris(df)
        return df



class Process_france2022(Table_inserts):
    def __init__(self, path_opendatasoft):
        super().__init__()
        self.path_datagouv_france2022 = path_datagouv_france2022
        self.path_opendatasoft = path_opendatasoft
        self.dask_read_block_size = dask_read_block_size
        self.table_name = 'france_pres_2022'
        self.opendatasoft_col_types = {'location': 'object', 'Code du département': 'object', 'Libellé du département': 'object',
                      'Libellé de la commune': 'object', 'Abstentions': 'object', 'Inscrits': 'object',
                      '% Abs/Ins': 'object',
                      'lib_du_b_vote': 'object'}
        self.opendatasoft_cols_to_read = ['location', 'Code du département', 'Libellé du département',
                        'Libellé de la commune', 'Inscrits', 'Abstentions', '% Abs/Ins', 'lib_du_b_vote']
    def paris_datagouv(self):
        all_csv_cols = pd.read_csv(self.path_datagouv_france2022, index_col=False, nrows=0, sep=';').columns.tolist() # remove encoding="ISO-8859-1" when file from automated download
        logging.info(f'header chunk read from csv file datagouv 2022 are {all_csv_cols}')
        cols_to_read = ['Code du département', 'Libellé du département',
                        'Libellé de la commune', 'Inscrits', 'Abstentions', '% Abs/Ins']
        col_indices_to_read = [all_csv_cols.index('Code du département'), all_csv_cols.index('Libellé du département'),
                               all_csv_cols.index('Libellé de la commune'),
                               all_csv_cols.index('Code de la circonscription'),
                               all_csv_cols.index('Code du b.vote'), all_csv_cols.index('Inscrits'),
                               all_csv_cols.index('Abstentions'), all_csv_cols.index('% Abs/Ins')]
        df = pd.read_csv(self.path_datagouv_france2022, index_col=False, sep=';', decimal=',' ,usecols=col_indices_to_read) # remove encoding="ISO-8859-1" when file from automated download
        df_paris = df[df['Libellé du département'] == 'Paris']
        df_paris['col_merge'] = df_paris['Code du département'].apply(lambda x : str(x)) + '-' +df_paris['Code de la circonscription'].apply(lambda x: str(x)).apply(lambda x: '0'+x if len(x)< 2 else x) + '_' + df_paris['Code du b.vote'].apply(lambda x: str(x)).apply(lambda x: '0'+x if len(x)< 4 else x)

        return df_paris

    def join_for_paris(self):
        df_paris = self.paris_datagouv()
        geo_paris = self.paris_geo_coords()
        df_merged = pd.merge(geo_paris, df_paris, left_on='col_merge', right_on='col_merge')
        return df_merged

    def add_paris(self, df):
        paris_with_coords = self.join_for_paris()
        keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département',
                        'Libellé de la commune', '% Abs/Ins', 'Inscrits', 'Abstentions', 'geo_adresse']
        paris_keep_columns = paris_with_coords[keep_columns]
        renamed_cols = {'geo_adresse': 'Adresse complète'}
        paris_keep_columns.rename(columns=renamed_cols, inplace=True)
        paris_keep_columns['Code du département'] = paris_keep_columns['Code du département'].apply(lambda x: str(x))
        paris_keep_columns = self.create_denomination_complete(paris_keep_columns)
        dask_paris = dd.from_pandas(paris_keep_columns, npartitions=5)
        dask_paris = self.ammend_pourcentage_abs_col(dask_paris)
        df = dd.concat([df, dask_paris])

        return df

    def create_adresse_complete(self, df):
        df['Adresse complète'] = df['lib_du_b_vote'].map(str) + ' ' + df['Libellé de la commune'].map(str)
        df = df.drop(['lib_du_b_vote'], axis=1)

        return df

    def dask_dataframe(self):
        df = self.process_raw_opendatasoft()
        df = self.create_denomination_complete(df)
        df = self.create_adresse_complete(df)
        df = self.add_paris(df)
        return df


def insert_france2017():
    process_france2017 = Process_france2017(path_opendatasoft_france2017)
    df_france2017 = process_france2017.dask_dataframe()
    process_france2017.insert_to_db(df_france2017)


def insert_france2022():
    process_france2022 = Process_france2022(path_opendatasoft_france2022)
    df_france2022 = process_france2022.dask_dataframe()
    process_france2022.insert_to_db(df_france2022)














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

class Process_france2017(Table_inserts):
    def __init__(self, path_opendatasoft):
        super().__init__()
        self.path_datagouv_france2017 = path_datagouv_france2017
        self.path_geo_coords = path_geo_coords
        self.path_opendatasoft = path_opendatasoft
        self.dask_read_block_size = dask_read_block_size

    def join_for_paris(self):
        dict_dtype = {'Code du département':'object', 'Code du b.vote':'object', 'Code de la circonscription':'object'}
        df_datagouv_france2017 = pd.read_csv(self.path_datagouv_france2017, sep=';', decimal=',', dtype=dict_dtype, index_col=False)
        logging.info(log_memory_after('read datagouv csv 2017'))
        df_geo_coords = pd.read_csv(self.path_geo_coords) #encoding="utf_8"
        logging.info(log_memory_after('read geo_coords csv 2017'))
        df_paris = df_datagouv_france2017[df_datagouv_france2017['Libellé du département'] == 'Paris']
        logging.info(f'columns types for pandas df_paris are {df_paris.dtypes}')
        df_paris['Code du b.vote'] = df_paris['Code du b.vote'].apply(lambda x: str(x))
        df_paris['Code du département'] = df_paris['Code du département'].apply(lambda x: str(x))
        df_paris['col_merge'] = df_paris['Code du département'].apply(str) + '-' + df_paris[
            'Code de la circonscription'].apply(str).apply(lambda x: '0' + x if len(x) < 2 else x) + '_' + df_paris['Code du b.vote'].apply(str).apply(
                                    lambda x: '0' + x if len(x) < 4 else x)
        postal = df_geo_coords['code_postal'].fillna('00')
        is_paris = postal.apply(lambda x: True if x.startswith('75') else False)
        geo_paris = df_geo_coords[is_paris]
        geo_paris['col_merge'] = geo_paris['circonscription_code'] + '_' + geo_paris['code_postal'].apply(
            lambda x: str(x)[-2:]) + geo_paris['code'].apply(lambda x: str(x)[-2:])
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
        df = dd.concat([df, dask_paris])
        return df


    def prepare_data_with_dask(self):
        header_chunk = pd.read_csv(self.path_opendatasoft, index_col=False, nrows=0, sep=';').columns
        all_csv_cols = header_chunk.tolist()
        print(f'header chunk read from csv file opendatasoft 2017 are {header_chunk}')
        cols_to_read = ['Coordonnées', 'Code du département', 'Département',
                        'Commune', 'Inscrits', 'Abstentions', '% Abs/Ins', 'Adresse', 'Code Postal']
        col_indices_to_read = [all_csv_cols.index('Coordonnées'), all_csv_cols.index('Code du département'),
                               all_csv_cols.index('Département'),
                               all_csv_cols.index('Commune'), all_csv_cols.index('Inscrits'),
                               all_csv_cols.index('Abstentions'), all_csv_cols.index('% Abs/Ins'),
                               all_csv_cols.index('Adresse'), all_csv_cols.index('Code Postal')]
        dict_dtype = {'Coordonnées': 'object', 'Code du département': 'object', 'Département': 'object',
                      'Libellé de la commune': 'object', 'Abstentions': 'object', 'Inscrits': 'object',
                      '% Abs/Ins': 'object', 'lib_du_b_vote': 'object', 'Code Postal': 'float64'} #, # code postal as float to avoid ValueError
        df = dd.read_csv(self.path_opendatasoft, sep=';', lineterminator='\r', usecols=col_indices_to_read, blocksize=self.dask_read_block_size,
                         dtype=dict_dtype)
        df = df.dropna()
        df = df[cols_to_read]
        df['latitude'] = df['Coordonnées'].apply(lambda x: float(x.split(',')[0]) if type(x) is str else x,
                                                 meta=df['Coordonnées'])
        df['longitude'] = df['Coordonnées'].apply(lambda x: float(x.split(',')[1]) if type(x) is str else x,
                                                  meta=df['Coordonnées'])
        df = df.drop('Coordonnées', axis=1)
        renamed_cols = {'Commune': 'Libellé de la commune', 'Département': 'Libellé du département'}
        df = df.rename(columns=renamed_cols)
        df['Code du département'] = df['Code du département'].apply(lambda x: str(x)[1:], meta=df[
            'Code du département'])  # truncate unwanted '\n'
        df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ')'
        df['Adresse complète'] = df['Adresse'].map(str) + ' ' + df['Libellé de la commune'].map(str) + ' ' + df[
            'Code Postal'].map(int).map(str)
        df = df.drop(['Adresse', 'Code Postal'], axis=1)
        df = self.add_paris(df)
        df = df.dropna()
        df['% Abs/Ins'] = df['% Abs/Ins'].apply(lambda x: x.replace(',', '.') if type(x) == str else x,
                                                meta=df['% Abs/Ins'])
        df = df.rename(columns={'% Abs/Ins': 'Pourcentage_Abstentions'})

        return df


class Process_france2022(Table_inserts):
    def __init__(self, path_opendatasoft):
        super().__init__()
        self.path_datagouv_france2022 = path_datagouv_france2022
        self.path_geo_coords = path_geo_coords
        self.path_opendatasoft = path_opendatasoft
        self.dask_read_block_size = dask_read_block_size

    def join_for_paris(self):
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
        geo = pd.read_csv(self.path_geo_coords, encoding='utf_8')
        postal = geo['code_postal'].fillna('00')
        is_paris = postal.apply(lambda x: True if x.startswith('75') else False)
        geo_paris = geo[is_paris]
        geo_paris['col_merge'] = geo_paris['circonscription_code'] + '_' + geo_paris['code_postal'].apply(
            lambda x: str(x)[-2:]) + geo_paris['code'].apply(lambda x: str(x)[-2:])
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
        df = dd.concat([df, dask_paris])

        return df

    def prepare_data_with_dask(self):
        header_chunk = pd.read_csv(self.path_opendatasoft, index_col=False, nrows=0, sep=';').columns
        all_csv_cols = header_chunk.tolist()
        logging.info(f'header chunk read from csv file opendatasoft 2022 are {header_chunk}')
        cols_to_read = ['location', 'Code du département', 'Libellé du département',
                        'Libellé de la commune', 'Inscrits', 'Abstentions', '% Abs/Ins', 'lib_du_b_vote']

        col_indices_to_read = [all_csv_cols.index('location'), all_csv_cols.index('Code du département'),
                               all_csv_cols.index('Libellé du département'),
                               all_csv_cols.index('Libellé de la commune'), all_csv_cols.index('Inscrits'),
                               all_csv_cols.index('Abstentions'), all_csv_cols.index('% Abs/Ins'),
                               all_csv_cols.index('lib_du_b_vote')]

        dict_dtype = {'location': 'object', 'Code du département': 'object', 'Libellé du département': 'object',
                      'Libellé de la commune': 'object', 'Abstentions': 'object', 'Inscrits': 'object',
                      '% Abs/Ins': 'object',
                      'lib_du_b_vote': 'object'}

        df = dd.read_csv(self.path_opendatasoft, sep=';', lineterminator='\r', usecols=col_indices_to_read, blocksize=self.dask_read_block_size,
                         dtype=dict_dtype)
        logging.info(log_memory_after('dask read opendatasoft csv 2022'))
        df = df.dropna()
        df = df[cols_to_read]
        df['latitude'] = df['location'].apply(lambda x: float(x.split(',')[0]) if type(x) is str else x,
                                              meta=df['location'])
        df['longitude'] = df['location'].apply(lambda x: float(x.split(',')[1]) if type(x) is str else x,
                                               meta=df['location'])
        df = df.drop('location', axis=1)

        df['Code du département'] = df['Code du département'].apply(lambda x: str(x)[1:], meta=df[
            'Code du département'])  # truncate unwanted '\n'
        df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ')'
        df['Adresse complète'] = df['lib_du_b_vote'].map(str) + ' ' + df['Libellé de la commune'].map(str)
        df = df.drop(['lib_du_b_vote'], axis=1)
        logging.info(log_memory_after('opendatasoft 2022 process extra cols'))
        df = self.add_paris(df)
        logging.info(log_memory_after('opendatasoft 2022 merge Paris data'))
        df = df.dropna()
        df['% Abs/Ins'] = df['% Abs/Ins'].apply(lambda x: x.replace(',', '.') if type(x) == str else x,
                                                meta=df['% Abs/Ins'])
        logging.info(log_memory_after('replace commas in abstention col'))
        df = df.rename(columns={'% Abs/Ins': 'Pourcentage_Abstentions'})
        logging.info(log_memory_after('dask process dataframe'))

        return df



def insert_france_2017():
    process_france2017 = Process_france2017(path_opendatasoft_france2017)
    conn, cursor = process_france2017.connect_driver()
    dbExists = process_france2017.check_database_exists(conn, cursor)
    if dbExists is False:
        process_france2017.create_db(cursor)
    conn_orm, db, uri = process_france2017.connect_orm()
    df_2017 = process_france2017.prepare_data_with_dask()
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
    for i in range(df_2017.npartitions):
        partition = df_2017.get_partition(i)
        if i == 0:
            partition.to_sql('france_pres_2017', uri=uri, if_exists='replace', index=False,
                    dtype=col_types)
        if i > 0:
            partition.to_sql('france_pres_2017', uri=uri, if_exists='append', index=False,
                    dtype=col_types)
    logging.info(log_memory_after('sql insertion france 2017'))


def insert_france_2022():
    process_france2022 = Process_france2022(path_opendatasoft_france2022)
    conn, cursor = process_france2022.connect_driver()
    dbExists = process_france2022.check_database_exists(conn, cursor)
    if dbExists is False:
        process_france2022.create_db(cursor)
    conn_orm, db, uri = process_france2022.connect_orm()
    df_2022 = process_france2022.prepare_data_with_dask()
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
    for i in range(df_2022.npartitions):
        partition = df_2022.get_partition(i)
        if i == 0:
            partition.to_sql('france_pres_2022', uri=uri, if_exists='replace', index=False,
                    dtype=col_types)
        if i > 0:
            partition.to_sql('france_pres_2022', uri=uri, if_exists='append', index=False,
                    dtype=col_types)

    logging.info(log_memory_after('sql insertion france 2022'))

if __name__ == '__main__':
    insert_france_2022()

    insert_france_2017()









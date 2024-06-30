import pandas as pd
import numpy as nd
import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float
from sqlalchemy.orm import sessionmaker
import os
import sys
from resource import getrusage, RUSAGE_SELF
from datetime import datetime
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

path_geo_coords = f'{project_directory}/raw/geo_bureaux_de_vote.csv'

path_datagouv_france2017 = f'{project_directory}/raw/france_2017/PR17_BVot_T1_FE.txt'
path_opendatasoft_france2017 = f'{project_directory}/raw/france_2017/election-presidentielle-2017-resultats-par-bureaux-de-vote-tour-1.csv'

path_datagouv_france2022 = f'{project_directory}/raw/france_2022/resultats-par-niveau-burvot-t1-france-entiere.txt'
path_opendatasoft_france2022 = f'{project_directory}/raw/france_2022/elections-france-presidentielles-2022-1er-tour-par-bureau-de-vote.csv'

path_processed_france2017 = f'{project_directory}/processed/csv_files/france_2017/final_france2017.csv'
path_processed_france2022 = f'{project_directory}/processed/csv_files/france_2022/final_france2022.csv'

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
        self.opendatasoft_read_chunks = True
        self.chunksize_read_opendatasoft = 200
        self.pandas_read_low_memory = True

    def join_for_paris(self):
        df_datagouv_france2017 = pd.read_csv(self.path_datagouv_france2017, encoding="ISO-8859-1", sep=';', decimal=',')
        logging.info(log_memory_after('read datagouv csv 2017'))
        df_geo_coords = pd.read_csv(self.path_geo_coords)
        logging.info(log_memory_after('read geo_coords csv 2017'))
        df_paris = df_datagouv_france2017[df_datagouv_france2017['Libellé du département'] == 'Paris']
        df_paris['col_merge'] = df_paris['Code du département'].apply(lambda x: str(x)) + '-' + df_paris[
            'Code de la circonscription'].apply(lambda x: str(x)).apply(lambda x: '0' + x if len(x) < 2 else x) + '_' + \
                                df_paris['Code du b.vote'].apply(lambda x: str(x)).apply(
                                    lambda x: '0' + x if len(x) < 4 else x)
        postal = df_geo_coords['code_postal'].fillna('00')
        is_paris = postal.apply(lambda x: True if x.startswith('75') else False)
        geo_paris = df_geo_coords[is_paris]
        geo_paris['col_merge'] = geo_paris['circonscription_code'] + '_' + geo_paris['code_postal'].apply(
            lambda x: str(x)[-2:]) + geo_paris['code'].apply(lambda x: str(x)[-2:])

        df_merged = pd.merge(geo_paris, df_paris, left_on='col_merge', right_on='col_merge')
        return df_merged

    def add_paris(self, df):
        #paris_with_coords = pd.read_csv(self.path_paris)

        paris_with_coords = self.join_for_paris()
        keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département',
                        'Libellé de la commune', '% Abs/Ins', 'Inscrits', 'Abstentions', 'geo_adresse']
        paris_keep_columns = paris_with_coords[keep_columns]
        renamed_cols = {'geo_adresse': 'Adresse complète'}
        paris_keep_columns.rename(columns=renamed_cols, inplace=True)
        paris_keep_columns['Code du département'] = paris_keep_columns['Code du département'].apply(lambda x: str(x))
        paris_keep_columns = self.create_denomination_complete(paris_keep_columns)
        df = df.append(paris_keep_columns)
        return df

    def ammend_jura_ain(self, df):
        df['code_postal'] = nd.where(
            (df['Libellé du département'] == 'Jura') & (df['Libellé de la commune'] == 'Chancia'),
            '39102', df['code_postal'])
        df['code_postal'] = nd.where(
            (df['Libellé du département'] == 'Jura') & (df['Libellé de la commune'] == 'Lavancia-Epercy'), '39283',
            df['code_postal'])
        return df

    def prepare_df(self, path):
        path_dropped_na = f'{project_directory}/processed/csv_files/france_2017/abstentions_dropped_na.csv'
        self.default_read(path, path_dropped_na)

        cols = ['longitude', 'latitude', 'libelle_du_departement',
         'ville', 'abs_ins', 'inscrits', 'abstentions', 'adresse', 'code_postal']
        dict_dtype = {'Code du département':'int8', 'abstentions':'int16', 'inscrits':'int16',
                      'longitude':'float32', 'latitude':'float32', 'code_postal':'object'}
        df = pd.read_csv(path_dropped_na, usecols=cols, dtype=dict_dtype)

        renamed_cols = {'ville': 'Libellé de la commune', 'abs_ins': '% Abs/Ins', 'abstentions': 'Abstentions',
                        'inscrits': 'Inscrits', 'libelle_du_departement': 'Libellé du département'}

        df.rename(columns=renamed_cols, inplace=True)
        df = self.ammend_jura_ain(df)
        #log_process_memory('ammend_jura_ain')
        df['Code du département'] = df['code_postal'].apply(lambda x: str(x)[:2])
        df = self.create_denomination_complete(df)
        df['Adresse complète'] = df['adresse'].map(str) + ' ' + df['code_postal'].map(str)
        keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département',
                        'dénomination complète',
                        'Libellé de la commune', '% Abs/Ins', 'Inscrits',
                        'Abstentions', 'Adresse complète']
        df = df[keep_columns]
        df_with_paris = self.add_paris(df)
        df_with_paris = df_with_paris.sort_values(by='Code du département')
        df_with_paris['% Abs/Ins'] = df_with_paris['% Abs/Ins'].apply(lambda x: x.replace(',', '.') if type(x) == str else x)
        df_with_paris = df_with_paris.rename(columns={'% Abs/Ins': 'Pourcentage_Abstentions'})
        df_with_paris['Abstentions'] = df_with_paris['Abstentions'].astype(int, )
        df_with_paris['Pourcentage_Abstentions'] = df_with_paris['Pourcentage_Abstentions'].astype(float, )

        return df_with_paris

    def prepare_data_opendatasoft(self):
        header_chunk = pd.read_csv(self.path_opendatasoft, index_col=False, nrows=0, sep=';').columns
        all_csv_cols = header_chunk.tolist()

        logging.info(f'header chunk read from csv file opendatasoft 2017 are {header_chunk}')
        cols_to_read = ['Coordonnées', 'Code du département', 'Département',
         'Commune', 'Inscrits', 'Abstentions', '% Abs/Ins',
         'Adresse', 'Code Postal']
        col_indices_to_read = [all_csv_cols.index('Coordonnées'), all_csv_cols.index('Code du département'), all_csv_cols.index('Département'),
                               all_csv_cols.index('Commune'), all_csv_cols.index('Inscrits'), all_csv_cols.index('Abstentions'), all_csv_cols.index('% Abs/Ins'),
                               all_csv_cols.index('Adresse'), all_csv_cols.index('Code Postal')]
        logging.info(f'column indices for read csv opendatasoft 2017 are {col_indices_to_read}')
        dict_dtype = {'Coordonnées':'object', 'Code du département':'object', 'Département':'object',
                      'Commune':'object', 'Abstentions':'int16', 'Inscrits':'int16', '% Abs/Ins':'float32',
                      'Adresse':'object', 'Code Postal':'object'}

        #header=0, skiprows=[0,],
        logging.info(log_memory_after('BEFORE read csv opendatasoft 2017'))
        if self.opendatasoft_read_chunks:
            df = pd.DataFrame()
            for slice_df in pd.read_csv(self.path_opendatasoft, sep=';', lineterminator='\r',
                                        low_memory=self.pandas_read_low_memory, usecols=col_indices_to_read,
                                        chunksize=self.chunksize_read_opendatasoft):
                df = pd.concat([df, slice_df], ignore_index=True)
        else:
            df = pd.read_csv(self.path_opendatasoft, sep=';', lineterminator='\r',
                             low_memory=self.pandas_read_low_memory, usecols=col_indices_to_read)
        logging.info(log_memory_after('read csv opendatasoft 2017'))
        df = df[cols_to_read]
        df = df.dropna()
        df['latitude'] = df['Coordonnées'].apply(lambda x: float(x.split(',')[0]) if type(x) is str else x)
        df['longitude'] = df['Coordonnées'].apply(lambda x: float(x.split(',')[1]) if type(x) is str else x)
        df = df.drop('Coordonnées', axis=1)
        renamed_cols = {'Commune': 'Libellé de la commune', 'Département': 'Libellé du département'}
        df.rename(columns=renamed_cols, inplace=True)
        # df = self.ammend_jura_ain(df)
        if self.opendatasoft_read_chunks or self.pandas_read_low_memory:
            df['Code du département'] = df['Code du département'].apply(
                lambda x: str(x)[1:] if '\n' in str(x) else str(x)).apply(
                lambda x: ''.join(('0', x) if len(x) < 2 else x))
        else:
            df['Code du département'] = df['Code du département'].apply(lambda x: str(x)[1:])  # truncate unwanted '\n'

        df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ')'
        df['Adresse complète'] = df['Adresse'].map(str) + ' ' + df['Libellé de la commune'].map(str) + ' ' + df[
            'Code Postal'].map(int).map(str)
        df = df.drop(['Adresse', 'Code Postal'], axis=1)
        df_with_paris = self.add_paris(df)

        df_with_paris = df_with_paris.sort_values(by='Code du département')
        df_with_paris['% Abs/Ins'] = df_with_paris['% Abs/Ins'].apply(lambda x: x.replace(',', '.') if type(x) == str else x)
        renamed_cols = {'% Abs/Ins': 'Pourcentage_Abstentions'}
        df.rename(columns=renamed_cols, inplace=True)

        df_with_paris = df_with_paris.rename(columns={'% Abs/Ins': 'Pourcentage_Abstentions'})# inplace=False by default
        df_with_paris['Abstentions'] = df_with_paris['Abstentions'].astype(int, )
        df_with_paris['Pourcentage_Abstentions'] = df_with_paris['Pourcentage_Abstentions'].astype(float, )


        return df_with_paris






class Process_france2022(Table_inserts):
    def __init__(self, path_opendatasoft):
        super().__init__()
        self.path_datagouv_france2022 = path_datagouv_france2022
        self.path_geo_coords = path_geo_coords
        self.path_opendatasoft = path_opendatasoft
        self.opendatasoft_read_chunks = True
        self.chunksize_read_opendatasoft = 200
        self.pandas_read_low_memory = True  # if True creates heterogenous data in Code du département column
    def join_for_paris(self):
        all_csv_cols = pd.read_csv(self.path_datagouv_france2022, index_col=False, nrows=0, sep=';',
                                   encoding="ISO-8859-1").columns.tolist()
        logging.info(f'header chunk read from csv file datagouv 2022 are {all_csv_cols}')
        cols_to_read = ['Code du département', 'Libellé du département',
                        'Libellé de la commune', 'Inscrits', 'Abstentions', '% Abs/Ins']
        col_indices_to_read = [all_csv_cols.index('Code du département'), all_csv_cols.index('Libellé du département'),
                               all_csv_cols.index('Libellé de la commune'),
                               all_csv_cols.index('Code de la circonscription'),
                               all_csv_cols.index('Code du b.vote'), all_csv_cols.index('Inscrits'),
                               all_csv_cols.index('Abstentions'), all_csv_cols.index('% Abs/Ins')]
        df = pd.read_csv(path_datagouv_france2022, index_col=False, encoding="ISO-8859-1", sep=';', decimal=',',
                         usecols=col_indices_to_read)


        df_paris = df[df['Libellé du département'] == 'Paris']
        df_paris['col_merge'] = df_paris['Code du département'].apply(lambda x : str(x)) + '-' +df_paris['Code de la circonscription'].apply(lambda x: str(x)).apply(lambda x: '0'+x if len(x)< 2 else x) + '_' + df_paris['Code du b.vote'].apply(lambda x: str(x)).apply(lambda x: '0'+x if len(x)< 4 else x)

        geo = pd.read_csv(self.path_geo_coords)
        postal = geo['code_postal'].fillna('00')
        is_paris = postal.apply(lambda x: True if x.startswith('75') else False)
        geo_paris = geo[is_paris]
        geo_paris['col_merge'] = geo_paris['circonscription_code'] + '_' + geo_paris['code_postal'].apply(
            lambda x: str(x)[-2:]) + geo_paris['code'].apply(lambda x: str(x)[-2:])
        df_merged = pd.merge(geo_paris, df_paris, left_on='col_merge', right_on='col_merge')

        return df_merged

    def add_paris(self, df):
        #paris_with_coords = pd.read_csv(self.path_paris)

        paris_with_coords = self.join_for_paris()
        keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département',
                        'Libellé de la commune', '% Abs/Ins', 'Inscrits', 'Abstentions', 'geo_adresse']
        paris_keep_columns = paris_with_coords[keep_columns]
        renamed_cols = {'geo_adresse': 'Adresse complète'}
        paris_keep_columns.rename(columns=renamed_cols, inplace=True)
        paris_keep_columns['Code du département'] = paris_keep_columns['Code du département'].apply(lambda x: str(x))
        paris_keep_columns = self.create_denomination_complete(paris_keep_columns)
        df = df.append(paris_keep_columns)
        all_deps = list(df['dénomination complète'].unique())
        logging.info(log_memory_after('AFTER APPEND Paris deno'))
        return df

    def prepare_data_opendatasoft(self):

        # path_dropped_na = f'{project_directory}/processed/csv_files/france_2017/path_opendatasoft_dropped_na.csv'
        # self.default_read(self.path_opendatasoft, path_dropped_na) # tailored read with specified datatypes int float only possible when no NaN

        header_chunk = pd.read_csv(self.path_opendatasoft, index_col=False, nrows=0, sep=';').columns
        all_csv_cols = header_chunk.tolist()
        logging.info(f'header chunk read from csv file opendatasoft 2022 are {header_chunk}')
        cols_to_read = ['location', 'Code du département', 'Libellé du département',
         'Libellé de la commune', 'Inscrits', 'Abstentions', '% Abs/Ins', 'lib_du_b_vote']
        col_indices_to_read = [all_csv_cols.index('location'), all_csv_cols.index('Code du département'), all_csv_cols.index('Libellé du département'),
                               all_csv_cols.index('Libellé de la commune'), all_csv_cols.index('Inscrits'), all_csv_cols.index('Abstentions'), all_csv_cols.index('% Abs/Ins'),
                               all_csv_cols.index('lib_du_b_vote')]
        logging.info(f'column indices for read csv opendatasoft 2022 are {col_indices_to_read}')

        # dict_dtype = {'Coordonnées':'object', 'Code du département':'object', 'Département':'object',
        #               'Commune':'object', 'Abstentions':'int16', 'Inscrits':'int16', '% Abs/Ins':'float32',
        #               'Adresse':'object', 'Code Postal':'object'}
        logging.info(log_memory_after('BEFORE read csv opendatasoft 2022'))
        #header=0, skiprows=[0,],
        if self.opendatasoft_read_chunks or self.pandas_read_low_memory:
            df = pd.DataFrame()
            for slice_df in pd.read_csv(self.path_opendatasoft, sep=';', lineterminator='\r', low_memory=self.pandas_read_low_memory, usecols=col_indices_to_read, chunksize=self.chunksize_read_opendatasoft):# boolean for low_memory can cause mixed types in Code du département and affect user scroll down menu
                df = pd.concat([df, slice_df], ignore_index=True)
        else:
            df = pd.read_csv(self.path_opendatasoft, sep=';', lineterminator='\r', low_memory=self.pandas_read_low_memory, usecols=col_indices_to_read)


        logging.info(log_memory_after('read opendatasoft csv 2022'))
        df = df.dropna()
        df = df[cols_to_read]
        df['latitude'] = df['location'].apply(lambda x: float(x.split(',')[0]) if type(x) is str else x)
        df['longitude'] = df['location'].apply(lambda x: float(x.split(',')[1]) if type(x) is str else x)
        df = df.drop('location', axis=1)
        # df = self.ammend_jura_ain(df)
        unique_dep_code = list(df['Code du département'].unique())
        logging.info(f'BEFORE truncate unwanted symbol code departement list is {unique_dep_code}')
        if self.opendatasoft_read_chunks:
            df['Code du département'] = df['Code du département'].apply(
                lambda x: str(x)[1:] if '\n' in str(x) else str(x)).apply(lambda x: ''.join(('0', x) if len(x) < 2 else x))
        else:
            df['Code du département'] = df['Code du département'].apply(lambda x: str(x)[1:])  # truncate unwanted '\n'
        logging.info(f'AFTER truncate unwanted symbol code departement list is {list(df["Code du département"].unique())}')
        df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ')'
        scroll_down_list = list(df['dénomination complète'].unique())
        logging.info(f'SCROLL DOWN LIST IS {scroll_down_list}')
        df['Adresse complète'] = df['lib_du_b_vote'].map(str) + ' ' + df['Libellé de la commune'].map(str)
        df = df.drop(['lib_du_b_vote'], axis=1)
        df = self.add_paris(df)
        df = df.dropna()
        df = df.sort_values(by='Code du département')
        df['% Abs/Ins'] = df['% Abs/Ins'].apply(lambda x: x.replace(',', '.') if type(x) == str else x)

        #renamed_cols = {'% Abs/Ins': 'Pourcentage_Abstentions'}
        #df.rename(columns=renamed_cols, inplace=True)
        df.rename(columns={'% Abs/Ins': 'Pourcentage_Abstentions'}, inplace=True)# inplace=TRue modifies dataframe as opposed to creating a new one new_df=other_df.rename(inplace=False)

        df['Abstentions'] = df['Abstentions'].astype(int, ) # UNcomment later
        df['Pourcentage_Abstentions'] = df['Pourcentage_Abstentions'].astype(float, )
        scroll_down_list_paris = list(df['dénomination complète'].unique())
        logging.info(f'SCROLL DOWN LIST with paris IS {scroll_down_list_paris}')
        logging.info(f'DATA TYPES are {df.dtypes}')

        return df


def write_final_csv_2017():
    '''
    Low RAM on ec2 instance
    '''
    process_france2017 = Process_france2017(path_opendatasoft_france2017)
    logging.info('LOAD and process opendatasoft source csv data')
    df_2017 = process_france2017.prepare_data_opendatasoft()
    df_2017.to_csv(path_processed_france2017, chunksize=process_france2017.chunksize_read_opendatasoft, index=False)
    logging.info(log_memory_after('Write final france2017 csv'))


def insert_france_2017():
    process_france2017 = Process_france2017(path_opendatasoft_france2017)
    if process_france2017.opendatasoft_read_chunks:
        df_2017 = pd.DataFrame()
        for slice_df in pd.read_csv(path_processed_france2017,
                                    low_memory=process_france2017.pandas_read_low_memory,
                                    chunksize=process_france2017.chunksize_read_opendatasoft):  # boolean for low_memory can cause mixed types in Code du département and affect user scroll down menu
            df_2017 = pd.concat([df_2017, slice_df], ignore_index=True)
    else:
        df_2017 = pd.read_csv(path_processed_france2017, low_memory=process_france2017.pandas_read_low_memory)

    conn, cursor = process_france2017.connect_driver()
    dbExists = process_france2017.check_database_exists(conn, cursor)
    if dbExists is False:
        process_france2017.create_db(cursor)
    conn_orm, db = process_france2017.connect_orm()

    Session = sessionmaker(bind=db)
    session = Session()
    table_name = 'france_pres_2017'

    all_columns = list(df_2017.columns)
    logging.info(f'All columns in dataframe 2017 {all_columns}')
    columns_for_table = list()

    for col in all_columns:
        if col in ['Code du département', 'Libellé du département', 'dénomination complète',
                   'Libellé de la commune',
                   'Adresse complète']:
            columns_for_table.append(Column(col, String, key=col.replace(' ', '_'), ))

        elif col in ['Abstentions', 'Inscrits']:
            columns_for_table.append(Column(col, Integer, key=col.replace(' ', '_'), ))

        else:
            columns_for_table.append(Column(col, Float, key=col.replace(' ', '_'), ))

    metadata_obj = MetaData()
    france_pres_2017 = Table(table_name, metadata_obj, *(column for column in columns_for_table), )
    metadata_obj.create_all(db)

    df_2017.to_sql('france_pres_2017', con=session.get_bind(), if_exists='replace', index=False, chunksize=configurations['table_insert_chunk_size'])
    logging.info(log_memory_after('pandas to sql method france 2017'))


def write_final_csv_2022():
    '''
    Low RAM on ec2 instance
    '''
    process_france2022 = Process_france2022(path_opendatasoft_france2022)
    logging.info('LOAD and process opendatasoft source csv data')
    df_2022 = process_france2022.prepare_data_opendatasoft()
    df_2022.to_csv(path_processed_france2022, chunksize=process_france2022.chunksize_read_opendatasoft, index=False)
    logging.info(log_memory_after('Write final france2022 csv'))


def insert_france_2022():
    process_france2022 = Process_france2022(path_opendatasoft_france2022)
    if process_france2022.opendatasoft_read_chunks:
        df_2022 = pd.DataFrame()
        for slice_df in pd.read_csv(path_processed_france2022,
                                    low_memory=process_france2022.pandas_read_low_memory,
                                    chunksize=process_france2022.chunksize_read_opendatasoft):  # boolean for low_memory can cause mixed types in Code du département and affect user scroll down menu
            df_2022 = pd.concat([df_2022, slice_df], ignore_index=True)
    else:
        df_2022 = pd.read_csv(path_processed_france2017, low_memory=process_france2022.pandas_read_low_memory)


    conn, cursor = process_france2022.connect_driver()
    dbExists = process_france2022.check_database_exists(conn, cursor)
    if dbExists is False:
        process_france2022.create_db(cursor)
    conn_orm, db = process_france2022.connect_orm()

    Session = sessionmaker(bind=db)
    session = Session()
    table_name = 'france_pres_2022'

    all_columns = list(df_2022.columns)
    logging.info(f'All columns in dataframe 2022 {all_columns}')
    columns_for_table = list()

    for col in all_columns:
        if col in ['Code du département', 'Libellé du département', 'dénomination complète',
                   'Libellé de la commune',
                   'Adresse complète']:
            columns_for_table.append(Column(col, String, key=col.replace(' ', '_'), ))

        elif col in ['Abstentions', 'Inscrits']:
            columns_for_table.append(Column(col, Integer, key=col.replace(' ', '_'), ))

        else:
            columns_for_table.append(Column(col, Float, key=col.replace(' ', '_'), ))

    metadata_obj = MetaData()
    france_pres_2022 = Table(table_name, metadata_obj, *(column for column in columns_for_table), )
    metadata_obj.create_all(db)

    df_2022.to_sql('france_pres_2022', con=session.get_bind(), if_exists='replace', index=False, chunksize=configurations['table_insert_chunk_size'])
    logging.info(log_memory_after('pandas to sql method france 2022'))




if __name__ == '__main__':
    write_final_csv_2022()
    insert_france_2022()

    #write_final_csv_2017()
    #insert_france_2017()









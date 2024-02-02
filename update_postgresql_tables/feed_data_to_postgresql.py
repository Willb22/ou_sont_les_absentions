import pandas as pd
import dask.dataframe as dd
import numpy as nd

import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float
from sqlalchemy.orm import sessionmaker

from resource import getrusage, RUSAGE_SELF
from datetime import datetime

now = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
database_name = 'ou_sont_les_abstentions'

project_directory = '/home/will/Documents/geo_project/Elections-pres-france'

path_abstentions_france2017 = f'{project_directory}/processed/csv_files/france_2017/abstentions.csv'
path_paris_france2017 = f'{project_directory}/processed/csv_files/france_2017/geo_paris.csv'
path_abstentions_france2022 = f'{project_directory}/processed/csv_files/france_2022/abstentions.csv'
path_paris_france2022 = f'{project_directory}/processed/csv_files/france_2022/no_data.csv'

def get_credentials():
    """
    Return user, password, host and port for database and table connection

    Parameters
    ---------
    Nothing is passed. user, password, host and port are defined within the function

    Returns
    -------
    string
        user, password, host and port
    """
    user = 'postgres'
    #passw = os.environ.get('PASSPOSTGRES')
    passw = 'mynewpassword'
    host = '127.0.0.1'
    port = '5433'

    return user, passw, host, port

def connect_driver():
    """
    Return psycopg2 connection and cursor objects

    Parameters
    ---------
    None

    Returns
    -------
    connction and cursor class
    """
    user, passw, host, port = get_credentials()

    # establishing the connection
    conn = psycopg2.connect(
        user=user, password=passw, host=host, port=port
    )
    conn.autocommit = True
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    return conn, cursor

def connect_orm():
    """
    Return sqlalchemy Engine instance and connection

    Parameters
    ---------
    None

    Returns
    -------
    sqlalchemy Engine instance and connection
    """
    user, passw, host, port = get_credentials()

    conn_string = f'postgresql://{user}:{passw}@{host}:{port}/{database_name}'
    engine = create_engine(conn_string)
    conn_orm = engine.connect()
    return conn_orm, engine
def log_process_memory(message):
    file = open(f"memory_usage_{now}.txt", "a")
    memory_message = f"Max Memory after {message} (MiB): {int(getrusage(RUSAGE_SELF).ru_maxrss / 1024)} \n"
    file.write(memory_message)
    file.close()

class Process_data:
    def __init__(self, path_abstentions, path_paris):
        self.path_abstentions = path_abstentions
        self.path_paris = path_paris

    def create_denomination_complete(self, df):
        df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ') '
        return df

    def default_read(self, path_read, path_write):

        df = pd.read_csv(path_read)
        df = df.dropna()
        df.to_csv(path_write)
        log_process_memory('default csv read')

    def all_departements(self):
        df = self.prepare_df(self.path_abstentions)
        res = list(df['dénomination complète'].unique())
        return res

class Process_france2017(Process_data):
    def __init__(self, path_abstentions, path_paris):
        super().__init__(path_abstentions, path_paris)

    def add_paris(self, df):
        paris_with_coords = pd.read_csv(self.path_paris)
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
        log_process_memory('tailored csv read')

        renamed_cols = {'ville': 'Libellé de la commune', 'abs_ins': '% Abs/Ins', 'abstentions': 'Abstentions',
                        'inscrits': 'Inscrits', 'libelle_du_departement': 'Libellé du département'}

        df.rename(columns=renamed_cols, inplace=True)
        df = self.ammend_jura_ain(df)
        log_process_memory('ammend_jura_ain')
        df['Code du département'] = df['code_postal'].apply(lambda x: str(x)[:2])
        df = self.create_denomination_complete(df)
        df['Adresse complète'] = df['adresse'].map(str) + ' ' + df['code_postal'].map(str)
        keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département',
                        'dénomination complète',
                        'Libellé de la commune', '% Abs/Ins', 'Inscrits',
                        'Abstentions', 'Adresse complète']
        df = df[keep_columns]
        df_with_paris = self.add_paris(df)
        log_process_memory('add Paris')
        df_with_paris = df_with_paris.sort_values(by='Code du département')
        df_with_paris['% Abs/Ins'] = df_with_paris['% Abs/Ins'].apply(lambda x: x.replace(',', '.') if type(x) == str else x)
        df_with_paris = df_with_paris.rename(columns={'% Abs/Ins': 'Pourcentage_Absentions'})
        df_with_paris['Abstentions'] = df_with_paris['Abstentions'].astype(int, )
        df_with_paris['Pourcentage_Absentions'] = df_with_paris['Pourcentage_Absentions'].astype(float, )

        return df_with_paris

    def create_table_metadata(self):

        conn, cursor = connect_driver()
        conn_orm, db = connect_orm()

        Session = sessionmaker(bind=db)
        session = Session()
        log_process_memory('ORM and driver connection')
        table_name = 'france_pres_2017'

        all_columns = list(df_2017.columns)
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

        return session

if __name__ == '__main__':
    process_france2017 = Process_france2017(path_abstentions_france2017, path_paris_france2017)
    df_2017 = process_france2017.prepare_df(path_abstentions_france2017)

    session = process_france2017.create_table_metadata()
    log_process_memory('metadata creation')

    df_2017.to_sql('france_pres_2017', con=session.get_bind(), if_exists='replace', index=False, chunksize=1000)
    log_process_memory('ALL completed')


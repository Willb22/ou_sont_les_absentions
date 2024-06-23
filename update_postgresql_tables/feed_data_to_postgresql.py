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

#connect_database = Connectdb(database_name=database_name, query_aws_table=False)

#now = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
current_directory = os.path.dirname(__file__)
project_directory = os.path.abspath(os.path.join(current_directory, os.pardir))


path_abstentions_france2017 = f'{project_directory}/processed/csv_files/france_2017/abstentions.csv'
path_paris_france2017 = f'{project_directory}/processed/csv_files/france_2017/geo_paris.csv'
path_opendatasoft_france2017 = f'{project_directory}/raw/election-presidentielle-2017-resultats-par-bureaux-de-vote-tour-1.csv'
path_geo_coords = f'{project_directory}/raw/geo_bureaux_de_vote.csv'
path_datagouv_france2017 = f'{project_directory}/raw/PR17_BVot_T1_FE.txt'

path_abstentions_france2022 = f'{project_directory}/processed/csv_files/france_2022/abstentions.csv'
path_paris_france2022 = f'{project_directory}/processed/csv_files/france_2022/no_data.csv'
path_opendatasoft_france2022 = f'{project_directory}/raw/elections-france-presidentielles-2022-1er-tour-par-bureau-de-vote.csv'


class Table_inserts(Connectdb):
    def __init__(self):
        super().__init__(database_name=database_name, query_aws_table=query_aws_table)
        # self.path_abstentions = path_abstentions
        # self.path_paris = path_paris

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

    def all_departements(self):
        df = self.prepare_df(self.path_abstentions)
        res = list(df['dénomination complète'].unique())
        return res

class Process_france2017(Table_inserts):
    def __init__(self, path_opendatasoft, path_abstentions, path_paris):
        super().__init__()
        self.path_opendatasoft = path_opendatasoft
        self.path_geo_coords = path_geo_coords
        self.path_datagouv_france2017 = path_datagouv_france2017

    def join_for_paris(self):
        df_datagouv_france2017 = pd.read_csv(self.path_datagouv_france2017, encoding = "ISO-8859-1", sep =';', decimal =',')
        df_geo_coords = pd.read_csv(self.path_geo_coords)
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

        # path_dropped_na = f'{project_directory}/processed/csv_files/france_2017/path_opendatasoft_dropped_na.csv'
        # self.default_read(self.path_opendatasoft, path_dropped_na) # tailored read with specified datatypes int float only possible when no NaN


        all_csv_cols = ['Code du département', 'Département', 'Code de la circonscription',
       'Circonscription', 'Code de la commune', 'Commune', 'Bureau de vote',
       'Inscrits', 'Abstentions', '% Abs/Ins', 'Votants', '% Vot/Ins',
       'Blancs', '% Blancs/Ins', '% Blancs/Vot', 'Nuls', '% Nuls/Ins',
       '% Nuls/Vot', 'Exprimés', '% Exp/Ins', '% Exp/Vot', 'N°Panneau', 'Sexe',
       'Nom', 'Prénom', 'Voix', '% Voix/Ins', '% Voix/Exp', 'Code Insee',
       'Coordonnées', 'Nom Bureau Vote', 'Adresse', 'Code Postal', 'Ville',
       'uniq_bdv']
        cols_to_read = ['Coordonnées', 'Code du département', 'Département',
         'Commune', 'Inscrits', 'Abstentions', '% Abs/Ins',
         'Adresse', 'Code Postal']

        dict_dtype = {'Coordonnées':'object', 'Code du département':'object', 'Département':'object',
                      'Commune':'object', 'Abstentions':'int16', 'Inscrits':'int16', '% Abs/Ins':'float32',
                      'Adresse':'object', 'Code Postal':'object'}

        #header=0, skiprows=[0,],
        df = pd.read_csv(self.path_opendatasoft, sep=';', lineterminator='\r', low_memory=False)
        df = df.dropna()
        df = df[cols_to_read]
        df['latitude'] = df['Coordonnées'].apply(lambda x: float(x.split(',')[0]) if type(x) is str else x)
        df['longitude'] = df['Coordonnées'].apply(lambda x: float(x.split(',')[1]) if type(x) is str else x)
        df = df.drop('Coordonnées', axis=1)
        renamed_cols = {'Commune': 'Libellé de la commune', 'Département': 'Libellé du département'}
        df.rename(columns=renamed_cols, inplace=True)
        # df = self.ammend_jura_ain(df)
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

        df_with_paris = df_with_paris.rename(columns={'% Abs/Ins': 'Pourcentage_Abstentions'})
        df_with_paris['Abstentions'] = df_with_paris['Abstentions'].astype(int, )
        df_with_paris['Pourcentage_Abstentions'] = df_with_paris['Pourcentage_Abstentions'].astype(float, )


        return df_with_paris






class Process_france2022(Table_inserts):
    def __init__(self, path_opendatasoft):
        super().__init__()
        self.path_opendatasoft = path_opendatasoft
        self.path_geo_coords = path_geo_coords

    def join_for_paris(self):
        # df_datagouv_france2017 = pd.read_csv(self.path_datagouv_france2017, encoding = "ISO-8859-1", sep =';', decimal =',')
        # df_geo_coords = pd.read_csv(self.path_geo_coords)
        # df_paris = df_datagouv_france2017[df_datagouv_france2017['Libellé du département'] == 'Paris']
        # df_paris['col_merge'] = df_paris['Code du département'].apply(lambda x: str(x)) + '-' + df_paris[
        #     'Code de la circonscription'].apply(lambda x: str(x)).apply(lambda x: '0' + x if len(x) < 2 else x) + '_' + \
        #                         df_paris['Code du b.vote'].apply(lambda x: str(x)).apply(
        #                             lambda x: '0' + x if len(x) < 4 else x)
        # postal = df_geo_coords['code_postal'].fillna('00')
        # is_paris = postal.apply(lambda x: True if x.startswith('75') else False)
        # geo_paris = df_geo_coords[is_paris]
        # geo_paris['col_merge'] = geo_paris['circonscription_code'] + '_' + geo_paris['code_postal'].apply(
        #     lambda x: str(x)[-2:]) + geo_paris['code'].apply(lambda x: str(x)[-2:])
        #
        # df_merged = pd.merge(geo_paris, df_paris, left_on='col_merge', right_on='col_merge')
        return 'df_merged'

    def add_paris(self, df):
        #paris_with_coords = pd.read_csv(self.path_paris)

        # paris_with_coords = self.join_for_paris()
        # keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département',
        #                 'Libellé de la commune', '% Abs/Ins', 'Inscrits', 'Abstentions', 'geo_adresse']
        # paris_keep_columns = paris_with_coords[keep_columns]
        # renamed_cols = {'geo_adresse': 'Adresse complète'}
        # paris_keep_columns.rename(columns=renamed_cols, inplace=True)
        # paris_keep_columns['Code du département'] = paris_keep_columns['Code du département'].apply(lambda x: str(x))
        # paris_keep_columns = self.create_denomination_complete(paris_keep_columns)
        # df = df.append(paris_keep_columns)
        return 'df'

    def prepare_data_opendatasoft(self):

        # path_dropped_na = f'{project_directory}/processed/csv_files/france_2017/path_opendatasoft_dropped_na.csv'
        # self.default_read(self.path_opendatasoft, path_dropped_na) # tailored read with specified datatypes int float only possible when no NaN


        all_csv_cols = ['Code du département', 'Libellé du département',
       'Code de la circonscription', 'Libellé de la circonscription',
       'Code de la commune', 'Libellé de la commune', 'Code du b.vote',
       'Inscrits', 'Abstentions', '% Abs/Ins', 'Votants', '% Vot/Ins',
       'Blancs', '% Blancs/Ins', '% Blancs/Vot', 'Nuls', '% Nuls/Ins',
       '% Nuls/Vot', 'Exprimés', '% Exp/Ins', '% Exp/Vot', 'N°Panneau', 'Sexe',
       'Nom', 'Prénom', 'Voix', '% Voix/Ins', '% Voix/Exp',
       'Code Officiel EPCI', 'Nom Officiel EPCI', 'Code Officiel Région',
       'Nom Officiel Région', 'scrutin_code', 'location', 'lib_du_b_vote']

        cols_to_read = ['location', 'Code du département', 'Libellé du département',
         'Libellé de la commune', 'Inscrits', 'Abstentions', '% Abs/Ins', 'lib_du_b_vote']

        # dict_dtype = {'Coordonnées':'object', 'Code du département':'object', 'Département':'object',
        #               'Commune':'object', 'Abstentions':'int16', 'Inscrits':'int16', '% Abs/Ins':'float32',
        #               'Adresse':'object', 'Code Postal':'object'}

        #header=0, skiprows=[0,],
        df = pd.read_csv(self.path_opendatasoft, sep=';', lineterminator='\r', low_memory=False)
        df = df.dropna()
        df = df[cols_to_read]
        df['latitude'] = df['location'].apply(lambda x: float(x.split(',')[0]) if type(x) is str else x)
        df['longitude'] = df['location'].apply(lambda x: float(x.split(',')[1]) if type(x) is str else x)
        df = df.drop('location', axis=1)
        # renamed_cols = {'Commune': 'Libellé de la commune', 'Département': 'Libellé du département'}
        # df.rename(columns=renamed_cols, inplace=True)
        # df = self.ammend_jura_ain(df)
        df['Code du département'] = df['Code du département'].apply(lambda x: str(x)[1:])  # truncate unwanted '\n'

        df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ')'
        df['Adresse complète'] = df['lib_du_b_vote'].map(str) + ' ' + df['Libellé de la commune'].map(str)
        df = df.drop(['lib_du_b_vote'], axis=1)
        #df_with_paris = self.add_paris(df)

        # df_with_paris = df_with_paris.sort_values(by='Code du département')
        # df_with_paris['% Abs/Ins'] = df_with_paris['% Abs/Ins'].apply(lambda x: x.replace(',', '.') if type(x) == str else x)
        renamed_cols = {'% Abs/Ins': 'Pourcentage_Abstentions'}
        df.rename(columns=renamed_cols, inplace=True)
        #
        # df_with_paris = df_with_paris.rename(columns={'% Abs/Ins': 'Pourcentage_Abstentions'})
        # df_with_paris['Abstentions'] = df_with_paris['Abstentions'].astype(int, )
        # df_with_paris['Pourcentage_Abstentions'] = df_with_paris['Pourcentage_Abstentions'].astype(float, )

        return df


if __name__ == '__main__':
    # process_france2017 = Process_france2017(path_opendatasoft_france2017, path_abstentions_france2017, path_paris_france2017)
    # if process_france2017.database_name == 'dev_ou_sont_les_abstentions':
    #     print('LOAD and process opendatasoft source csv data')
    #     df_2017 = process_france2017.prepare_data_opendatasoft()
    # else:
    #     df_2017 = process_france2017.prepare_df(path_abstentions_france2017)
    #
    # conn, cursor = process_france2017.connect_driver()
    # dbExists = process_france2017.check_database_exists(conn, cursor)
    # if dbExists is False:
    #     process_france2017.create_db(cursor)
    # conn_orm, db = process_france2017.connect_orm()
    #
    # Session = sessionmaker(bind=db)
    # session = Session()
    # table_name = 'france_pres_2017'
    #
    # all_columns = list(df_2017.columns)
    # print(all_columns)
    # columns_for_table = list()
    #
    # for col in all_columns:
    #     if col in ['Code du département', 'Libellé du département', 'dénomination complète',
    #                'Libellé de la commune',
    #                'Adresse complète']:
    #         columns_for_table.append(Column(col, String, key=col.replace(' ', '_'), ))
    #
    #     elif col in ['Abstentions', 'Inscrits']:
    #         columns_for_table.append(Column(col, Integer, key=col.replace(' ', '_'), ))
    #
    #     else:
    #         columns_for_table.append(Column(col, Float, key=col.replace(' ', '_'), ))
    #
    # metadata_obj = MetaData()
    # france_pres_2017 = Table(table_name, metadata_obj, *(column for column in columns_for_table), )
    # metadata_obj.create_all(db)
    #
    # df_2017.to_sql('france_pres_2017', con=session.get_bind(), if_exists='replace', index=False, chunksize=800)
    #
    #




    process_france2022 = Process_france2022(path_opendatasoft_france2022)
    if process_france2022.database_name == 'dev_ou_sont_les_abstentions':
        print('LOAD and process opendatasoft source csv data')
        df_2022 = process_france2022.prepare_data_opendatasoft()
    else:
        print('No dataframe to work with')

    conn, cursor = process_france2022.connect_driver()
    dbExists = process_france2022.check_database_exists(conn, cursor)
    if dbExists is False:
        process_france2022.create_db(cursor)
    conn_orm, db = process_france2022.connect_orm()

    Session = sessionmaker(bind=db)
    session = Session()
    table_name = 'france_pres_2022'

    all_columns = list(df_2022.columns)
    print(all_columns)
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

    df_2022.to_sql('france_pres_2022', con=session.get_bind(), if_exists='replace', index=False, chunksize=800)


import pandas as pd
import numpy as nd

import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, select, distinct, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect

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


class Process_data:
	def __init__(self, path_abstentions, path_paris):
		self.path_abstentions = path_abstentions
		self.path_paris = path_paris

	def create_denomination_complete(self, df):
		df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ') '
		return df

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
		df = pd.read_csv(path)

		df = df.dropna()
		renamed_cols = {'ville': 'Libellé de la commune', 'abs_ins': '% Abs/Ins', 'abstentions': 'Abstentions',
						'inscrits': 'Inscrits', 'libelle_du_departement': 'Libellé du département'}
		df.rename(columns=renamed_cols, inplace=True)
		df = self.ammend_jura_ain(df)
		df['Code du département'] = df['code_postal'].apply(lambda x: str(x)[:2])

		df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ') '
		df['Adresse complète'] = df['adresse'].map(str) + ' ' + df['code_postal'].map(str)
		keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département',
						'dénomination complète',
						'Libellé de la commune', '% Abs/Ins', 'Inscrits',
						'Abstentions', 'Adresse complète']
		df = df[keep_columns]
		df_with_paris = self.add_paris(df)
		df_with_paris = df_with_paris.sort_values(by='Code du département')

		return df_with_paris

	def liste_communes(self, departements):
		# create dictionary with all communes for entered departements
		resu = {}
		df = self.prepare_df(self.path_abstentions)
		# df = add_paris(df)

		for i in departements:
			dep = i.split(' ')
			communes = list(df[df['Libellé du département'] == dep[0]]['Libellé de la commune'].unique())
			communes = [i + ' ' + dep[1] for i in communes]
			communes.insert(0, "Département entier {}".format(dep[1]))
			resu[i] = communes

		return resu

	def all_departements(self):
		df = self.prepare_df(self.path_abstentions)
		# df = add_paris(df)
		res = list(df['dénomination complète'].unique())
		return res


class Process_france2017(Process_data):
	def __init__(self, path_abstentions, path_paris):
		super().__init__(path_abstentions, path_paris)


if __name__ == '__main__':

    process_france2017 = Process_france2017(path_abstentions_france2017, path_paris_france2017)
    df_2017 = process_france2017.prepare_df(path_abstentions_france2017)

    df_2017['% Abs/Ins'] = df_2017['% Abs/Ins'].apply(lambda x: x.replace(',', '.') if type(x)== str else x)
    df_2017[df_2017['Code du département']== '75' ]

    conn, cursor = connect_driver()
    conn_orm, db = connect_orm()

    Session = sessionmaker(bind=db)
    session = Session()

    table_name = 'france_pres_2017'
    df_2017 = df_2017.rename(columns={'% Abs/Ins': 'Pourcentage_Absentions'})
    df_2017['Abstentions'] = df_2017['Abstentions'].astype(int, )
    df_2017['Pourcentage_Absentions'] = df_2017['Pourcentage_Absentions'].astype(float, )
    all_columns = list(df_2017.columns)
    columns_for_table = list()

    for col in all_columns:
        if col in ['Code du département', 'Libellé du département', 'dénomination complète', 'Libellé de la commune',
                   'Adresse complète']:
            columns_for_table.append(Column(col, String, key=col.replace(' ', '_'), ))

        elif col in ['Abstentions', 'Inscrits']:
            columns_for_table.append(Column(col, Integer, key=col.replace(' ', '_'), ))

        else:
            columns_for_table.append(Column(col, Float, key=col.replace(' ', '_'), ))

    metadata_obj = MetaData()
    france_pres_2017 = Table(table_name, metadata_obj, *(column for column in columns_for_table), )
    metadata_obj.create_all(db)

    df_2017.to_sql('france_pres_2017', con=session.get_bind(), if_exists='append', index=False)

# -*- coding: latin-1 -*-
import numpy as nd
import pandas as pd
import os
from keplergl import KeplerGl
from resource import getrusage, RUSAGE_SELF
import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, select, distinct
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import registry
from datetime import datetime
from map_configuration import dbmapconfig

import logging

database_name = 'ou_sont_les_abstentions'

now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

log_filename = f'logs/app_{now}.log'
os.makedirs(os.path.dirname(log_filename), exist_ok=True)
logging.basicConfig(level=logging.DEBUG,
                    filename=log_filename ,
                    filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


current_directory = os.path.dirname(__file__)
path_abstentions_france2017 = f'{current_directory}/processed/csv_files/france_2017/abstentions.csv'
path_paris_france2017 = f'{current_directory}/processed/csv_files/france_2017/geo_paris.csv'
path_abstentions_france2022 = f'{current_directory}/processed/csv_files/france_2022/abstentions.csv'
path_paris_france2022 = f'{current_directory}/processed/csv_files/france_2022/no_data.csv'


class User:
    pass


class Table_queries:
	def __init__(self, path_abstentions, path_paris, table_name, query_aws_table):
		self.path_abstentions = path_abstentions
		self.path_paris = path_paris
		self.table_name =table_name
		self.static_francemetropole=True
		self.query_aws_table=query_aws_table


	def define_mapper(self):
		all_columns = ['longitude',
					   'latitude',
					   'Code du département',
					   'Libellé du département',
					   'dénomination complète',
					   'Libellé de la commune',
					   'Pourcentage_Absentions',
					   'Inscrits',
					   'Abstentions',
					   'Adresse complète']

		columns_for_table = list()

		for col in all_columns:
			if col in ['Code du département', 'Libellé du département', 'dénomination complète',
					   'Libellé de la commune', 'Adresse complète']:
				columns_for_table.append(Column(col, String, key=col.replace(' ', '_'), primary_key=True))
			elif col in ['Abstentions', 'Inscrits']:
				columns_for_table.append(Column(col, Integer, key=col.replace(' ', '_'), primary_key=True))


			else:
				columns_for_table.append(Column(col, Float, key=col.replace(' ', '_'), primary_key=True))

		# Create the Metadata Object
		metadata_obj = MetaData()
		france_pres_2017 = Table(self.table_name, metadata_obj, *(column for column in columns_for_table)) #
		metadata_obj.create_all(self.db)

		mapper_registry = registry()
		mapper_registry.map_imperatively(User, france_pres_2017)

	def get_credentials(self, use_aws):
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
		if use_aws:
			host = 'ec2-54-173-241-113.compute-1.amazonaws.com' # Public IPv4 DNS, contains elastic IP to AWS instance
			port = os.environ.get('PORT_POSTGRESQL_AWS')
			passw = os.environ.get('PASSPOSTGRES')
		else:
			host = '127.0.0.1'
			port = os.environ.get('PORT_POSTGRESQL')
			passw = os.environ.get('PASSPOSTGRES')

		return user, passw, host, port

	def connect_driver(self):
		"""
        Return psycopg2 connection and cursor objects

        Parameters
        ---------
        None

        Returns
        -------
        connction and cursor class
        """
		user, passw, host, port = self.get_credentials(self.query_aws_table)

		# establishing the connection
		conn = psycopg2.connect(
			user=user, password=passw, host=host, port=port
		)
		conn.autocommit = True
		# Creating a cursor object using the cursor() method
		cursor = conn.cursor()
		return conn, cursor

	def connect_orm(self):
		"""
        Return sqlalchemy Engine instance and connection

        Parameters
        ---------
        None

        Returns
        -------
        sqlalchemy Engine instance and connection
        """
		user, passw, host, port = self.get_credentials(self.query_aws_table)

		conn_string = f'postgresql://{user}:{passw}@{host}:{port}/{database_name}'
		engine = create_engine(conn_string, pool_size=42)
		conn_orm = engine.connect()
		return conn_orm, engine


	def create_denomination_complete(self, df):
		df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ') '
		#self.df['dénomination complète'] = self.df['Libellé du département'] + ' (' + self.df['Code du département'] + ') '
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
		df['code_postal'] = nd.where((df['Libellé du département'] == 'Jura') & (df['Libellé de la commune'] == 'Chancia'),
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
		keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département','dénomination complète',
						'Libellé de la commune', '% Abs/Ins', 'Inscrits',
						'Abstentions', 'Adresse complète']
		df = df[keep_columns]
		df_with_paris = self.add_paris(df)
		df_with_paris = df_with_paris.sort_values(by='Code du département')

		return df_with_paris

	def francemetropole(self):
		res = KeplerGl(height=500, data={"data_1": self.df}, config=_mapconfig)
		# print(self.df.to_dict())
		# res = KeplerGl(height=500, data={"data_1": self.df.to_dict()}, config=_mapconfig)
		return res


	def liste_communes(self, departements)-> dict:
		#create dictionary with all communes for entered departements
		resu = {}
		#df = self.prepare_df(self.path_abstentions)
		df = self.df

		for i in departements:
			dep = i.split(' ')
			communes = list(df[df['Libellé du département']==dep[0]]['Libellé de la commune'].unique() )
			communes = [i + ' '+ dep[1] for i in communes]
			communes.insert(0, "Département entier {}".format(dep[1]))
			resu[i] = communes

		return resu

	def all_departements(self):
		df = self.df
		res = list(df['dénomination complète'].unique())
		return res

	def communes_for_map(self, communes_liste):
		list_dep_entier = list()
		df = self.df
		deps_communes = list()
		for i in communes_liste:
			new = i.split('(')
			if len(new) > 2:
				return 'Problem in commune name'
			new[0] = new[0].strip(' ')
			new[-1] = new[-1].strip('( )')

			if 'Département entier' in new[0]:
				departement_code = pd.DataFrame(data=[new[-1]], columns=['Code du département'])
				departement_entier = pd.merge(df, departement_code, left_on=['Code du département'],
							   right_on=['Code du département'])
				list_dep_entier.append(departement_entier)
				continue
			deps_communes.append(new)

		#deps_communes = nd.array(deps_communes) # returns error if deps_communes empty
		df_choix = pd.DataFrame(data=deps_communes, columns=['Libellé de la commune', 'Code du département'])
		filtered_df = pd.merge(df, df_choix, left_on=['Code du département', 'Libellé de la commune'],
							   right_on=['Code du département', 'Libellé de la commune'])
		if len(list_dep_entier) > 0:
			for df_departement in list_dep_entier:
				filtered_df = filtered_df.append(df_departement)
		input_dict = filtered_df.to_dict('split')
		res = KeplerGl(height=500, data={"data_1": input_dict}, config=_mapconfig)
		return res

class Queries_france2017(Table_queries):
	def __init__(self, path_abstentions, path_paris, query_aws_table):
		super().__init__(path_abstentions, path_paris, table_name = 'france_pres_2017', query_aws_table=query_aws_table)
		self.conn_orm, self.db = self.connect_orm()
		Session = sessionmaker(autocommit=False, autoflush=False, bind=self.db)
		self.session = Session()
		self.define_mapper()


	def create_dict_for_map(self, list_data, columns):
		data_indices = list(range(len(list_data)))
		column_label_for_map = [col.replace('_', ' ') for col in columns]
		dict_data = {'index': data_indices, 'columns': column_label_for_map, 'data': list_data}
		return dict_data

	def query_francemetropole(self):
		call_col = ['longitude', 'latitude', 'Libellé_du_département', 'Libellé_de_la_commune','Pourcentage_Absentions', 'Inscrits', 'Abstentions', 'Adresse_complète']
		ref_to_cols = [User.__dict__[key] for key in call_col]
		iter_stm = select(*ref_to_cols)
		data = self.session.execute(iter_stm).all()
		list_data = [list(row) for row in data]
		dict_data = self.create_dict_for_map(list_data, call_col)
		res = KeplerGl(height=500, data={"data_1": dict_data}, config=dbmapconfig)
		return res

	def query_all_departements(self):
		def dep_val(denomination):
			code_dep = int(denomination.strip(')').split('(')[-1][-2:])
			return code_dep
		query_denomination_complete = self.session.query(distinct(User.dénomination_complète)).all()
		all_departements = [row[0].strip(' ') for row in query_denomination_complete]
		all_departements.sort(key=dep_val)
		return all_departements

	def query_liste_communes(self, departements)-> dict:
		#create dictionary with all communes for entered departements
		resu = {}
		for i in departements:
			dep = i.strip(' ').split(' ')
			query = self.session.query(User.Libellé_de_la_commune).filter(
				User.dénomination_complète == f"{i} ").distinct()
			communes = [row[0] for row in query.all()]
			communes = [name + ' '+ dep[1] for name in communes]
			communes.insert(0, "Département entier {}".format(dep[1]))
			resu[i] = communes

		return resu

	def generate_kepler_map(self, communes_liste):
		deps_communes = list()
		call_col = ['longitude', 'latitude', 'Libellé_du_département', 'Libellé_de_la_commune','Pourcentage_Absentions', 'Inscrits', 'Abstentions', 'Adresse_complète']
		ref_to_cols = [User.__dict__[key] for key in call_col]
		data_chunks = list()
		for i in communes_liste:
			new = i.split('(')
			if len(new) > 2:
				return 'Problem in commune name'
			new[0] = new[0].strip(' ')
			new[-1] = new[-1].strip('( )')
			if 'Département entier' in new[0]:
				iter_stm = select(*ref_to_cols).where(User.Code_du_département.in_([new[-1]]))
				data = self.session.execute(iter_stm).all()
				data_chunks.extend(data)
				continue
			deps_communes.append(new)
			iter_stm = select(*ref_to_cols).where(User.Libellé_de_la_commune.in_([new[0]]), User.Code_du_département.in_([new[-1]]))
			data = self.session.execute(iter_stm).all()
			data_chunks.extend(data)

		list_data  = [list(row) for row in data_chunks]
		dict_data = self.create_dict_for_map(list_data, call_col)
		res = KeplerGl(height=500, data={"data_1": dict_data}, config=dbmapconfig)
		return res



class Queries_france2022(Table_queries):

	def __init__(self, path_absentions, path_paris, table_name):
		pass
		super().__init__(path_absentions, path_paris, table_name = 'france_pres_2022')
		self.df = self.prepare_df(self.path_abstentions)


	def add_paris(self, df):
		# paris_with_coords = pd.read_csv(self.path_paris)
		# keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département',
		# 				'Libellé de la commune', '% Abs/Ins', 'Inscrits', 'Abstentions', 'geo_adresse']
		# paris_keep_columns = paris_with_coords[keep_columns]
		# renamed_cols = {'geo_adresse': 'Adresse complète'}
		# paris_keep_columns.rename(columns=renamed_cols, inplace=True)
		# paris_keep_columns['Code du département'] = paris_keep_columns['Code du département'].apply(lambda x: str(x))
		# paris_keep_columns = self.create_denomination_complete(paris_keep_columns)
		# df = df.append(paris_keep_columns)
		return df


	def prepare_df(self, path):
		df = pd.read_csv(path)

		df = df.dropna()
		# renamed_cols = {'ville': 'Libellé de la commune', 'abs_ins': '% Abs/Ins', 'abstentions': 'Abstentions',
		# 				'inscrits': 'Inscrits', 'libelle_du_departement': 'Libellé du département'}
		# df.rename(columns=renamed_cols, inplace=True)
		# df = self.ammend_jura_ain(df)
		df['Code du département'] = df['Code du département'].apply(lambda x: str(x)[1:]) # remove unwanted '\n'

		df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ')'

		df['Adresse complète'] = df['lib_du_b_vote'].map(str) + ' ' + df['Libellé de la commune'].map(str) + ' ' + df['Libellé du département'].map(str)


		keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département','dénomination complète',
						'Libellé de la commune', '% Abs/Ins', 'Inscrits',
						'Abstentions', 'Adresse complète']
		df = df[keep_columns]
		df_with_paris = self.add_paris(df)
		df_with_paris = df_with_paris.sort_values(by='Code du département')

		return df_with_paris


process_france2017 = Queries_france2017(path_abstentions_france2017, path_paris_france2017, query_aws_table=True)
#france2022 = Process_france2022(path_abstentions_france2022, path_paris_france2022, table_name='france_pres_2022')



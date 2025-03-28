# -*- coding: latin-1 -*-
import numpy as nd
import pandas as pd
import os
from keplergl import KeplerGl

import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, select, distinct
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import registry
from datetime import datetime
from config import dbmapconfig
from db_connections import Connectdb, log_memory_after, database_name, table_connection
from config import configurations, logging, now

logging.info(f'DATABSE is {database_name}')





current_directory = os.path.dirname(__file__)
path_abstentions_france2017 = f'{current_directory}/processed/csv_files/france_2017/abstentions.csv'
path_paris_france2017 = f'{current_directory}/processed/csv_files/france_2017/geo_paris.csv'
path_abstentions_france2022 = f'{current_directory}/processed/csv_files/france_2022/abstentions.csv'
path_paris_france2022 = f'{current_directory}/processed/csv_files/france_2022/no_data.csv'


class User_france2017:
    pass

class User_france2022:
    pass



class Table_queries(Connectdb):
	def __init__(self, table_connection, france_metropole_static_html):
		super().__init__(database_name=database_name, table_connection=table_connection)
		self.static_francemetropole = france_metropole_static_html
		self.table_connection = table_connection
		self.conn_orm, self.db, _ = self.connect_orm()
		Session = sessionmaker(autocommit=False, autoflush=False, bind=self.db)
		self.session = Session()




	def francemetropole(self):
		res = KeplerGl(height=500, data={"data_1": self.df}, config=_mapconfig)
		# print(self.df.to_dict())
		# res = KeplerGl(height=500, data={"data_1": self.df.to_dict()}, config=_mapconfig)
		return res
	# def all_departements(self):
	# 	df = self.df
	# 	res = list(df['d�nomination compl�te'].unique())
	# 	return res



	def create_dict_for_map(self, list_data, columns):
		data_indices = list(range(len(list_data)))
		column_label_for_map = [col.replace('_', ' ') for col in columns]
		dict_data = {'index': data_indices, 'columns': column_label_for_map, 'data': list_data}
		return dict_data





class Queries_france2017(Table_queries):
	def __init__(self, table_connection, france_metropole_static_html):
		super().__init__(table_connection=table_connection, france_metropole_static_html=france_metropole_static_html)
		self.table_name = 'france_pres_2017'
		self.define_mapper_france2017()

	# def create_dict_for_map(self, list_data, columns):
	# 	data_indices = list(range(len(list_data)))
	# 	column_label_for_map = [col.replace('_', ' ') for col in columns]
	# 	dict_data = {'index': data_indices, 'columns': column_label_for_map, 'data': list_data}
	# 	return dict_data



	def define_mapper_france2017(self):
		all_columns = ['longitude',
					   'latitude',
					   'Code du d�partement',
					   'Libell� du d�partement',
					   'd�nomination compl�te',
					   'Libell� de la commune',
					   'Pourcentage_Abstentions',
					   'Inscrits',
					   'Abstentions',
					   'Adresse compl�te']

		columns_for_table = list()

		for col in all_columns:
			if col in ['Code du d�partement', 'Libell� du d�partement', 'd�nomination compl�te',
					   'Libell� de la commune', 'Adresse compl�te']:
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
		mapper_registry.map_imperatively(User_france2017, france_pres_2017)



	def query_francemetropole(self):
		call_col = ['longitude', 'latitude', 'Libell�_du_d�partement', 'Libell�_de_la_commune','Pourcentage_Abstentions', 'Inscrits', 'Abstentions', 'Adresse_compl�te']
		ref_to_cols = [User_france2017.__dict__[key] for key in call_col]
		iter_stm = select(*ref_to_cols)
		data = self.session.execute(iter_stm).all()
		list_data = [list(row) for row in data]
		dict_data = self.create_dict_for_map(list_data, call_col)
		res = KeplerGl(height=500, data={"data_1": dict_data}, config=dbmapconfig)
		return res

	def query_all_departements(self):
		def dep_val(denomination):
			code_dep =denomination.strip(')').split('(')[-1][-2:]
			return code_dep
		def int_dep_val(denomination):
			code_dep =int(denomination.strip(')').split('(')[-1][-2:])
			return code_dep

		query_denomination_complete = self.session.query(distinct(User_france2017.d�nomination_compl�te)).all()
		logging.info(f'RAW query DISTINCT denomination complete IS {query_denomination_complete}')
		all_departements = [row[0].strip(' ') for row in query_denomination_complete]
		logging.info(f'LIST all_departements is {all_departements}')
		metropole = [dep for dep in all_departements if dep_val(dep).isdigit()]
		logging.info(f'LIST metropole is {metropole}')
		corse = [dep for dep in all_departements if dep not in metropole]
		logging.info(f'LIST corse is {corse}')

		metropole.sort(key=int_dep_val)
		logging.info(f'AFTER sort LIST metropole is {metropole}')
		return metropole

	def query_liste_communes(self, departements)-> dict:
		#create dictionary with all communes for entered departements
		resu = {}
		for i in departements:
			dep = i.strip(' ').split(' ')
			query = self.session.query(User_france2017.Libell�_de_la_commune).filter(
				User_france2017.d�nomination_compl�te == f"{i}").distinct()#Beware of f"{i} " with trailing space
			communes = [row[0] for row in query.all()]
			communes.sort()
			communes = [name + ' '+ dep[1] for name in communes]
			communes.insert(0, "D�partement entier {}".format(dep[1]))
			resu[i] = communes

		return resu

	def generate_kepler_map(self, communes_liste):
		deps_communes = list()
		call_col = ['longitude', 'latitude', 'Libell�_du_d�partement', 'Libell�_de_la_commune','Pourcentage_Abstentions', 'Inscrits', 'Abstentions', 'Adresse_compl�te']
		ref_to_cols = [User_france2017.__dict__[key] for key in call_col]
		data_chunks = list()
		for i in communes_liste:
			new = i.split('(')
			if len(new) > 2:
				return 'Problem in commune name'
			new[0] = new[0].strip(' ')
			new[-1] = new[-1].strip('( )')
			if 'D�partement entier' in new[0]:
				iter_stm = select(*ref_to_cols).where(User_france2017.Code_du_d�partement.in_([new[-1]]))
				data = self.session.execute(iter_stm).all()
				data_chunks.extend(data)
				continue
			deps_communes.append(new)
			iter_stm = select(*ref_to_cols).where(User_france2017.Libell�_de_la_commune.in_([new[0]]), User_france2017.Code_du_d�partement.in_([new[-1]]))
			data = self.session.execute(iter_stm).all()
			data_chunks.extend(data)

		list_data  = [list(row) for row in data_chunks]
		dict_data = self.create_dict_for_map(list_data, call_col)
		res = KeplerGl(height=500, data={"data_1": dict_data}, config=dbmapconfig)
		return res



class Queries_france2022(Table_queries):

	def __init__(self, table_connection, france_metropole_static_html):
		super().__init__(table_connection=table_connection, france_metropole_static_html=france_metropole_static_html)
		self.table_name = 'france_pres_2022'
		self.define_mapper_france2022()

	def define_mapper_france2022(self):
		all_columns = ['longitude',
					   'latitude',
					   'Code du d�partement',
					   'Libell� du d�partement',
					   'd�nomination compl�te',
					   'Libell� de la commune',
					   'Pourcentage_Abstentions',
					   'Inscrits',
					   'Abstentions',
					   'Adresse compl�te']

		columns_for_table = list()

		for col in all_columns:
			if col in ['Code du d�partement', 'Libell� du d�partement', 'd�nomination compl�te',
					   'Libell� de la commune', 'Adresse compl�te']:
				columns_for_table.append(Column(col, String, key=col.replace(' ', '_'), primary_key=True))
			elif col in ['Abstentions', 'Inscrits']:
				columns_for_table.append(Column(col, Integer, key=col.replace(' ', '_'), primary_key=True))


			else:
				columns_for_table.append(Column(col, Float, key=col.replace(' ', '_'), primary_key=True))

		# Create the Metadata Object
		metadata_obj = MetaData()
		france_pres_2022 = Table(self.table_name, metadata_obj, *(column for column in columns_for_table)) #
		metadata_obj.create_all(self.db)

		mapper_registry = registry()
		mapper_registry.map_imperatively(User_france2022, france_pres_2022)

	def query_francemetropole(self):
		call_col = ['longitude', 'latitude', 'Libell�_du_d�partement', 'Libell�_de_la_commune','Pourcentage_Abstentions', 'Inscrits', 'Abstentions', 'Adresse_compl�te']
		ref_to_cols = [User_france2022.__dict__[key] for key in call_col]
		iter_stm = select(*ref_to_cols)
		data = self.session.execute(iter_stm).all()
		list_data = [list(row) for row in data]
		dict_data = self.create_dict_for_map(list_data, call_col)
		res = KeplerGl(height=500, data={"data_1": dict_data}, config=dbmapconfig)
		return res

	def add_paris(self, df):
		# paris_with_coords = pd.read_csv(self.path_paris)
		# keep_columns = ['longitude', 'latitude', 'Code du d�partement', 'Libell� du d�partement',
		# 				'Libell� de la commune', '% Abs/Ins', 'Inscrits', 'Abstentions', 'geo_adresse']
		# paris_keep_columns = paris_with_coords[keep_columns]
		# renamed_cols = {'geo_adresse': 'Adresse compl�te'}
		# paris_keep_columns.rename(columns=renamed_cols, inplace=True)
		# paris_keep_columns['Code du d�partement'] = paris_keep_columns['Code du d�partement'].apply(lambda x: str(x))
		# paris_keep_columns = self.create_denomination_complete(paris_keep_columns)
		# df = df.append(paris_keep_columns)
		return df


	def query_all_departements(self):
		def dep_val(denomination):
			code_dep =denomination.strip(')').split('(')[-1][-2:]
			return code_dep

		def int_dep_val(denomination):
			code_dep =int(denomination.strip(')').split('(')[-1][-2:])
			return code_dep

		query_denomination_complete = self.session.query(distinct(User_france2022.d�nomination_compl�te)).all()
		logging.info(f'RAW query DISTINCT denomination complete IS {query_denomination_complete}')
		all_departements = [row[0].strip(' ') for row in query_denomination_complete]
		logging.info(f'LIST all_departements is {all_departements}')
		metropole = [dep for dep in all_departements if dep_val(dep).isdigit() and dep_val(dep) != '97']
		logging.info(f'LIST metropole is {metropole}')
		corse = [dep for dep in all_departements if dep not in metropole]
		logging.info(f'LIST corse is {corse}')

		metropole.sort(key=int_dep_val)
		logging.info(f'AFTER sort LIST metropole is {metropole}')
		return metropole



	def query_liste_communes(self, departements)-> dict:
		#create dictionary with all communes for entered departements
		resu = {}
		for i in departements:
			dep = i.strip(' ').split(' ')
			query = self.session.query(User_france2022.Libell�_de_la_commune).filter(
				User_france2022.d�nomination_compl�te == f"{i}").distinct()#Beware of f"{i} " with trailing space
			communes = [row[0] for row in query.all()]
			communes.sort()
			communes = [name + ' ' + dep[1] for name in communes]
			communes.insert(0, "D�partement entier {}".format(dep[1]))
			resu[i] = communes

		return resu

	def generate_kepler_map(self, communes_liste):
		deps_communes = list()
		call_col = ['longitude', 'latitude', 'Libell�_du_d�partement', 'Libell�_de_la_commune', 'Pourcentage_Abstentions', 'Inscrits', 'Abstentions', 'Adresse_compl�te']
		ref_to_cols = [User_france2022.__dict__[key] for key in call_col]
		data_chunks = list()
		for i in communes_liste:
			new = i.split('(')
			if len(new) > 2:
				return 'Problem in commune name'
			new[0] = new[0].strip(' ')
			new[-1] = new[-1].strip('( )')
			if 'D�partement entier' in new[0]:
				iter_stm = select(*ref_to_cols).where(User_france2022.Code_du_d�partement.in_([new[-1]]))
				data = self.session.execute(iter_stm).all()
				data_chunks.extend(data)
				continue
			deps_communes.append(new)
			iter_stm = select(*ref_to_cols).where(User_france2022.Libell�_de_la_commune.in_([new[0]]), User_france2022.Code_du_d�partement.in_([new[-1]]))
			data = self.session.execute(iter_stm).all()
			data_chunks.extend(data)

		list_data = [list(row) for row in data_chunks]
		dict_data = self.create_dict_for_map(list_data, call_col)
		res = KeplerGl(height=500, data={"data_1": dict_data}, config=dbmapconfig)
		return res





query_france2017 = Queries_france2017(table_connection=table_connection, france_metropole_static_html=configurations['france_metropole_static_html'])
query_france2022 = Queries_france2022(table_connection=table_connection,france_metropole_static_html=configurations['france_metropole_static_html'])


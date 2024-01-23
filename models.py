# -*- coding: latin-1 -*-
import numpy as nd
import pandas as pd
import os
from keplergl import KeplerGl


import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, select, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect

database_name = 'ou_sont_les_abstentions'


current_directory = os.path.dirname(__file__)
path_abstentions_france2017 = f'{current_directory}/processed/csv_files/france_2017/abstentions.csv'
path_paris_france2017 = f'{current_directory}/processed/csv_files/france_2017/geo_paris.csv'
path_abstentions_france2022 = f'{current_directory}/processed/csv_files/france_2022/abstentions.csv'
path_paris_france2022 = f'{current_directory}/processed/csv_files/france_2022/no_data.csv'

class Process_data:
	def __init__(self, path_abstentions, path_paris):
		self.path_abstentions = path_abstentions
		self.path_paris = path_paris
		self.df = self.prepare_df(self.path_abstentions)

	def get_credentials(self):
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
		# passw = os.environ.get('PASSPOSTGRES')
		passw = 'mynewpassword'
		host = '127.0.0.1'
		port = '5433'

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
		user, passw, host, port = self.get_credentials()

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
		user, passw, host, port = self.get_credentials()

		conn_string = f'postgresql://{user}:{passw}@{host}:{port}/{database_name}'
		engine = create_engine(conn_string)
		conn_orm = engine.connect()
		return conn_orm, engine


	def create_denomination_complete(self, df):
		df['d�nomination compl�te'] = df['Libell� du d�partement'] + ' (' + df['Code du d�partement'] + ') '
		#self.df['d�nomination compl�te'] = self.df['Libell� du d�partement'] + ' (' + self.df['Code du d�partement'] + ') '
		return df


	def add_paris(self, df):
		paris_with_coords = pd.read_csv(self.path_paris)
		keep_columns = ['longitude', 'latitude', 'Code du d�partement', 'Libell� du d�partement',
						'Libell� de la commune', '% Abs/Ins', 'Inscrits', 'Abstentions', 'geo_adresse']
		paris_keep_columns = paris_with_coords[keep_columns]
		renamed_cols = {'geo_adresse': 'Adresse compl�te'}
		paris_keep_columns.rename(columns=renamed_cols, inplace=True)
		paris_keep_columns['Code du d�partement'] = paris_keep_columns['Code du d�partement'].apply(lambda x: str(x))
		paris_keep_columns = self.create_denomination_complete(paris_keep_columns)
		df = df.append(paris_keep_columns)
		return df


	def ammend_jura_ain(self, df):
		df['code_postal'] = nd.where((df['Libell� du d�partement'] == 'Jura') & (df['Libell� de la commune'] == 'Chancia'),
									 '39102', df['code_postal'])
		df['code_postal'] = nd.where(
			(df['Libell� du d�partement'] == 'Jura') & (df['Libell� de la commune'] == 'Lavancia-Epercy'), '39283',
			df['code_postal'])
		return df


	def prepare_df(self, path):
		df = pd.read_csv(path)

		df = df.dropna()
		renamed_cols = {'ville': 'Libell� de la commune', 'abs_ins': '% Abs/Ins', 'abstentions': 'Abstentions',
						'inscrits': 'Inscrits', 'libelle_du_departement': 'Libell� du d�partement'}
		df.rename(columns=renamed_cols, inplace=True)
		df = self.ammend_jura_ain(df)
		df['Code du d�partement'] = df['code_postal'].apply(lambda x: str(x)[:2])

		df['d�nomination compl�te'] = df['Libell� du d�partement'] + ' (' + df['Code du d�partement'] + ') '
		df['Adresse compl�te'] = df['adresse'].map(str) + ' ' + df['code_postal'].map(str)
		keep_columns = ['longitude', 'latitude', 'Code du d�partement', 'Libell� du d�partement','d�nomination compl�te',
						'Libell� de la commune', '% Abs/Ins', 'Inscrits',
						'Abstentions', 'Adresse compl�te']
		df = df[keep_columns]
		df_with_paris = self.add_paris(df)
		df_with_paris = df_with_paris.sort_values(by='Code du d�partement')

		return df_with_paris

	def francemetropole(self):
		#df = self.prepare_df(self.path_abstentions)
		#df = self.df
		# keep_columns = ['longitude','latitude','Libell� de la commune','% Abs/Ins', 'Inscrits', 'Abstentions', 'Libell� du d�partement', 'Adresse compl�te']
		# df_keep_columns = self.df[keep_columns]
		res = KeplerGl(height=500, data={"data_1": self.df}, config=_mapconfig)
		# print(self.df.to_dict())
		# res = KeplerGl(height=500, data={"data_1": self.df.to_dict()}, config=_mapconfig)
		return res


	def liste_communes(self, departements):
		#create dictionary with all communes for entered departements
		resu = {}
		#df = self.prepare_df(self.path_abstentions)
		df = self.df

		for i in departements:
			dep = i.split(' ')
			communes = list(df[df['Libell� du d�partement']==dep[0]]['Libell� de la commune'].unique() )
			communes = [i + ' '+ dep[1] for i in communes]
			communes.insert(0, "D�partement entier {}".format(dep[1]))
			resu[i] = communes

		return resu

	def all_departements(self):
		#df = self.prepare_df(self.path_abstentions)
		df = self.df
		res = list(df['d�nomination compl�te'].unique())
		return res

	def query_all_departements(self):
		conn_orm, db = self.connect_orm()
		# query = 'f"select * from {table_name};"'
		query = "select * from france_pres_2017;"
		# all_table_cols = list(conn_orm.execute(''f"select * from {table_name};"'').keys())
		all_table_cols = list(conn_orm.execute(text(query)).keys())
		return 0


	def communes_for_map(self, communes_liste):
		#df = self.prepare_df(self.path_abstentions)
		list_dep_entier = list()
		df = self.df
		deps_communes = list()
		for i in communes_liste:
			new = i.split('(')
			if len(new) > 2:
				return 'Problem in commune name'
			new[0] = new[0].strip(' ')
			new[-1] = new[-1].strip('( )')

			if 'D�partement entier' in new[0]:
				departement_code = pd.DataFrame(data=[new[-1]], columns=['Code du d�partement'])
				departement_entier = pd.merge(df, departement_code, left_on=['Code du d�partement'],
							   right_on=['Code du d�partement'])
				list_dep_entier.append(departement_entier)
				continue
			deps_communes.append(new)

		#deps_communes = nd.array(deps_communes) # returns error if deps_communes empty
		#print("VOILA deps_communes   {}".format(deps_communes))
		df_choix = pd.DataFrame(data=deps_communes, columns=['Libell� de la commune', 'Code du d�partement'])
		#print("VOILA DF_CHOIX   {}".format(df_choix))
		filtered_df = pd.merge(df, df_choix, left_on=['Code du d�partement', 'Libell� de la commune'],
							   right_on=['Code du d�partement', 'Libell� de la commune'])
		if len(list_dep_entier) > 0:
			#filtered_df
			for df_departement in list_dep_entier:
				filtered_df = filtered_df.append(df_departement)
		input_dict = filtered_df.to_dict('split')
		#print("VOILA le dictionnaire   {}".format(input_dict) )
		res = KeplerGl(height=500, data={"data_1": input_dict}, config=_mapconfig)
		return res

class Process_france2017(Process_data):
	def __init__(self, path_abstentions, path_paris):
		super().__init__(path_abstentions, path_paris)

class Process_france2022(Process_data):

	def __init__(self, path_absentions, path_paris):
		super().__init__(path_absentions, path_paris)


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


	def prepare_df(self, path):
		df = pd.read_csv(path)

		df = df.dropna()
		# renamed_cols = {'ville': 'Libell� de la commune', 'abs_ins': '% Abs/Ins', 'abstentions': 'Abstentions',
		# 				'inscrits': 'Inscrits', 'libelle_du_departement': 'Libell� du d�partement'}
		# df.rename(columns=renamed_cols, inplace=True)
		# df = self.ammend_jura_ain(df)
		df['Code du d�partement'] = df['Code du d�partement'].apply(lambda x: str(x)[1:]) # remove unwanted '\n'

		df['d�nomination compl�te'] = df['Libell� du d�partement'] + ' (' + df['Code du d�partement'] + ') '

		df['Adresse compl�te'] = df['lib_du_b_vote'].map(str) + ' ' + df['Libell� de la commune'].map(str) + ' ' + df['Libell� du d�partement'].map(str)


		keep_columns = ['longitude', 'latitude', 'Code du d�partement', 'Libell� du d�partement','d�nomination compl�te',
						'Libell� de la commune', '% Abs/Ins', 'Inscrits',
						'Abstentions', 'Adresse compl�te']
		df = df[keep_columns]
		df_with_paris = self.add_paris(df)
		df_with_paris = df_with_paris.sort_values(by='Code du d�partement')

		return df_with_paris

process_france2017 = Process_france2017(path_abstentions_france2017, path_paris_france2017)
#process_france2022 = Process_france2022(path_abstentions_france2022, path_paris_france2022)


colorscheme = [
					  "#5A1846",
					  "#900C3F",
					  "#C70039",
					  "#E3611C",
					  "#F1920E",
					  "#FFC300"
					]

colorscheme = ['FFFF00', 'FFCC00', 'FF9900', 'FF6600', 'FF3300', 'FF0000']
_mapconfig = {
	  "version": "v1",
	  "config": {
		"visState": {
		  "filters": [],
		  "layers": [
			{
			  "id": "ltrbs46",
			  "type": "point",
			  "config": {
				"dataId": "data_1",
				"label": "Point",
				"color": [
				  18,
				  147,
				  154
				],
				"columns": {
				  "lat": "latitude",
				  "lng": "longitude",
				  "altitude": None
				},
				"isVisible": True,
				"visConfig": {
				  "radius": 17.2,
				  "fixedRadius": False,
				  "opacity": 0.8,
				  "outline": False,
				  "thickness": 2,
				  "strokeColor": None,
				  "colorRange": {
					"name": "Global Warming",
					"type": "sequential",
					"category": "Uber",
					"colors": colorscheme
				  },
				  "strokeColorRange": {
					"name": "Global Warming",
					"type": "sequential",
					"category": "Uber",
					"colors": colorscheme
				  },
				  "radiusRange": [
					0,
					50
				  ],
				  "filled": True
				},
				"hidden": False,
				"textLabel": [
				  {
					"field": None,
					"color": [
					  255,
					  255,
					  255
					],
					"size": 18,
					"offset": [
					  0,
					  0
					],
					"anchor": "start",
					"alignment": "center"
				  }
				]
			  },
			  "visualChannels": {
				"colorField": {
				  "name": "Abstentions",
				  "type": "integer"
				},
				"colorScale": "quantile",
				"strokeColorField": None,
				"strokeColorScale": "quantile",
				"sizeField": {
				  "name": "Inscrits",
				  "type": "integer"
				},
				"sizeScale": "sqrt"
			  }
			}
		  ],
		  "interactionConfig": {
			"tooltip": {
			  "fieldsToShow": {
				"data_1": [

				  {
					"name": "% Abs/Ins",
					"format": None
				  },
				  {
					"name": "Abstentions",
					"format": None
				  },
				  {
					"name": "Inscrits",
					"format": None
				  },
				  				  {
					"name": "Libell� du d�partement",
					"format": None
				  },
				  {
					"name": "Libell� de la commune",
					"format": None
				  },
				  {
					"name": "Adresse compl�te",
					"format": None
				  }
				]
			  },
			  "compareMode": False,
			  "compareType": "absolute",
			  "enabled": True
			},
			"brush": {
			  "size": 0.5,
			  "enabled": False
			},
			"geocoder": {
			  "enabled": False
			},
			"coordinate": {
			  "enabled": False
			}
		  },
		  "layerBlending": "normal",
		  "splitMaps": [],
		  "animationConfig": {
			"currentTime": None,
			"speed": 1
		  }
		},
		"mapState": {
		  "bearing": 0,
		  "dragRotate": False,
		  "latitude": 46.82432869985292,
		  "longitude": 0.6931065883195648,
		  "pitch": 0,
		  "zoom": 5.279017859889528,
		  "isSplit": False,
			"mapboxApiAccessToken": "pk.eyJ1Ijoid2lsbGJheWUiLCJhIjoiY2xwMDdjcGMxMDV4NTJscW80YTVudnQ0eCJ9.QyXlCq0H6T4YkSLbI0seDw"

		},
		"mapStyle": {
		  "styleType": "dark",
		  "topLayerGroups": {},
		  "visibleLayerGroups": {
			"label": True,
			"road": True,
			"border": False,
			"building": True,
			"water": True,
			"land": True,
			"3d building": False,
			"mapboxApiAccessToken" : "pk.eyJ1Ijoid2lsbGJheWUiLCJhIjoiY2xwMDdjcGMxMDV4NTJscW80YTVudnQ0eCJ9.QyXlCq0H6T4YkSLbI0seDw"
		  },
		  "threeDBuildingColor": [
			9.665468314072013,
			17.18305478057247,
			31.1442867897876
		  ],
		  "mapStyles": {},

		}
	  }
	}

#_mapconfig['config']['visState']['interactionConfig']['tooltip']['fieldsToShow']['data_1'][0]['name'] = _mapconfig['config']['visState']['interactionConfig']['tooltip']['fieldsToShow']['data_1'][0]['name'].str.decode('latin1').encode('utf-8')
#map_1 = KeplerGl(height=500, data={"data_1": dummy_coords}, config = _mapconfig)
#map_1 = KeplerGl(height=500, data={"data_1": dummy_coords})

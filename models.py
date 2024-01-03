# -*- coding: latin-1 -*-
import numpy as nd
import pandas as pd
import os
from keplergl import KeplerGl

current_directory = os.getcwd()
path_abstentions = f'{current_directory}/processed/abstentions.csv'
path_paris = f'{current_directory}/processed/csv_files/geo_paris.csv'

class Integrate_cities:
	def __int__(self):
		pass
	def add_paris(self):
		pass


def create_denomination_complete(df):
	df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ') '
	return df


def add_paris(df):
	paris_with_coords = pd.read_csv(path_paris)
	keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département',
					'Libellé de la commune', '% Abs/Ins', 'Inscrits', 'Abstentions', 'geo_adresse']
	paris_keep_columns = paris_with_coords[keep_columns]
	renamed_cols = {'geo_adresse': 'Adresse complète'}
	paris_keep_columns.rename(columns=renamed_cols, inplace=True)
	paris_keep_columns['Code du département'] = paris_keep_columns['Code du département'].apply(lambda x: str(x))
	paris_keep_columns = create_denomination_complete(paris_keep_columns)
	df = df.append(paris_keep_columns)
	return df


def ammend_jura_ain(df):
	df['code_postal'] = nd.where((df['Libellé du département'] == 'Jura') & (df['Libellé de la commune'] == 'Chancia'),
								 '39102', df['code_postal'])
	df['code_postal'] = nd.where(
		(df['Libellé du département'] == 'Jura') & (df['Libellé de la commune'] == 'Lavancia-Epercy'), '39283',
		df['code_postal'])
	return df


def prepare_df(path):
	df = pd.read_csv(path)

	df = df.dropna()
	renamed_cols = {'ville': 'Libellé de la commune', 'abs_ins': '% Abs/Ins', 'abstentions': 'Abstentions',
					'inscrits': 'Inscrits', 'libelle_du_departement': 'Libellé du département'}
	df.rename(columns=renamed_cols, inplace=True)
	df = ammend_jura_ain(df)
	df['Code du département'] = df['code_postal'].apply(lambda x: str(x)[:2])

	df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ') '
	df['Adresse complète'] = df['adresse'].map(str) + ' ' + df['code_postal'].map(str)
	keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département','dénomination complète',
					'Libellé de la commune', '% Abs/Ins', 'Inscrits',
					'Abstentions', 'Adresse complète']
	df = df[keep_columns]
	df_with_paris = add_paris(df)
	df_with_paris = df_with_paris.sort_values(by='Code du département')

	return df_with_paris

def francemetropole(path):
	df = prepare_df(path)
	#df = add_paris(df)
	keep_columns = ['longitude','latitude','Libellé de la commune','% Abs/Ins', 'Inscrits', 'Abstentions', 'Libellé du département', 'Adresse complète']
	df_keep_columns = df[keep_columns]
	res = KeplerGl(height=500, data={"data_1": df_keep_columns}, config=_mapconfig)
	return res


def liste_communes(departements):
	#create dictionary with all communes for entered departements
	resu = {}
	df = prepare_df(path_abstentions)
	#df = add_paris(df)

	for i in departements:
		dep = i.split(' ')
		communes = list(df[df['Libellé du département']==dep[0]]['Libellé de la commune'].unique() )
		communes = [i + ' '+ dep[1] for i in communes]
		communes.insert(0, "Département entier {}".format(dep[1]))
		resu[i] = communes

	return resu

def all_departements():
	df = prepare_df(path_abstentions)
	#df = add_paris(df)
	res = list(df['dénomination complète'].unique())
	return res


def communes_for_map_a(communes_liste):
	df = prepare_df(path_abstentions)
	list_dep_entier = list()
	#df = add_paris(df)
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
	print("VOILA deps_communes   {}".format(deps_communes))
	df_choix = pd.DataFrame(data=deps_communes, columns=['Libellé de la commune', 'Code du département'])
	print("VOILA DF_CHOIX   {}".format(df_choix))
	filtered_df = pd.merge(df, df_choix, left_on=['Code du département', 'Libellé de la commune'],
						   right_on=['Code du département', 'Libellé de la commune'])
	if len(list_dep_entier) > 0:
		#filtered_df
		for df_departement in list_dep_entier:
			filtered_df = filtered_df.append(df_departement)

	res = KeplerGl(height=500, data={"data_1": filtered_df}, config=_mapconfig)
	return res


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
					"name": "Libellé du département",
					"format": None
				  },
				  {
					"name": "Libellé de la commune",
					"format": None
				  },
				  {
					"name": "Adresse complète",
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

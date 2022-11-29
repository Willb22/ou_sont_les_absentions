# -*- coding: latin-1 -*-
import numpy as nd
import pandas as pd
#import geopandas as gpd
from keplergl import KeplerGl

def ammend_jura_ain(df):
    df['code_postal'] = nd.where((df['Libellé du département']=='Jura') & (df['Libellé de la commune']=='Chancia'), '39102', df['code_postal'] )
    df['code_postal']= nd.where((df['Libellé du département']=='Jura') & (df['Libellé de la commune']=='Lavancia-Epercy'), '39283', df['code_postal'] )
    return df

def prepare_df(path):
	df = pd.read_csv('./processed/abstentions.csv')
	df = df.dropna()
	renamed_cols = {'ville': 'Libellé de la commune', 'abs_ins': '% Abs/Ins', 'abstentions': 'Abstentions',
					'inscrits': 'Inscrits', 'libelle_du_departement': 'Libellé du département'}
	df.rename(columns=renamed_cols, inplace=True)
	df = ammend_jura_ain(df)
	df['Code du département'] = df['code_postal'].apply(lambda x: str(x)[:2])
	df = df.sort_values(by='Code du département')
	df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ') '
	df['Adresse complète'] = df['adresse'].map(str) + ' ' + df['code_postal'].map(str)

	return df

def liste_communes(departements):
	#create dictionary with all communes for entered departements
	resu = {}
	path = './processed/abstentions.csv'
	df = prepare_df(path)

	for i in departements:
		dep = i.split(' ')
		communes = list(df[df['Libellé du département']==dep[0]]['Libellé de la commune'].unique() )
		communes = [i + ' '+ dep[1] for i in communes]
		resu[i] = communes

	return resu

def all_departements():
	path = './processed/abstentions.csv'
	df = prepare_df(path)
	res = list(df['dénomination complète'].unique())
	return res

def communes_for_map( communes_liste):

	path = './processed/abstentions.csv'
	df = prepare_df(path)
	
	deps_communes = list()
	for i in communes_liste:
		new = i.split(' ')
		new[1] = new[1].strip('()')
		deps_communes.append(new)

	deps_communes = nd.array(deps_communes)
	df_choix = pd.DataFrame(data = deps_communes, columns=['Libellé de la commune', 'Code du département'])
	filtered_df = pd.merge(df, df_choix, left_on = ['Code du département', 'Libellé de la commune'], right_on = ['Code du département', 'Libellé de la commune'])
	res = KeplerGl(height=500, data={"data_1": filtered_df}, config = _mapconfig)
	return res


def communes_for_map_a(communes_liste):
	path = './processed/abstentions.csv'
	df = prepare_df(path)
	deps_communes = list()
	for i in communes_liste:
		new = i.split('(')
		if len(new) > 2:
			return 'Problem in commune name'
		new[0] = new[0].strip(' ')
		new[-1] = new[-1].strip('( )')
		deps_communes.append(new)

	deps_communes = nd.array(deps_communes)
	print("VOILA deps_communes   {}".format(deps_communes))
	df_choix = pd.DataFrame(data=deps_communes, columns=['Libellé de la commune', 'Code du département'])
	print("VOILA DF_CHOIX   {}".format(df_choix))
	filtered_df = pd.merge(df, df_choix, left_on=['Code du département', 'Libellé de la commune'],
						   right_on=['Code du département', 'Libellé de la commune'])
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
		  "isSplit": False
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
			"3d building": False
		  },
		  "threeDBuildingColor": [
			9.665468314072013,
			17.18305478057247,
			31.1442867897876
		  ],
		  "mapStyles": {}
		}
	  }
	}

#_mapconfig['config']['visState']['interactionConfig']['tooltip']['fieldsToShow']['data_1'][0]['name'] = _mapconfig['config']['visState']['interactionConfig']['tooltip']['fieldsToShow']['data_1'][0]['name'].str.decode('latin1').encode('utf-8')
#map_1 = KeplerGl(height=500, data={"data_1": dummy_coords}, config = _mapconfig)
#map_1 = KeplerGl(height=500, data={"data_1": dummy_coords})

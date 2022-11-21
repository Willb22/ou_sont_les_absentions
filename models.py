# -*- coding: latin-1 -*-
import numpy as nd
import pandas as pd
import geopandas as gpd
from keplergl import KeplerGl


#df = pd.read_csv("./raw/PR17_BVot_T1_FE (copy).txt", encoding = "ISO-8859-1", sep =';')
dummy_coords = pd.read_csv('./processed/csv_files/dummycoord_bur.csv')
geo = pd.read_csv('./raw/geo_bureaux_de_vote.csv')
#print(geo.shape)

def liste_communes(departements):
	#create dictionary with all communes for entered departements
	resu = {}
	df = pd.read_csv('./processed/csv_files/dummycoord_bur.csv')
	for i in departements:
		dep = i.split(' ')
		communes = list(df[df['Libell� du d�partement']==dep[0]]['Libell� de la commune'].unique() )
		communes = [i + ' '+ dep[1] for i in communes]
		resu[i] = communes
		
	
	#return list(dummy_coords[dummy_coords['Libell� du d�partement']==un_departement[:-5])
	return resu

def all_departements():
	df = pd.read_csv('./processed/csv_files/dummycoord_bur.csv')
	df['Code du d�partement'] = df['Code du d�partement'].apply(lambda x : str(x))
	together =  pd.DataFrame({ 'Libell� du d�partement' : pd.Series(df['Libell� du d�partement'].unique()) ,  'Code du d�partement' : pd.Series(df['Code du d�partement'].unique() )})
	together['d�nomination compl�te'] =  together['Libell� du d�partement'] + ' (' + together['Code du d�partement'] + ') ' 

	res = list(together['d�nomination compl�te'].unique() )
	
	return res

def communes_for_map( communes_liste):
	df = pd.read_csv('./processed/csv_files/dummycoord_bur.csv')
	df['Code du d�partement'] = df['Code du d�partement'].apply(lambda x : str(x))
	
	deps_communes = list()
	for i in communes_liste:
		new = i.split(' ')
		new[1] = new[1].strip('()')
		deps_communes.append(new)

	deps_communes = nd.array(deps_communes)
	df_choix = pd.DataFrame(data = deps_communes, columns=['Libell� de la commune', 'Code du d�partement'])
	filtered_df = pd.merge(df, df_choix, left_on = ['Code du d�partement', 'Libell� de la commune'], right_on = ['Code du d�partement', 'Libell� de la commune'])
	res = KeplerGl(height=500, data={"data_1": filtered_df}, config = _mapconfig)
	return res


def communes_for_map_a(communes_liste):
	df = pd.read_csv('./processed/abstentions.csv')
	df = df.dropna()
	df['Code du d�partement'] = df['code_postal'].apply(lambda x: str(x)[:2])
	df['Adresse compl�te'] = df['adresse'].map(str) + ' ' + df['code_postal'].map(str)
	df.rename(columns = {'ville':'Libell� de la commune', 'abs_ins' : '% Abs/Ins', 'abstentions':'Abstentions','inscrits' : 'Inscrits'}, inplace = True)

	deps_communes = list()
	for i in communes_liste:
		new = i.split(' ')
		new[1] = new[1].strip('()')
		deps_communes.append(new)

	deps_communes = nd.array(deps_communes)
	df_choix = pd.DataFrame(data=deps_communes, columns=['Libell� de la commune', 'Code du d�partement'])
	print("VOILA DF_CHOIX   {}".format(df_choix))
	filtered_df = pd.merge(df, df_choix, left_on=['Code du d�partement', 'Libell� de la commune'],
						   right_on=['Code du d�partement', 'Libell� de la commune'])
	res = KeplerGl(height=500, data={"data_1": filtered_df}, config=_mapconfig)
	return res


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
					"colors": [
					  "#5A1846",
					  "#900C3F",
					  "#C70039",
					  "#E3611C",
					  "#F1920E",
					  "#FFC300"
					]
				  },
				  "strokeColorRange": {
					"name": "Global Warming",
					"type": "sequential",
					"category": "Uber",
					"colors": [
					  "#5A1846",
					  "#900C3F",
					  "#C70039",
					  "#E3611C",
					  "#F1920E",
					  "#FFC300"
					]
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
				  "name": "Abstentions",
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
map_1 = KeplerGl(height=500, data={"data_1": dummy_coords}, config = _mapconfig)
#map_1 = KeplerGl(height=500, data={"data_1": dummy_coords})

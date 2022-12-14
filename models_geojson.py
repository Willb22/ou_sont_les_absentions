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
		communes = list(df[df['Libellé du département']==dep[0]]['Libellé de la commune'].unique() )
		communes = [i + ' '+ dep[1] for i in communes]
		resu[i] = communes
		
	
	#return list(dummy_coords[dummy_coords['Libellé du département']==un_departement[:-5])
	return resu

def all_departements():
	df = pd.read_csv('./processed/csv_files/dummycoord_bur.csv')
	df['Code du département'] = df['Code du département'].apply(lambda x : str(x))
	together =  pd.DataFrame({ 'Libellé du département' : pd.Series(df['Libellé du département'].unique()) ,  'Code du département' : pd.Series(df['Code du département'].unique() )})
	together['dénomination complète'] =  together['Libellé du département'] + ' (' + together['Code du département'] + ') ' 

	res = list(together['dénomination complète'].unique() )
	
	return res

def communes_for_map( communes_liste):
	print('HERE IS commune_liste {}'.format(communes_liste))
	#df = pd.read_csv('./processed/csv_files/dummycoord_bur.csv')
	df = gpd.read_file('./processed/abstentions.geojson')
	df['Code du département'] = df['code_postal'].apply(lambda x : str(x)[:2])
	
	deps_communes = list()
	for i in communes_liste:
		new = i.split(' ')
		new[1] = new[1].strip('()')
		deps_communes.append(new)

	deps_communes = nd.array(deps_communes)
	df_choix = pd.DataFrame(data = deps_communes, columns=['ville', 'Code du département'])
	filtered_df = pd.merge(df, df_choix, left_on = ['Code du département', 'ville'], right_on = ['Code du département', 'ville'])
	res = KeplerGl(height=500, data={"data_1": filtered_df}, config = _mapconfig)
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
				  "name": "abstentions",
				  "type": "integer"
				},
				"colorScale": "quantile",
				"strokeColorField": None,
				"strokeColorScale": "quantile",
				"sizeField": {
				  "name": "abstentions",
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
					"name": "abs_ins",
					"format": None
				  },
				  {
					"name": "abstentions",
					"format": None
				  },
				  {
					"name": "inscrits",
					"format": None
				  },
				  {
					"name": "ville",
					"format": None
				  },
				  {
					"name": "adresse",
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

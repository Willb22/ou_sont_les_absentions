import yaml
import logging
from datetime import datetime
import os

now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

log_filename = f'logs/app_{now}.log'
os.makedirs(os.path.dirname(log_filename), exist_ok=True)
logging.basicConfig(level=logging.DEBUG,
                    filename=log_filename ,
                    filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
with open('dev_config.yaml', 'r') as file:
		configurations = yaml.safe_load(file)

print(configurations)


colorscheme = [
					  "#5A1846",
					  "#900C3F",
					  "#C70039",
					  "#E3611C",
					  "#F1920E",
					  "#FFC300"
					]

#colorscheme = ['FFFF00', 'FFCC00', 'FF9900', 'FF6600', 'FF3300', 'FF0000']
colorscheme = ['#FFFF00', '#FFCC00', '#FF9900', '#FF6600', '#FF3300', '#FF0000']
#colorscheme = ['#f2e600', '#e6cc00', '#d9b300', '#cc9900', '#c08000', '#b36600', '#a64d00', '#993300', '#8d1a00', '#800000']
#colorscheme = ['#FFE6E6', '#FFCCCC', '#FFFB2B2', '#FF9999', '#FF8080', '#FF1D1D', '#FF1919','CC0000', '#990000', '#660000']
#colorscheme = ['#ffe6e6', '#ffcccc', '#ff9999', '#ff4d4d', '#ff1a1a', '#cc0000','#CC0000', '#800000', '#330000']

#dbmapconfig["config"]["visState"]["interactionConfig"]["tooltip"]["fieldsToShow"]["data_1"][0]["name"] = "Pourcentage Absentions"
dbmapconfig = {
	  "version": "v1",
	  "config": {
		"visState": {
		  "filters": [],
		  "layers": [
			{
			  "id": "c9y8sgp",
			  "type": "point",
			  "config": {
				"dataId": "data_1",
				"label": "Point",
				"color":[255,203,153],
			    "highlightColor":[252,242,26,255],
				"columns": {
				  "lat": "latitude",
				  "lng": "longitude",
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
					"alignment": "center", "outlineWidth":0,"outlineColor":[255,0,0,255],"background":False,"backgroundColor":[0,0,200,255]
				  }
				]
			  },
			  "visualChannels": {
				"colorField": {
				  "name": "Pourcentage Absentions",
				  "type": "float"
				},
				"colorScale": "quantile",
				"strokeColorField": "Pourcentage Absentions",
				"strokeColorScale": "quantile",
				"sizeField": {
				  "name": "Inscrits",
				  "type": "integer"
				},
				"sizeScale": "linear"
			  }
			}
		  ],
		  "interactionConfig": {
			"tooltip": {
			  "fieldsToShow": {
				"data_1": [

				  {
					"name": "Pourcentage Absentions",
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
		  "backgroundColor":[0,0,0],
		  "mapStyles": {},

		}
	  }
	}
#/usr/bin/python3
from flask import Flask, request, jsonify, render_template
from models import  pd, nd, map_1, liste_communes, all_departements, communes_for_map
import json
app = Flask(__name__, template_folder= "./processed/html_files/")
#app = Flask(__name__)


@app.route('/', methods = ['POST'])
def onef():
	
	
	
    return "<h1>Say hi !!</h1>"

@app.route('/', methods = ['GET'])
def trial():
	#res = df['Libellé du département'][0]
	
	
	#res = request.form
	#res = request.args
	#line = int(request.args.get('commune') )
	commune = request.args.get('commune') 
	_filter = df[df['Libellé de la commune']== commune]
	#res = df['Libellé de la circonscription'][line]
	#res = render_template('geo33nv.html')
	#res = str(_filter['latitude'].mean())
	res = render_template('choix_departements.html')
	return res
	
@app.route('/formulaire', methods = ['GET'])
def choix_departements():
	_alldepartements = all_departements()

	res = render_template('formulaire_pers.html', liste = _alldepartements)
	return res


@app.route('/departements', methods = ['GET'])
def create_form_communes():
	deps = request.args.getlist('choix_des_departements[]')
	deps_communes = liste_communes(deps)
	res = render_template('choi_communes.html', name = deps_communes)
	return res
	

@app.route('/communes', methods = ['GET'])
def afunc():
	res = render_template('geo33nv.html')
	return res


@app.route('/generatemap', methods = ['GET'])
def test_map():
    #return map_1._repr_html_()
	deps = request.args.getlist('choix_des_communes[]')
	#res = render_template('choix_communes.html', name = deps)
	map_to_go = communes_for_map(deps)
	return map_to_go._repr_html_()
    
    
    
    
if __name__ == '__main__':
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)

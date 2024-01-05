#/usr/bin/python3
from flask import Flask, request, jsonify, render_template
from models import  pd, nd, liste_communes, all_departements, communes_for_map_a, francemetropole, path_abstentions
#from models_geojson import  pd, nd, map_1, liste_communes, all_departements, communes_for_map
import json
import git
app = Flask(__name__, template_folder= "./processed/html_files/")

@app.route('/update_server', methods=['POST'])
def webhook():
    if request.method == 'POST':
        repo = git.Repo('/home/ousontlesabstentions/mysite')
        origin = repo.remotes.origin
        origin.pull()
        return 'Updated PythonAnywhere successfully', 200
    else:
        return 'Wrong event type', 400

@app.route('/', methods = ['GET'])
def trial():

	return render_template('index.html')


@app.route('/france2017', methods=['GET'])
def whyname():
	return render_template('./france_2017/menu.html')

@app.route('/france2017/paris75', methods=['GET'])
def whyname1():
	return render_template('./france_2017/paris.html')

@app.route('/france2017/francemetropole', methods=['GET'])
def whyname2():

	map_to_go = francemetropole(path_abstentions)
	return map_to_go._repr_html_()

@app.route('/france2017/choix_departements', methods = ['GET'])
def choix_departements():
	_alldepartements = all_departements()

	res = render_template('file_choix_departements.html', liste = _alldepartements)
	return res


@app.route('/choix_communes', methods = ['GET'])
def create_form_communes():
	deps = request.args.getlist('choix_des_departements[]')
	deps_communes = liste_communes(deps)
	res = render_template('file_choix_communes.html', name = deps_communes)
	return res

@app.route('/generatemap', methods = ['GET'])
def test_map():
	deps = request.args.getlist('choix_des_communes[]')
	#res = render_template('choix_communes.html', name = deps)
	map_to_go = communes_for_map_a(deps)
	return map_to_go._repr_html_()
    
    
    
    
if __name__ == '__main__':
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)

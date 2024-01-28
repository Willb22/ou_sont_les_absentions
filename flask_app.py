#/usr/bin/python3
from flask import Flask, request, jsonify, render_template
#from models import  pd, nd, liste_communes, all_departements, communes_for_map_a, francemetropole, path_abstentions
from models import process_france2017, process_france2022
#import os
import git

from resource import getrusage, RUSAGE_SELF
print("Peak memory (MiB):", int(getrusage(RUSAGE_SELF).ru_maxrss / 1024))

app = Flask(__name__, template_folder= "./processed/html_files/")



# current_directory = os.path.dirname(__file__)
# path_abstentions_france2017 = f'{current_directory}/processed/csv_files/france_2017/abstentions.csv'
# path_paris_france2017 = f'{current_directory}/processed/csv_files/france_2017/geo_paris.csv'
# path_abstentions_france2022 = f'{current_directory}/processed/csv_files/france_2022/abstentions.csv'


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
def whyname2017():
	return render_template('./france_2017/menu.html')

@app.route('/france2017/paris75', methods=['GET'])
def whyname12017():
	return render_template('./france_2017/paris.html')

@app.route('/france2017/francemetropole', methods=['GET'])
def whyname22017():

	#map_to_go = process_france2017.francemetropole()
	map_to_go = process_france2017.query_francemetropole()
	return map_to_go._repr_html_()

@app.route('/france2017/choix_departements', methods = ['GET'])
def choix_departements2017():
	#alldepartements = process_france2017.all_departements()
	alldepartements = process_france2017.query_all_departements()

	res = render_template('france_2017/file_choix_departements.html', liste = alldepartements)
	return res


@app.route('/france2017/choix_communes', methods = ['GET'])
def create_form_communes2017():
	deps = request.args.getlist('choix_des_departements[]')
	#deps_communes = process_france2017.liste_communes(deps)
	deps_communes = process_france2017.query_liste_communes(deps)
	res = render_template('france_2017/file_choix_communes.html', name = deps_communes)
	return res

@app.route('/france2017/generatemap', methods = ['GET'])
def test_map2017():
	deps = request.args.getlist('choix_des_communes[]')
	#res = render_template('choix_communes.html', name = deps)
	#map_to_go = process_france2017.communes_for_map(deps)
	map_to_go = process_france2017.generate_kepler_map(deps)

	return map_to_go._repr_html_()




@app.route('/france2022', methods=['GET'])
def whyname():
	return render_template('./france_2022/menu.html')

@app.route('/france2022/paris75', methods=['GET'])
def whyname1():
	return render_template('./france_2022/paris.html')

@app.route('/france2022/francemetropole', methods=['GET'])
def whyname2():

	map_to_go = process_france2022.francemetropole()
	return map_to_go._repr_html_()

@app.route('/france2022/choix_departements', methods = ['GET'])
def choix_departements():
	alldepartements = process_france2022.all_departements()

	res = render_template('france_2022/file_choix_departements.html', liste = alldepartements)
	return res


@app.route('/france2022/choix_communes', methods = ['GET'])
def create_form_communes():
	deps = request.args.getlist('choix_des_departements[]')
	deps_communes = process_france2022.liste_communes(deps)
	res = render_template('france_2022/file_choix_communes.html', name = deps_communes)
	return res

@app.route('/france2022/generatemap', methods = ['GET'])
def test_map():
	deps = request.args.getlist('choix_des_communes[]')
	#res = render_template('choix_communes.html', name = deps)
	map_to_go = process_france2022.communes_for_map(deps)

	return map_to_go._repr_html_()
    
    
    
    
if __name__ == '__main__':
    # Threaded option to enable multiple instances for multiple user access support
    #app.run(threaded=True, ssl_context=('cert.pem', 'key.pem'), port=5000) # attempt https
	app.run(threaded=True, host='0.0.0.0', port=5000)


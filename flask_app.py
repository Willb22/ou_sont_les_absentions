#/usr/bin/python3
from flask import Flask, request, render_template
from models import process_france2017, now
from db_connections import log_memory_after
#from models import process_france2022
import git
import gevent.pywsgi
import os
from resource import getrusage, RUSAGE_SELF
from models import logging


app = Flask(__name__, template_folder= "./processed/html_files/")
production_wsgi_server = False



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
	if process_france2017.static_francemetropole:
		html_map = render_template('france_2017/francemetropole.html')
		#log_process_memory('load france metropole 2017')
		logging.info(log_memory_after('load france metropole 2017'))
	else:
		map_to_go = process_france2017.query_francemetropole()
		html_map=map_to_go._repr_html_(center_map=True)
		#map_to_go.save_to_html(file_name='./processed/html_files/france_2017/francemetropole.html')
		logging.info(log_memory_after('load france metropole 2017'))
	return html_map

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
	#map_to_go.save_to_html(file_name='./processed/html_files/france_2017/paris.html', center_map=True)
	return map_to_go._repr_html_(center_map=True)




@app.route('/france2022', methods=['GET'])
def whyname():
	res = render_template('./france_2022/menu.html')
	res = render_template('./france_2022/coming_soon.html')
	return res

@app.route('/france2022/paris75', methods=['GET'])
def whyname1():
	res = render_template('./france_2022/paris.html')
	res = render_template('./france_2022/coming_soon.html')
	return res

@app.route('/france2022/francemetropole', methods=['GET'])
def whyname2():

	map_to_go = process_france2022.francemetropole()
	res = map_to_go._repr_html_()
	res = render_template('./france_2022/coming_soon.html')
	return res

@app.route('/france2022/choix_departements', methods = ['GET'])
def choix_departements():
	alldepartements = process_france2022.all_departements()

	res = render_template('france_2022/file_choix_departements.html', liste = alldepartements)
	res = render_template('./france_2022/coming_soon.html')
	return res


@app.route('/france2022/choix_communes', methods = ['GET'])
def create_form_communes():
	deps = request.args.getlist('choix_des_departements[]')
	deps_communes = process_france2022.liste_communes(deps)
	res = render_template('france_2022/file_choix_communes.html', name = deps_communes)
	res = render_template('./france_2022/coming_soon.html')
	return res

@app.route('/france2022/generatemap', methods = ['GET'])
def test_map():
	deps = request.args.getlist('choix_des_communes[]')
	map_to_go = process_france2022.communes_for_map(deps)
	res = map_to_go._repr_html_()
	res = render_template('./france_2022/coming_soon.html')

	return res
    
    
    
    
if __name__ == '__main__':
    # Threaded option to enable multiple instances for multiple user access support
    #app.run(threaded=True, ssl_context=('cert.pem', 'key.pem'), port=5000) # attempt https
	#app.run(threaded=True, host='0.0.0.0', port=5000)
	#app_server = gevent.pywsgi.WSGIServer(('0.0.0.0', 5000), app)
	if production_wsgi_server:
		app_server = gevent.pywsgi.WSGIServer(('0.0.0.0', 443), app, keyfile='key.pem', certfile='cert.pem')
		app_server.serve_forever()
	else:
		app.run(threaded=True, host='0.0.0.0', port=5000, debug=True)


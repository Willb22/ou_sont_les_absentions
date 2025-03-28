#/usr/bin/python3
import traceback
from flask import Flask, request, render_template
from web_app.models import query_france2017, query_france2022
from db_connections import log_memory_after
import git
import gevent.pywsgi
from config import configurations, logging, now


app = Flask(__name__, template_folder="./html_files/")
production_wsgi_server = configurations['web_deployment']['production_wsgi_server']
port = configurations['web_deployment']['port']


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
	if query_france2017.static_francemetropole:
		html_map = render_template('france_2017/francemetropole.html')
		logging.info(log_memory_after('load france metropole 2017'))
	else:
		map_to_go = query_france2017.query_francemetropole()
		html_map=map_to_go._repr_html_(center_map=True)
		#map_to_go.save_to_html(file_name='./processed/html_files/france_2017/francemetropole.html')
		logging.info(log_memory_after('load france metropole 2017'))
	return html_map

@app.route('/france2017/choix_departements', methods = ['GET'])
def choix_departements2017():
	#alldepartements = query_france2017.all_departements()
	alldepartements = query_france2017.query_all_departements()
	res = render_template('france_2017/file_choix_departements.html', liste = alldepartements)
	return res


@app.route('/france2017/choix_communes', methods = ['GET'])
def create_form_communes2017():
	deps = request.args.getlist('choix_des_departements[]')
	deps_communes = query_france2017.query_liste_communes(deps)
	res = render_template('france_2017/file_choix_communes.html', name=deps_communes)
	return res

@app.route('/france2017/generatemap', methods = ['GET'])
def test_map2017():
	zones = request.args.getlist('choix_des_communes[]')
	#res = render_template('choix_communes.html', name = deps)
	map_to_go = query_france2017.generate_kepler_map(zones)
	#map_to_go.save_to_html(file_name='./processed/html_files/france_2017/paris.html', center_map=True)
	return map_to_go._repr_html_(center_map=True)




@app.route('/france2022', methods=['GET'])
def whyname():
	res = render_template('./france_2022/menu.html')
	#res = render_template('./france_2022/coming_soon.html')
	return res

@app.route('/france2022/paris75', methods=['GET'])
def whyname1():
	#res = render_template('./france_2022/dummy.html')
	res = render_template('./france_2022/coming_soon.html')
	return res

@app.route('/france2022/francemetropole', methods=['GET'])
def whyname2():
	if query_france2022.static_francemetropole:
		#html_map = render_template('france_2022/francemetropole.html')
		html_map = render_template('./france_2022/coming_soon.html')
		logging.info(log_memory_after('load france metropole 2022'))
	else:
		map_to_go = query_france2022.query_francemetropole()
		html_map=map_to_go._repr_html_(center_map=True)
		#map_to_go.save_to_html(file_name='./processed/html_files/france_2017/francemetropole.html')
		logging.info(log_memory_after('load france metropole 2022'))
	return html_map

@app.route('/france2022/choix_departements', methods = ['GET'])
def choix_departements():
	alldepartements = query_france2022.query_all_departements()
	logging.info(f'All queried departements are {alldepartements}')

	res = render_template('france_2022/file_choix_departements.html', liste=alldepartements)
	#res = render_template('./france_2022/coming_soon.html')
	return res


@app.route('/france2022/choix_communes', methods = ['GET'])
def create_form_communes():
	deps = request.args.getlist('choix_des_departements[]')
	deps_communes = query_france2022.query_liste_communes(deps)
	res = render_template('france_2022/file_choix_communes.html', name=deps_communes)
	#res = render_template('./france_2022/coming_soon.html')
	return res

@app.route('/france2022/generatemap', methods = ['GET'])
def test_map():
	zones = request.args.getlist('choix_des_communes[]')
	map_to_go = query_france2022.generate_kepler_map(zones)
	res = map_to_go._repr_html_(center_map=True)
	#res = render_template('./france_2022/coming_soon.html')

	return res
    
    
    
    
def run_flask_app():
    # Threaded option to enable multiple instances for multiple user access support
    #app.run(threaded=True, ssl_context=('cert.pem', 'key.pem'), port=5000) # attempt https
	#app.run(threaded=True, host='0.0.0.0', port=5000)
	#app_server = gevent.pywsgi.WSGIServer(('0.0.0.0', 5000), app)
	try:
		if production_wsgi_server:
			app_server = gevent.pywsgi.WSGIServer(('0.0.0.0', port), app, keyfile='key.pem', certfile='cert.pem')
			app_server.serve_forever()
		else:
			app.run(threaded=True, host='0.0.0.0', port=port, debug=True)
	except Exception as e:
		traceback.format_exc()
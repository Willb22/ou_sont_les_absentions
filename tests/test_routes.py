# tests/conftest.py
import pytest
from web_app.flask_app import app as flask_app  # flask_app.py contains app = Flask(...)

@pytest.fixture(scope="function")
def client():
    with flask_app.test_client() as client:
        yield client

routes = ["/","/france2022","/france2022/paris75", "/france2022/francemetropole", "/france2022/choix_departements", "/france2022/choix_communes", "/france2022/generatemap",
          "/france2017","/france2017/paris75", "/france2017/francemetropole", "/france2017/choix_departements", "/france2017/choix_communes", "/france2017/generatemap"]

@pytest.mark.parametrize("url", routes)
def test_routes_status_code(client, url):
    try:
        response = client.get(url)
        print(f'for ROUTE {url} RESPONSE IS {response}')
        assert response.status_code == 200, f'Failed for URL: {url}'
    except Exception as e:
        print(f'for ROUTE {url}')
        pytest.fail(f"Failed to load page: {e}")

url_choix_departements = ["/france2017/choix_departements", "/france2022/choix_departements"]
@pytest.mark.parametrize("url", url_choix_departements)
def test_routes_choix_departements(client, url):
    try:
        response = client.get(url)
        #print(f'for ROUTE {url} RESPONSE IS {response}')
        html = response.get_data(as_text=True)
        #print(f'for ROUTE {url} HTML is {html}')
        assert "Choisissez un ou plusieurs départements" in html , f'Failed for URL: {url}'
        assert "Seine-Saint-Denis (93)" in html , f'Failed for URL: {url}'
        assert "Paris (75)" in html , f'Failed for URL: {url}'
        #assert b"Aucun departement de choisi" in response.data , f'Failed for URL: {url}'
    except Exception as e:
        print(f'for ROUTE {url}')
        pytest.fail(f"Failed to load page: {e}")

user_input_choix_communes = {"choix_des_departements[]": ["Paris (75)", "Seine-Saint-Denis (93)"]}
url_choix_communes = ["/france2017/choix_communes", "/france2022/choix_communes"]
query_details=[ (url, user_input_choix_communes) for url in url_choix_communes ]
@pytest.mark.parametrize("url, query_params", query_details)
def test_routes_choix_communes(client, url, query_params):
    try:
        response = client.get(url, query_string = query_params)
        #print(f'for ROUTE {url} RESPONSE IS {response}')
        html = response.get_data(as_text=True)
        #print(f'for ROUTE {url} HTML is {html}')
        assert "choisissez une ou plusieurs communes" in html , f'Failed for URL: {url}'
        assert "Seine-Saint-Denis (93)" in html , f'Failed for URL: {url}'
        assert "Le Blanc-Mesnil (93)" in html , f'Failed for URL: {url}'
        #assert b"Aucun departement de choisi" in response.data , f'Failed for URL: {url}'
    except Exception as e:
        print(f'for ROUTE {url}')
        pytest.fail(f"Failed to load page: {e}")
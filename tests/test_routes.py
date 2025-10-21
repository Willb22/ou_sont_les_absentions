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


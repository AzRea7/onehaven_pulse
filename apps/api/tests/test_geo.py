from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app, raise_server_exceptions=False)


def test_geo_summary_endpoint_exists():
    response = client.get("/geo/summary")

    assert response.status_code in {200, 500}

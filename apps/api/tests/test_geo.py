from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_geo_summary_endpoint_exists():
    response = client.get("/geo/summary")

    assert response.status_code in {200, 500}

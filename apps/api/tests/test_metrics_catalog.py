from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_metrics_catalog_returns_supported_metrics():
    response = client.get("/metrics/catalog")

    assert response.status_code == 200

    payload = response.json()

    metric_names = {metric["name"] for metric in payload}

    assert "home_price_yoy" in metric_names
    assert "rent_yoy" in metric_names
    assert "composite_cycle_score" in metric_names
    assert "building_permits" in metric_names

    home_price = next(metric for metric in payload if metric["name"] == "home_price_yoy")

    assert home_price["unit"] == "percent"
    assert home_price["category"] == "price"
    assert home_price["is_derived"] is True
    assert home_price["is_timeseries_supported"] is True


def test_metrics_catalog_sets_cache_header():
    response = client.get("/metrics/catalog")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=3600"

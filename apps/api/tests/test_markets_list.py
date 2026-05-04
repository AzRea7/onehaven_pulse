from fastapi.testclient import TestClient
from sqlalchemy import text

from app.db.session import engine
from app.main import app


client = TestClient(app)


TEST_GEO_IDS = [
    "test_market_state_mi",
    "test_market_metro_detroit",
    "test_market_county_wayne",
]


def seed_test_markets() -> None:
    cleanup_test_markets()

    sql = text(
        """
        INSERT INTO geo.dim_geo (
            geo_id,
            geo_type,
            name,
            display_name,
            state_code,
            state_name,
            county_fips,
            cbsa_code,
            zcta,
            country_code,
            latitude,
            longitude,
            is_active,
            created_at,
            updated_at
        )
        VALUES
            (
                'test_market_state_mi',
                'state',
                'Michigan',
                'Michigan',
                'MI',
                'Michigan',
                NULL,
                NULL,
                NULL,
                'US',
                44.3148,
                -85.6024,
                true,
                now(),
                now()
            ),
            (
                'test_market_metro_detroit',
                'metro',
                'Detroit-Warren-Dearborn, MI',
                'Detroit-Warren-Dearborn, MI',
                'MI',
                'Michigan',
                NULL,
                '19820',
                NULL,
                'US',
                42.3314,
                -83.0458,
                true,
                now(),
                now()
            ),
            (
                'test_market_county_wayne',
                'county',
                'Wayne County, MI',
                'Wayne County, MI',
                'MI',
                'Michigan',
                '26163',
                NULL,
                NULL,
                'US',
                42.2791,
                -83.3362,
                true,
                now(),
                now()
            )
        """
    )

    with engine.begin() as connection:
        connection.execute(sql)


def cleanup_test_markets() -> None:
    sql = text(
        """
        DELETE FROM geo.dim_geo
        WHERE geo_id = ANY(:geo_ids)
        """
    )

    with engine.begin() as connection:
        connection.execute(sql, {"geo_ids": TEST_GEO_IDS})


def setup_module() -> None:
    seed_test_markets()


def teardown_module() -> None:
    cleanup_test_markets()


def test_get_markets_returns_paginated_response():
    response = client.get("/markets?limit=2&offset=0")

    assert response.status_code == 200

    payload = response.json()

    assert "items" in payload
    assert "limit" in payload
    assert "offset" in payload
    assert "total" in payload

    assert payload["limit"] == 2
    assert payload["offset"] == 0
    assert len(payload["items"]) <= 2
    assert payload["total"] >= 3


def test_get_markets_filters_by_geo_type():
    response = client.get("/markets?geo_type=metro&search=Detroit")

    assert response.status_code == 200

    payload = response.json()

    assert payload["total"] >= 1
    assert all(item["geo_type"] == "metro" for item in payload["items"])
    assert any(item["geo_id"] == "test_market_metro_detroit" for item in payload["items"])


def test_get_markets_filters_by_state():
    response = client.get("/markets?state=MI&search=Wayne")

    assert response.status_code == 200

    payload = response.json()

    assert payload["total"] >= 1
    assert all(item["state_code"] == "MI" for item in payload["items"])
    assert any(item["geo_id"] == "test_market_county_wayne" for item in payload["items"])


def test_get_markets_supports_search_by_name():
    response = client.get("/markets?search=Detroit")

    assert response.status_code == 200

    payload = response.json()

    assert payload["total"] >= 1
    assert any(item["geo_id"] == "test_market_metro_detroit" for item in payload["items"])


def test_get_markets_supports_search_by_cbsa_code():
    response = client.get("/markets?search=19820")

    assert response.status_code == 200

    payload = response.json()

    assert payload["total"] >= 1
    assert any(item["geo_id"] == "test_market_metro_detroit" for item in payload["items"])


def test_get_markets_rejects_invalid_geo_type():
    response = client.get("/markets?geo_type=planet")

    assert response.status_code == 422


def test_get_markets_rejects_invalid_state():
    response = client.get("/markets?state=Michigan")

    assert response.status_code == 422


def test_get_markets_rejects_invalid_limit():
    response = client.get("/markets?limit=5000")

    assert response.status_code == 422


def test_get_markets_rejects_invalid_offset():
    response = client.get("/markets?offset=-1")

    assert response.status_code == 422

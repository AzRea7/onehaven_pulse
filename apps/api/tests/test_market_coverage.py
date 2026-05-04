from fastapi.testclient import TestClient
from sqlalchemy import text

from app.db.session import engine
from app.main import app


client = TestClient(app)

TEST_GEO_ID = "test_coverage_market"


def cleanup_test_data() -> None:
    with engine.begin() as connection:
        connection.execute(
            text("DELETE FROM analytics.market_monthly_metrics WHERE geo_id = :geo_id"),
            {"geo_id": TEST_GEO_ID},
        )
        connection.execute(
            text("DELETE FROM geo.dim_geo WHERE geo_id = :geo_id"),
            {"geo_id": TEST_GEO_ID},
        )


def seed_test_data() -> None:
    cleanup_test_data()

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO geo.dim_geo (
                    geo_id,
                    geo_type,
                    name,
                    display_name,
                    state_code,
                    state_name,
                    country_code,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES (
                    :geo_id,
                    'metro',
                    'Test Coverage Market, MI',
                    'Test Coverage Market, MI',
                    'MI',
                    'Michigan',
                    'US',
                    true,
                    now(),
                    now()
                )
                """
            ),
            {"geo_id": TEST_GEO_ID},
        )

        connection.execute(
            text(
                """
                INSERT INTO analytics.market_monthly_metrics (
                    geo_id,
                    period_month,
                    building_permits,
                    zhvi_yoy,
                    zori_yoy,
                    unemployment_rate,
                    created_at,
                    updated_at
                )
                VALUES (
                    :geo_id,
                    DATE '2026-03-01',
                    100,
                    2.1,
                    3.2,
                    4.5,
                    now(),
                    now()
                )
                """
            ),
            {"geo_id": TEST_GEO_ID},
        )


def setup_module() -> None:
    seed_test_data()


def teardown_module() -> None:
    cleanup_test_data()


def test_market_coverage_returns_coverage_groups():
    response = client.get(f"/markets/{TEST_GEO_ID}/coverage")

    assert response.status_code == 200

    payload = response.json()

    assert payload["geo_id"] == TEST_GEO_ID
    assert payload["latest_data_period"] == "2026-03-01"
    assert payload["coverage"]["price"] is True
    assert payload["coverage"]["rent"] is True
    assert payload["coverage"]["labor"] is True
    assert payload["coverage"]["permits"] is True


def test_market_coverage_returns_404_for_invalid_market():
    response = client.get("/markets/not_a_real_geo_id/coverage")

    assert response.status_code == 404

    payload = response.json()

    assert payload["error"]["code"] == "market_not_found"

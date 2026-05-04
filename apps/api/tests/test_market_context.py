from fastapi.testclient import TestClient
from sqlalchemy import text

from app.db.session import engine
from app.main import app


client = TestClient(app)

TEST_GEO_ID = "test_context_market"
TEST_SPARSE_GEO_ID = "test_context_sparse_market"


def cleanup_test_data() -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM analytics.market_metric_sources
                WHERE geo_id IN (:geo_id, :sparse_geo_id)
                """
            ),
            {
                "geo_id": TEST_GEO_ID,
                "sparse_geo_id": TEST_SPARSE_GEO_ID,
            },
        )

        connection.execute(
            text(
                """
                DELETE FROM analytics.market_monthly_metrics
                WHERE geo_id IN (:geo_id, :sparse_geo_id)
                """
            ),
            {
                "geo_id": TEST_GEO_ID,
                "sparse_geo_id": TEST_SPARSE_GEO_ID,
            },
        )

        connection.execute(
            text(
                """
                DELETE FROM geo.dim_geo
                WHERE geo_id IN (:geo_id, :sparse_geo_id)
                """
            ),
            {
                "geo_id": TEST_GEO_ID,
                "sparse_geo_id": TEST_SPARSE_GEO_ID,
            },
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
                VALUES
                    (
                        :geo_id,
                        'metro',
                        'Test Context Market, MI',
                        'Test Context Market, MI',
                        'MI',
                        'Michigan',
                        'US',
                        true,
                        now(),
                        now()
                    ),
                    (
                        :sparse_geo_id,
                        'metro',
                        'Test Sparse Context Market, MI',
                        'Test Sparse Context Market, MI',
                        'MI',
                        'Michigan',
                        'US',
                        true,
                        now(),
                        now()
                    )
                """
            ),
            {
                "geo_id": TEST_GEO_ID,
                "sparse_geo_id": TEST_SPARSE_GEO_ID,
            },
        )

        connection.execute(
            text(
                """
                INSERT INTO analytics.market_monthly_metrics (
                    geo_id,
                    period_month,
                    zhvi_yoy,
                    zori_yoy,
                    active_listings_yoy,
                    months_supply,
                    median_days_on_market,
                    payment_to_income_ratio,
                    price_to_income_ratio,
                    unemployment_rate,
                    building_permits,
                    source_flags,
                    quality_flags,
                    created_at,
                    updated_at
                )
                VALUES
                    (
                        :geo_id,
                        DATE '2026-03-01',
                        2.1,
                        3.4,
                        -8.2,
                        2.8,
                        21,
                        0.29,
                        4.1,
                        4.2,
                        100,
                        '{"test": true}',
                        '{"validated": true}',
                        now(),
                        now()
                    ),
                    (
                        :sparse_geo_id,
                        DATE '2026-03-01',
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        250,
                        '{"building_permits": {"source": "test"}}',
                        '{"building_permits": {"validated": true}}',
                        now(),
                        now()
                    )
                """
            ),
            {
                "geo_id": TEST_GEO_ID,
                "sparse_geo_id": TEST_SPARSE_GEO_ID,
            },
        )


def setup_module() -> None:
    seed_test_data()


def teardown_module() -> None:
    cleanup_test_data()


def test_market_context_returns_structured_json_for_scoreable_market():
    response = client.get(f"/markets/{TEST_GEO_ID}/context")

    assert response.status_code == 200

    payload = response.json()

    assert payload["geo_id"] == TEST_GEO_ID
    assert payload["market"] == "Test Context Market, MI"
    assert payload["latest_period"] == "2026-03-01"
    assert payload["latest_data_period"] == "2026-03-01"
    assert payload["data_status"] == "latest_period_scoreable"

    assert payload["cycle_phase"] != "Insufficient Data"
    assert payload["investor_signal"] != "Insufficient Data"
    assert payload["confidence_score"] > 0

    assert payload["evidence"]["price_growth_yoy"] == 2.1
    assert payload["evidence"]["price_growth_metric"] == "zhvi_yoy"
    assert payload["evidence"]["rent_growth_yoy"] == 3.4
    assert payload["evidence"]["rent_growth_metric"] == "zori_yoy"
    assert payload["evidence"]["inventory_trend"] in ["falling", "tight", "stable"]
    assert payload["evidence"]["affordability"] in ["favorable", "neutral", "strained"]
    assert payload["evidence"]["building_permits"] == 100.0
    assert payload["evidence"]["composite_cycle_score"] is not None

    assert "score_breakdown" in payload
    assert "coverage" in payload
    assert "risks" in payload
    assert "data_quality" in payload

    assert payload["mcp"]["tool_name"] == "get_market_context"
    assert payload["mcp"]["resource_type"] == "market"
    assert payload["mcp"]["resource_id"] == TEST_GEO_ID
    assert payload["mcp"]["schema_version"] == "1.0"


def test_market_context_returns_sparse_context_without_prose():
    response = client.get(f"/markets/{TEST_SPARSE_GEO_ID}/context")

    assert response.status_code == 200

    payload = response.json()

    assert payload["geo_id"] == TEST_SPARSE_GEO_ID
    assert payload["latest_period"] is None
    assert payload["latest_data_period"] == "2026-03-01"
    assert payload["data_status"] == "no_scoreable_period"
    assert payload["cycle_phase"] == "Insufficient Data"
    assert payload["investor_signal"] == "Insufficient Data"

    assert payload["evidence"]["price_growth_yoy"] is None
    assert payload["evidence"]["rent_growth_yoy"] is None
    assert payload["evidence"]["inventory_trend"] == "unknown"
    assert payload["evidence"]["affordability"] == "unknown"
    assert payload["evidence"]["building_permits"] == 250.0
    assert payload["evidence"]["composite_cycle_score"] is None

    risk_codes = {risk["code"] for risk in payload["risks"]}

    assert "missing_scoreable_metrics" in risk_codes
    assert "missing_price_metrics" in risk_codes
    assert "missing_rent_metrics" in risk_codes


def test_market_context_returns_404_for_invalid_market():
    response = client.get("/markets/not_a_real_geo_id/context")

    assert response.status_code == 404

    payload = response.json()

    assert payload["error"]["code"] in ["http_error", "market_not_found"]

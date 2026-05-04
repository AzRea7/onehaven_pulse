from fastapi.testclient import TestClient
from sqlalchemy import text

from app.db.session import engine
from app.main import app


client = TestClient(app)

TEST_GEO_ID = "test_market_detail_metro"
TEST_NO_METRICS_GEO_ID = "test_market_detail_no_metrics"


def cleanup_test_data() -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM analytics.market_metric_sources
                WHERE geo_id IN (:geo_id, :no_metrics_geo_id)
                """
            ),
            {
                "geo_id": TEST_GEO_ID,
                "no_metrics_geo_id": TEST_NO_METRICS_GEO_ID,
            },
        )

        connection.execute(
            text(
                """
                DELETE FROM analytics.market_monthly_metrics
                WHERE geo_id IN (:geo_id, :no_metrics_geo_id)
                """
            ),
            {
                "geo_id": TEST_GEO_ID,
                "no_metrics_geo_id": TEST_NO_METRICS_GEO_ID,
            },
        )

        connection.execute(
            text(
                """
                DELETE FROM audit.source_freshness
                WHERE source = 'test_source'
                  AND dataset IN ('zhvi', 'zori')
                """
            )
        )

        connection.execute(
            text(
                """
                DELETE FROM geo.dim_geo
                WHERE geo_id IN (:geo_id, :no_metrics_geo_id)
                """
            ),
            {
                "geo_id": TEST_GEO_ID,
                "no_metrics_geo_id": TEST_NO_METRICS_GEO_ID,
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
                        :geo_id,
                        'metro',
                        'Test Detail Metro, MI',
                        'Test Detail Metro, MI',
                        'MI',
                        'Michigan',
                        NULL,
                        '99999',
                        NULL,
                        'US',
                        42.3314,
                        -83.0458,
                        true,
                        now(),
                        now()
                    ),
                    (
                        :no_metrics_geo_id,
                        'metro',
                        'Test No Metrics Metro, MI',
                        'Test No Metrics Metro, MI',
                        'MI',
                        'Michigan',
                        NULL,
                        '99998',
                        NULL,
                        'US',
                        42.0,
                        -83.0,
                        true,
                        now(),
                        now()
                    )
                """
            ),
            {
                "geo_id": TEST_GEO_ID,
                "no_metrics_geo_id": TEST_NO_METRICS_GEO_ID,
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
                    unemployment_rate,
                    source_flags,
                    quality_flags,
                    created_at,
                    updated_at
                )
                VALUES
                    (
                        :geo_id,
                        DATE '2026-02-01',
                        1.2,
                        2.0,
                        -3.0,
                        3.5,
                        24,
                        0.31,
                        4.5,
                        '{"test": true}',
                        '{"validated": true}',
                        now(),
                        now()
                    ),
                    (
                        :geo_id,
                        DATE '2026-03-01',
                        2.1,
                        3.4,
                        -8.2,
                        2.8,
                        21,
                        0.29,
                        4.2,
                        '{"test": true, "latest": true}',
                        '{"validated": true}',
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
                INSERT INTO analytics.market_metric_sources (
                    geo_id,
                    period_month,
                    metric_name,
                    source,
                    dataset,
                    source_value,
                    normalized_value,
                    source_period,
                    transformation_notes,
                    created_at
                )
                VALUES
                    (
                        :geo_id,
                        DATE '2026-03-01',
                        'zhvi_yoy',
                        'test_source',
                        'zhvi',
                        2.1,
                        2.1,
                        DATE '2026-03-01',
                        'test source metric',
                        now()
                    ),
                    (
                        :geo_id,
                        DATE '2026-03-01',
                        'zori_yoy',
                        'test_source',
                        'zori',
                        3.4,
                        3.4,
                        DATE '2026-03-01',
                        'test source metric',
                        now()
                    )
                """
            ),
            {"geo_id": TEST_GEO_ID},
        )

        connection.execute(
            text(
                """
                INSERT INTO audit.source_freshness (
                    source,
                    dataset,
                    expected_frequency,
                    freshness_threshold_days,
                    latest_source_period,
                    last_loaded_at,
                    last_status,
                    is_stale,
                    stale_reason,
                    record_count,
                    created_at,
                    updated_at
                )
                VALUES
                    (
                        'test_source',
                        'zhvi',
                        'monthly',
                        45,
                        DATE '2026-03-01',
                        now(),
                        'success',
                        false,
                        NULL,
                        1,
                        now(),
                        now()
                    ),
                    (
                        'test_source',
                        'zori',
                        'monthly',
                        45,
                        DATE '2026-03-01',
                        now(),
                        'success',
                        false,
                        NULL,
                        1,
                        now(),
                        now()
                    )
                """
            )
        )


def setup_module() -> None:
    seed_test_data()


def teardown_module() -> None:
    cleanup_test_data()


def test_get_market_detail_returns_latest_data():
    response = client.get(f"/markets/{TEST_GEO_ID}")

    assert response.status_code == 200

    payload = response.json()

    assert payload["market"]["geo_id"] == TEST_GEO_ID
    assert payload["market"]["geo_type"] == "metro"
    assert payload["latest_period"] == "2026-03-01"

    assert payload["price_growth"]["metric"] == "zhvi_yoy"
    assert payload["price_growth"]["value"] == 2.1

    assert payload["rent_growth"]["metric"] == "zori_yoy"
    assert payload["rent_growth"]["value"] == 3.4

    assert payload["inventory_condition"]["active_listings_yoy"] == -8.2
    assert payload["inventory_condition"]["months_supply"] == 2.8
    assert payload["inventory_condition"]["condition"] in [
        "tightening",
        "tight",
        "balanced",
        "loosening",
        "loose",
        "unknown",
    ]


def test_get_market_detail_includes_score_components():
    response = client.get(f"/markets/{TEST_GEO_ID}")

    assert response.status_code == 200

    payload = response.json()
    score_breakdown = payload["score_breakdown"]

    assert payload["cycle_phase"] in [
        "Expansion",
        "Peak",
        "Correction",
        "Recovery",
        "Stabilizing",
        "Insufficient Data",
    ]
    assert payload["investor_signal"] in [
        "Buy Watch",
        "Selective Buy",
        "Hold",
        "Caution",
        "Avoid Watch",
        "Insufficient Data",
    ]
    assert 0 <= payload["confidence_score"] <= 1

    assert "composite_cycle_score" in score_breakdown
    assert "price_momentum" in score_breakdown
    assert "rent_momentum" in score_breakdown
    assert "inventory_tightness" in score_breakdown
    assert "affordability" in score_breakdown
    assert "labor_market" in score_breakdown
    assert "data_completeness" in score_breakdown


def test_get_market_detail_includes_source_freshness():
    response = client.get(f"/markets/{TEST_GEO_ID}")

    assert response.status_code == 200

    payload = response.json()

    assert len(payload["source_freshness"]) >= 2

    source_freshness = {
        (item["source"], item["dataset"]): item
        for item in payload["source_freshness"]
    }

    assert ("test_source", "zhvi") in source_freshness
    assert ("test_source", "zori") in source_freshness
    assert source_freshness[("test_source", "zhvi")]["is_stale"] is False


def test_get_market_detail_returns_404_for_invalid_market():
    response = client.get("/markets/not_a_real_geo_id")

    assert response.status_code == 404

    payload = response.json()

    assert payload["error"]["code"] == "http_error"
    assert "not_a_real_geo_id" in payload["error"]["message"]


def test_get_market_detail_handles_market_without_metrics():
    response = client.get(f"/markets/{TEST_NO_METRICS_GEO_ID}")

    assert response.status_code == 200

    payload = response.json()

    assert payload["market"]["geo_id"] == TEST_NO_METRICS_GEO_ID
    assert payload["latest_period"] is None
    assert payload["cycle_phase"] == "Insufficient Data"
    assert payload["investor_signal"] == "Insufficient Data"
    assert payload["confidence_score"] == 0.0
    assert payload["quality_flags"]["missing_latest_metrics"] is True

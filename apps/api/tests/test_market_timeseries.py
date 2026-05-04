from fastapi.testclient import TestClient
from sqlalchemy import text

from app.db.session import engine
from app.main import app


client = TestClient(app)

TEST_GEO_ID = "test_market_timeseries_metro"


def cleanup_test_data() -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM analytics.market_monthly_metrics
                WHERE geo_id = :geo_id
                """
            ),
            {"geo_id": TEST_GEO_ID},
        )

        connection.execute(
            text(
                """
                DELETE FROM geo.dim_geo
                WHERE geo_id = :geo_id
                """
            ),
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
                    cbsa_code,
                    country_code,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES (
                    :geo_id,
                    'metro',
                    'Test Timeseries Metro, MI',
                    'Test Timeseries Metro, MI',
                    'MI',
                    'Michigan',
                    '99997',
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
                    zhvi_yoy,
                    zori_yoy,
                    active_listings_yoy,
                    months_supply,
                    payment_to_income_ratio,
                    unemployment_rate,
                    building_permits,
                    created_at,
                    updated_at
                )
                VALUES
                    (
                        :geo_id,
                        DATE '2026-01-01',
                        1.0,
                        2.0,
                        -2.0,
                        3.5,
                        0.31,
                        4.1,
                        100,
                        now(),
                        now()
                    ),
                    (
                        :geo_id,
                        DATE '2026-02-01',
                        2.0,
                        3.0,
                        -5.0,
                        3.0,
                        0.30,
                        4.0,
                        120,
                        now(),
                        now()
                    ),
                    (
                        :geo_id,
                        DATE '2026-03-01',
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        140,
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


def test_get_market_timeseries_returns_ordered_monthly_values():
    response = client.get(
        f"/markets/{TEST_GEO_ID}/timeseries"
        "?metrics=home_price_yoy,rent_yoy,building_permits"
    )

    assert response.status_code == 200

    payload = response.json()

    assert payload["market"]["geo_id"] == TEST_GEO_ID
    assert payload["metrics"] == [
        "home_price_yoy",
        "rent_yoy",
        "building_permits",
    ]

    periods = [item["period_month"] for item in payload["items"]]

    assert periods == [
        "2026-01-01",
        "2026-02-01",
        "2026-03-01",
    ]

    assert payload["items"][0]["values"]["home_price_yoy"] == 1.0
    assert payload["items"][0]["values"]["rent_yoy"] == 2.0
    assert payload["items"][0]["values"]["building_permits"] == 100.0


def test_get_market_timeseries_supports_date_filters():
    response = client.get(
        f"/markets/{TEST_GEO_ID}/timeseries"
        "?metrics=building_permits"
        "&start_date=2026-02-01"
        "&end_date=2026-03-01"
    )

    assert response.status_code == 200

    payload = response.json()

    periods = [item["period_month"] for item in payload["items"]]

    assert periods == [
        "2026-02-01",
        "2026-03-01",
    ]


def test_get_market_timeseries_supports_composite_cycle_score():
    response = client.get(
        f"/markets/{TEST_GEO_ID}/timeseries"
        "?metrics=composite_cycle_score"
    )

    assert response.status_code == 200

    payload = response.json()

    assert payload["items"][0]["values"]["composite_cycle_score"] is not None
    assert payload["items"][1]["values"]["composite_cycle_score"] is not None
    assert payload["items"][2]["values"]["composite_cycle_score"] is None
    assert "composite_cycle_score" in payload["items"][2]["missing_metrics"]


def test_get_market_timeseries_handles_missing_values_cleanly():
    response = client.get(
        f"/markets/{TEST_GEO_ID}/timeseries"
        "?metrics=home_price_yoy,rent_yoy"
    )

    assert response.status_code == 200

    payload = response.json()

    march = payload["items"][2]

    assert march["period_month"] == "2026-03-01"
    assert march["values"]["home_price_yoy"] is None
    assert march["values"]["rent_yoy"] is None
    assert march["missing_metrics"] == [
        "home_price_yoy",
        "rent_yoy",
    ]


def test_get_market_timeseries_rejects_unsupported_metrics():
    response = client.get(
        f"/markets/{TEST_GEO_ID}/timeseries"
        "?metrics=home_price_yoy,not_a_metric"
    )

    assert response.status_code == 422

    payload = response.json()

    assert payload["error"]["code"] == "unsupported_metric"
    assert payload["error"]["message"] == "Unsupported timeseries metrics requested."
    assert payload["error"]["details"]["unsupported_metrics"] == ["not_a_metric"]
    assert "home_price_yoy" in payload["error"]["details"]["supported_metrics"]


def test_get_market_timeseries_returns_404_for_invalid_market():
    response = client.get(
        "/markets/not_a_real_geo_id/timeseries"
        "?metrics=home_price_yoy"
    )

    assert response.status_code == 404

from fastapi.testclient import TestClient
from sqlalchemy import text

from app.db.session import engine
from app.main import app


client = TestClient(app)

GEO_ID_A = "test_compare_market_a"
GEO_ID_B = "test_compare_market_b"


def cleanup_test_data() -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM analytics.market_monthly_metrics
                WHERE geo_id IN (:geo_id_a, :geo_id_b)
                """
            ),
            {
                "geo_id_a": GEO_ID_A,
                "geo_id_b": GEO_ID_B,
            },
        )

        connection.execute(
            text(
                """
                DELETE FROM geo.dim_geo
                WHERE geo_id IN (:geo_id_a, :geo_id_b)
                """
            ),
            {
                "geo_id_a": GEO_ID_A,
                "geo_id_b": GEO_ID_B,
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
                    cbsa_code,
                    country_code,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES
                    (
                        :geo_id_a,
                        'metro',
                        'Test Compare Market A, MI',
                        'Test Compare Market A, MI',
                        'MI',
                        'Michigan',
                        '99991',
                        'US',
                        true,
                        now(),
                        now()
                    ),
                    (
                        :geo_id_b,
                        'metro',
                        'Test Compare Market B, OH',
                        'Test Compare Market B, OH',
                        'OH',
                        'Ohio',
                        '99992',
                        'US',
                        true,
                        now(),
                        now()
                    )
                """
            ),
            {
                "geo_id_a": GEO_ID_A,
                "geo_id_b": GEO_ID_B,
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
                    payment_to_income_ratio,
                    unemployment_rate,
                    building_permits,
                    created_at,
                    updated_at
                )
                VALUES
                    (
                        :geo_id_a,
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
                        :geo_id_a,
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
                        :geo_id_b,
                        DATE '2026-01-01',
                        -1.0,
                        1.0,
                        10.0,
                        5.0,
                        0.38,
                        5.5,
                        80,
                        now(),
                        now()
                    ),
                    (
                        :geo_id_b,
                        DATE '2026-02-01',
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        90,
                        now(),
                        now()
                    )
                """
            ),
            {
                "geo_id_a": GEO_ID_A,
                "geo_id_b": GEO_ID_B,
            },
        )


def setup_module() -> None:
    seed_test_data()


def teardown_module() -> None:
    cleanup_test_data()


def test_compare_markets_returns_latest_and_timeseries():
    response = client.get(
        f"/compare/markets?geo_ids={GEO_ID_A},{GEO_ID_B}"
        "&metrics=home_price_yoy,rent_yoy,building_permits,composite_cycle_score"
        "&start_date=2026-01-01"
        "&end_date=2026-02-01"
    )

    assert response.status_code == 200

    payload = response.json()

    assert [market["geo_id"] for market in payload["markets"]] == [
        GEO_ID_A,
        GEO_ID_B,
    ]

    assert payload["metrics"] == [
        "home_price_yoy",
        "rent_yoy",
        "building_permits",
        "composite_cycle_score",
    ]

    assert len(payload["latest"]) == 2
    assert len(payload["timeseries"]) == 2

    jan = payload["timeseries"][0]
    feb = payload["timeseries"][1]

    assert jan["period_month"] == "2026-01-01"
    assert feb["period_month"] == "2026-02-01"

    assert jan["markets"][GEO_ID_A]["home_price_yoy"] == 1.0
    assert jan["markets"][GEO_ID_B]["home_price_yoy"] == -1.0

    assert feb["markets"][GEO_ID_A]["building_permits"] == 120.0
    assert feb["markets"][GEO_ID_B]["building_permits"] == 90.0


def test_compare_markets_uses_default_metrics():
    response = client.get(
        f"/compare/markets?geo_ids={GEO_ID_A},{GEO_ID_B}"
        "&start_date=2026-01-01"
        "&end_date=2026-01-01"
    )

    assert response.status_code == 200

    payload = response.json()

    assert payload["metrics"] == [
        "home_price_yoy",
        "rent_yoy",
        "building_permits",
        "composite_cycle_score",
    ]


def test_compare_markets_rejects_too_few_markets():
    response = client.get(f"/compare/markets?geo_ids={GEO_ID_A}")

    assert response.status_code == 422

    payload = response.json()

    assert payload["error"]["code"] == "compare_market_count_invalid"


def test_compare_markets_rejects_too_many_markets():
    response = client.get(
        "/compare/markets?geo_ids=a,b,c,d,e,f"
    )

    assert response.status_code == 422

    payload = response.json()

    assert payload["error"]["code"] == "compare_market_count_invalid"


def test_compare_markets_reports_invalid_geo_ids_cleanly():
    response = client.get(
        f"/compare/markets?geo_ids={GEO_ID_A},not_a_real_geo_id"
    )

    assert response.status_code == 422

    payload = response.json()

    assert payload["error"]["code"] == "invalid_geo_ids"
    assert payload["error"]["details"]["invalid_geo_ids"] == [
        "not_a_real_geo_id"
    ]


def test_compare_markets_rejects_unsupported_metric():
    response = client.get(
        f"/compare/markets?geo_ids={GEO_ID_A},{GEO_ID_B}"
        "&metrics=home_price_yoy,bad_metric"
    )

    assert response.status_code == 422

    payload = response.json()

    assert payload["error"]["code"] == "unsupported_metric"
    assert payload["error"]["details"]["unsupported_metrics"] == [
        "bad_metric"
    ]

from fastapi.testclient import TestClient
from sqlalchemy import text

from app.db.session import engine
from app.main import app


client = TestClient(app)

TEST_GEO_ID = "test_map_metro"


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
                DELETE FROM geo.geo_geometry
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
                    'Test Map Metro, MI',
                    'Test Map Metro, MI',
                    'MI',
                    'Michigan',
                    '99996',
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
                INSERT INTO geo.geo_geometry (
                    geo_id,
                    geo_type,
                    geometry_source,
                    geometry_year,
                    geometry,
                    simplified_geometry,
                    created_at,
                    updated_at
                )
                VALUES (
                    :geo_id,
                    'metro',
                    'test',
                    2026,
                    ST_Multi(
                        ST_GeomFromText(
                            'POLYGON((-83.10 42.30, -83.00 42.30, -83.00 42.40, -83.10 42.40, -83.10 42.30))',
                            4326
                        )
                    ),
                    ST_Multi(
                        ST_GeomFromText(
                            'POLYGON((-83.10 42.30, -83.00 42.30, -83.00 42.40, -83.10 42.40, -83.10 42.30))',
                            4326
                        )
                    ),
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
                VALUES (
                    :geo_id,
                    DATE '2026-03-01',
                    2.5,
                    3.0,
                    -4.0,
                    3.2,
                    0.31,
                    4.2,
                    100,
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


def test_get_map_markets_returns_geojson_feature_collection():
    response = client.get(
        "/map/markets?geo_type=metro&metric=building_permits&period_month=2026-03-01"
    )

    assert response.status_code == 200

    payload = response.json()

    assert payload["type"] == "FeatureCollection"
    assert "features" in payload

    test_features = [
        feature
        for feature in payload["features"]
        if feature["properties"]["geo_id"] == TEST_GEO_ID
    ]

    assert len(test_features) == 1

    feature = test_features[0]

    assert feature["type"] == "Feature"
    assert feature["geometry"]["type"] == "MultiPolygon"

    assert feature["properties"]["geo_id"] == TEST_GEO_ID
    assert feature["properties"]["metric"] == "building_permits"
    assert feature["properties"]["value"] == 100.0
    assert feature["properties"]["period_month"] == "2026-03-01"
    assert feature["properties"]["cycle_phase"] != ""
    assert feature["properties"]["investor_signal"] != ""


def test_get_map_markets_defaults_to_latest_available_period():
    response = client.get("/map/markets?geo_type=metro&metric=building_permits")

    assert response.status_code == 200

    payload = response.json()

    test_features = [
        feature
        for feature in payload["features"]
        if feature["properties"]["geo_id"] == TEST_GEO_ID
    ]

    assert len(test_features) == 1
    assert test_features[0]["properties"]["period_month"] == "2026-03-01"


def test_get_map_markets_supports_derived_composite_cycle_score():
    response = client.get(
        "/map/markets?geo_type=metro&metric=composite_cycle_score&period_month=2026-03-01"
    )

    assert response.status_code == 200

    payload = response.json()

    test_features = [
        feature
        for feature in payload["features"]
        if feature["properties"]["geo_id"] == TEST_GEO_ID
    ]

    assert len(test_features) == 1

    value = test_features[0]["properties"]["value"]

    assert value is not None
    assert value > 0


def test_get_map_markets_rejects_bad_metric():
    response = client.get("/map/markets?geo_type=metro&metric=bad_metric")

    assert response.status_code == 422

    payload = response.json()

    assert payload["error"]["code"] == "unsupported_metric"
    assert payload["error"]["details"]["unsupported_metric"] == "bad_metric"


def test_get_map_markets_rejects_bad_geo_type():
    response = client.get("/map/markets?geo_type=national&metric=building_permits")

    assert response.status_code == 422

    payload = response.json()

    assert payload["error"]["code"] in ["validation_error", "unsupported_geo_type"]


def test_get_map_markets_sets_cache_header():
    response = client.get(
        "/map/markets?geo_type=metro&metric=building_permits&period_month=2026-03-01"
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=300"

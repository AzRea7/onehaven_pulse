from pipelines.extractors.overture_maps_api.config import OVERTURE_MAPS_API_PLACES


def test_overture_maps_api_places_configured():
    assert OVERTURE_MAPS_API_PLACES.dataset == "places"
    assert OVERTURE_MAPS_API_PLACES.endpoint == "places"
    assert OVERTURE_MAPS_API_PLACES.filename.endswith(".json")
    assert OVERTURE_MAPS_API_PLACES.base_url.startswith("https://")
    assert OVERTURE_MAPS_API_PLACES.area_slug
    assert OVERTURE_MAPS_API_PLACES.country
    assert OVERTURE_MAPS_API_PLACES.radius > 0
    assert OVERTURE_MAPS_API_PLACES.limit > 0

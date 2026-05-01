from unittest.mock import Mock, patch

from pipelines.extractors.overture_maps_api.client import OvertureMapsApiClient
from pipelines.extractors.overture_maps_api.config import OvertureMapsApiDataset


def _dataset(**overrides) -> OvertureMapsApiDataset:
    values = {
        "dataset": "places",
        "endpoint": "places",
        "filename": "overture_maps_api_detroit_metro_places.json",
        "description": "Test Overture Maps API places",
        "expected_frequency": "on_demand",
        "base_url": "https://api.overturemapsapi.com",
        "area_slug": "detroit_metro",
        "area_name": "Detroit Metro",
        "country": "US",
        "lat": 42.3314,
        "lng": -83.0458,
        "radius": 25000,
        "categories": "cafes,supermarket",
        "brand_name": "",
        "limit": 10,
    }
    values.update(overrides)
    return OvertureMapsApiDataset(**values)


def test_overture_maps_api_client_requests_places(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "overture_maps_api_key", "test_key")

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.url = "https://api.overturemapsapi.com/places"
    mock_response.json.return_value = [
        {
            "id": "place_1",
            "geometry": {"type": "Point", "coordinates": [-83.1, 42.3]},
            "properties": {
                "names": {"primary": "Test Cafe"},
                "categories": {"primary": "cafe"},
            },
        }
    ]
    mock_response.raise_for_status.return_value = None
    mock_response.text = ""

    with patch(
        "pipelines.extractors.overture_maps_api.client.requests.get",
        return_value=mock_response,
    ) as mock_get:
        client = OvertureMapsApiClient()
        payload = client.get_places(_dataset())

    assert payload["record_count"] == 1
    assert payload["records"][0]["id"] == "place_1"

    called_url = mock_get.call_args.args[0]
    called_headers = mock_get.call_args.kwargs["headers"]
    called_params = mock_get.call_args.kwargs["params"]

    assert called_url == "https://api.overturemapsapi.com/places"
    assert called_headers["x-api-key"] == "test_key"
    assert called_params["country"] == "US"
    assert called_params["lat"] == "42.3314"
    assert called_params["lng"] == "-83.0458"
    assert called_params["radius"] == "25000"
    assert called_params["categories"] == "cafes,supermarket"
    assert called_params["limit"] == "10"


def test_overture_maps_api_client_requires_key(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "overture_maps_api_key", None)

    try:
        OvertureMapsApiClient()
    except ValueError as exc:
        assert "OVERTURE_MAPS_API_KEY is missing" in str(exc)
    else:
        raise AssertionError("Expected ValueError when API key is missing")


def test_overture_maps_api_client_rejects_non_places_endpoint(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "overture_maps_api_key", "test_key")

    client = OvertureMapsApiClient()

    try:
        client.get_places(_dataset(endpoint="places/brands"))
    except ValueError as exc:
        assert "Only the /places endpoint" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unsupported endpoint")

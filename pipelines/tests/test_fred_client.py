from unittest.mock import Mock, patch

from pipelines.extractors.fred.client import FredClient


def test_fred_client_builds_observation_request():
    mock_response = Mock()
    mock_response.json.return_value = {
        "observations": [
            {
                "date": "2026-01-01",
                "value": "6.5",
            }
        ]
    }
    mock_response.raise_for_status.return_value = None

    with patch("pipelines.extractors.fred.client.requests.get", return_value=mock_response) as mock_get:
        client = FredClient(api_key="test_key")
        payload = client.get_series_observations(series_id="MORTGAGE30US")

    assert payload["observations"][0]["value"] == "6.5"

    called_url = mock_get.call_args.args[0]
    called_params = mock_get.call_args.kwargs["params"]

    assert called_url.endswith("/series/observations")
    assert called_params["series_id"] == "MORTGAGE30US"
    assert called_params["api_key"] == "test_key"
    assert called_params["file_type"] == "json"


def test_fred_client_rejects_placeholder_api_key():
    try:
        FredClient(api_key="replace_with_your_fred_api_key")
    except ValueError as exc:
        assert "FRED_API_KEY is missing" in str(exc)
    else:
        raise AssertionError("Expected ValueError for placeholder FRED API key")

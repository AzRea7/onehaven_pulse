from unittest.mock import Mock, patch

from pipelines.extractors.fred.client import FredClient


def test_fred_client_fetches_observations(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "fred_api_key", "test_fred_key")
    monkeypatch.setattr(settings_module.settings, "fred_base_url", "https://api.stlouisfed.org/fred")

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "observations": [
            {
                "date": "2026-01-01",
                "value": "4.25",
            }
        ]
    }
    mock_response.raise_for_status.return_value = None

    with patch("pipelines.extractors.fred.client.requests.get", return_value=mock_response) as mock_get:
        client = FredClient()
        payload = client.get_series_observations("DGS10")

    assert "observations" in payload
    assert payload["observations"][0]["value"] == "4.25"

    called_params = mock_get.call_args.kwargs["params"]

    assert called_params["series_id"] == "DGS10"
    assert called_params["api_key"] == "test_fred_key"
    assert called_params["file_type"] == "json"


def test_fred_client_requires_api_key(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "fred_api_key", None)

    try:
        FredClient()
    except ValueError as exc:
        assert "FRED_API_KEY is missing" in str(exc)
    else:
        raise AssertionError("Expected ValueError when FRED_API_KEY is missing")


def test_fred_client_rejects_missing_observations(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "fred_api_key", "test_fred_key")

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {}
    mock_response.raise_for_status.return_value = None

    with patch("pipelines.extractors.fred.client.requests.get", return_value=mock_response):
        client = FredClient()

        try:
            client.get_series_observations("DGS10")
        except ValueError as exc:
            assert "missing observations" in str(exc)
        else:
            raise AssertionError("Expected ValueError for missing observations")

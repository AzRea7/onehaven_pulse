from unittest.mock import Mock, patch

from pipelines.extractors.census_acs.client import CensusAcsClient
from pipelines.extractors.census_acs.config import CENSUS_ACS_STATE


def test_census_acs_client_requests_dataset_with_api_key(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "census_data_api_key", "test_census_key")

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        ["NAME", "DP05_0001E", "state"],
        ["Michigan", "10000000", "26"],
    ]
    mock_response.raise_for_status.return_value = None

    with patch("pipelines.extractors.census_acs.client.requests.get", return_value=mock_response) as mock_get:
        client = CensusAcsClient()
        payload = client.get_dataset(CENSUS_ACS_STATE)

    assert payload[0][0] == "NAME"
    assert payload[1][0] == "Michigan"

    called_url = mock_get.call_args.args[0]
    called_params = mock_get.call_args.kwargs["params"]

    assert "/acs/acs5/profile" in called_url
    assert "DP05_0001E" in called_params["get"]
    assert called_params["for"] == "state:*"
    assert called_params["key"] == "test_census_key"


def test_census_acs_client_omits_placeholder_api_key(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(
        settings_module.settings,
        "census_data_api_key",
        "replace_with_your_census_data_api_key",
    )

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        ["NAME", "DP05_0001E", "state"],
        ["Michigan", "10000000", "26"],
    ]
    mock_response.raise_for_status.return_value = None

    with patch("pipelines.extractors.census_acs.client.requests.get", return_value=mock_response) as mock_get:
        client = CensusAcsClient()
        client.get_dataset(CENSUS_ACS_STATE)

    called_params = mock_get.call_args.kwargs["params"]

    assert "key" not in called_params


def test_census_acs_client_rejects_empty_payload():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [["NAME", "state"]]
    mock_response.raise_for_status.return_value = None

    with patch("pipelines.extractors.census_acs.client.requests.get", return_value=mock_response):
        client = CensusAcsClient()

        try:
            client.get_dataset(CENSUS_ACS_STATE)
        except ValueError as exc:
            assert "no data rows" in str(exc)
        else:
            raise AssertionError("Expected ValueError for empty ACS payload")


def test_census_acs_client_reports_403():
    mock_response = Mock()
    mock_response.status_code = 403
    mock_response.url = "https://api.census.gov/data/test"
    mock_response.text = "forbidden"

    with patch("pipelines.extractors.census_acs.client.requests.get", return_value=mock_response):
        client = CensusAcsClient()

        try:
            client.get_dataset(CENSUS_ACS_STATE)
        except ValueError as exc:
            assert "Check CENSUS_DATA_API_KEY" in str(exc)
        else:
            raise AssertionError("Expected ValueError for 403 ACS response")

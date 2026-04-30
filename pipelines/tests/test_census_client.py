from unittest.mock import Mock, patch

from pipelines.extractors.census.client import CensusClient
from pipelines.extractors.census.config import CENSUS_STATE


def test_census_client_downloads_zip():
    mock_response = Mock()
    mock_response.content = b"PK fake zip content"
    mock_response.raise_for_status.return_value = None

    with patch("pipelines.extractors.census.client.requests.get", return_value=mock_response) as mock_get:
        client = CensusClient()
        content = client.download_dataset(CENSUS_STATE)

    assert content.startswith(b"PK")
    assert mock_get.call_args.args[0] == CENSUS_STATE.url


def test_census_client_rejects_non_zip_response():
    mock_response = Mock()
    mock_response.content = b"<html>not a zip</html>"
    mock_response.raise_for_status.return_value = None

    with patch("pipelines.extractors.census.client.requests.get", return_value=mock_response):
        client = CensusClient()

        try:
            client.download_dataset(CENSUS_STATE)
        except ValueError as exc:
            assert "Expected ZIP bytes" in str(exc)
        else:
            raise AssertionError("Expected ValueError for non-ZIP Census response")

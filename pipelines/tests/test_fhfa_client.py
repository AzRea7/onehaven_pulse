from unittest.mock import Mock, patch

from pipelines.extractors.fhfa.client import FhfaClient
from pipelines.extractors.fhfa.config import FHFA_HPI_MASTER


def test_fhfa_client_downloads_dataset():
    mock_response = Mock()
    mock_response.content = (
        b"hpi_type,hpi_flavor,frequency,level,place_name,place_id,yr,period,index_nsa,index_sa\n"
        b"traditional,purchase-only,monthly,USA,United States,USA,2026,1,100.0,101.0\n"
    )
    mock_response.headers = {"content-type": "text/csv"}
    mock_response.raise_for_status.return_value = None

    with patch("pipelines.extractors.fhfa.client.requests.get", return_value=mock_response) as mock_get:
        client = FhfaClient()
        content = client.download_dataset(FHFA_HPI_MASTER)

    assert b"hpi_type" in content
    assert mock_get.call_args.args[0] == FHFA_HPI_MASTER.url
    assert "User-Agent" in mock_get.call_args.kwargs["headers"]


def test_fhfa_client_rejects_empty_response():
    mock_response = Mock()
    mock_response.content = b""
    mock_response.headers = {"content-type": "text/csv"}
    mock_response.raise_for_status.return_value = None

    with patch("pipelines.extractors.fhfa.client.requests.get", return_value=mock_response):
        client = FhfaClient()

        try:
            client.download_dataset(FHFA_HPI_MASTER)
        except ValueError as exc:
            assert "empty" in str(exc)
        else:
            raise AssertionError("Expected ValueError for empty FHFA response")

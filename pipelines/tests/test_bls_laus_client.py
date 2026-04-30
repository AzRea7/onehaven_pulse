from unittest.mock import Mock, patch

from pipelines.extractors.bls_laus.client import BlsLausClient
from pipelines.extractors.bls_laus.config import BLS_LAUS_DATASET


def test_bls_laus_client_requests_dataset():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [
                {
                    "seriesID": "LASST260000000000003",
                    "data": [
                        {
                            "year": "2026",
                            "period": "M01",
                            "periodName": "January",
                            "value": "4.1",
                        }
                    ],
                }
            ]
        },
    }
    mock_response.raise_for_status.return_value = None
    mock_response.text = ""

    with patch(
        "pipelines.extractors.bls_laus.client.requests.post",
        return_value=mock_response,
    ) as mock_post:
        client = BlsLausClient()
        payload = client.get_dataset(BLS_LAUS_DATASET)

    called_payload = mock_post.call_args.kwargs["json"]

    assert "seriesid" in called_payload
    assert "LASST260000000000003" in called_payload["seriesid"]
    assert payload["response"]["status"] == "REQUEST_SUCCEEDED"
    assert client.get_observation_count(payload) == 1


def test_bls_laus_client_rejects_failed_response():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": "REQUEST_FAILED",
        "message": ["Invalid series id"],
    }
    mock_response.raise_for_status.return_value = None
    mock_response.text = ""

    with patch(
        "pipelines.extractors.bls_laus.client.requests.post",
        return_value=mock_response,
    ):
        client = BlsLausClient()

        try:
            client.get_dataset(BLS_LAUS_DATASET)
        except ValueError as exc:
            assert "REQUEST_FAILED" in str(exc)
        else:
            raise AssertionError("Expected ValueError for failed BLS response")

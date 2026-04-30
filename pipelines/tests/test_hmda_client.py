from unittest.mock import Mock, patch

from pipelines.extractors.hmda.client import HmdaClient
from pipelines.extractors.hmda.config import HmdaDataset


def _dataset() -> HmdaDataset:
    return HmdaDataset(
        dataset="modified_lar",
        filename="hmda_modified_lar_2023_states_MI.csv",
        description="Test HMDA API CSV",
        expected_frequency="annual",
        year=2023,
        geography_type="states",
        geography_values="MI",
        actions_taken="1",
        loan_purposes="1",
        loan_types="",
        lien_statuses="",
    )


def test_hmda_client_requests_csv_dataset():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = (
        b"activity_year,lei,state_code,county_code,census_tract,loan_amount,action_taken\n"
        b"2023,TESTLEI,MI,163,26163500000,250000,1\n"
    )
    mock_response.url = (
        "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv"
        "?states=MI&years=2023&actions_taken=1&loan_purposes=1"
    )
    mock_response.raise_for_status.return_value = None
    mock_response.text = mock_response.content.decode("utf-8")

    with patch("pipelines.extractors.hmda.client.requests.get", return_value=mock_response) as mock_get:
        client = HmdaClient()
        content = client.get_modified_lar_csv(_dataset())

    assert b"activity_year" in content
    assert b"loan_amount" in content

    called_url = mock_get.call_args.args[0]
    called_params = mock_get.call_args.kwargs["params"]

    assert called_url.endswith("/csv")
    assert called_params["years"] == "2023"
    assert called_params["states"] == "MI"
    assert called_params["actions_taken"] == "1"
    assert called_params["loan_purposes"] == "1"
    assert "loan_types" not in called_params
    assert "lien_statuses" not in called_params


def test_hmda_client_rejects_invalid_geography_type():
    dataset = HmdaDataset(
        dataset="modified_lar",
        filename="bad.csv",
        description="Bad HMDA",
        expected_frequency="annual",
        year=2023,
        geography_type="bad_geo",
        geography_values="MI",
        actions_taken="1",
        loan_purposes="1",
        loan_types="",
        lien_statuses="",
    )

    client = HmdaClient()

    try:
        client.get_modified_lar_csv(dataset)
    except ValueError as exc:
        assert "Invalid HMDA_GEOGRAPHY_TYPE" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid geography type")


def test_hmda_client_rejects_too_many_filters():
    dataset = HmdaDataset(
        dataset="modified_lar",
        filename="too_many.csv",
        description="Too many HMDA filters",
        expected_frequency="annual",
        year=2023,
        geography_type="states",
        geography_values="MI",
        actions_taken="1",
        loan_purposes="1",
        loan_types="1",
        lien_statuses="1",
    )

    client = HmdaClient()

    try:
        client.get_modified_lar_csv(dataset)
    except ValueError as exc:
        assert "two or fewer HMDA data filters" in str(exc)
    else:
        raise AssertionError("Expected ValueError for too many filters")


def test_hmda_client_rejects_error_payload():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b'{"errorType":"provide-atleast-msamds-or-states","message":"Provide year"}'
    mock_response.url = "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv"
    mock_response.raise_for_status.return_value = None
    mock_response.text = mock_response.content.decode("utf-8")

    with patch("pipelines.extractors.hmda.client.requests.get", return_value=mock_response):
        client = HmdaClient()

        try:
            client.get_modified_lar_csv(_dataset())
        except ValueError as exc:
            assert "error payload" in str(exc)
        else:
            raise AssertionError("Expected ValueError for HMDA error payload")

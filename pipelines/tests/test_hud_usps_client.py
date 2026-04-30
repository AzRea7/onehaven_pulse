from unittest.mock import Mock, patch

from pipelines.extractors.hud_usps.client import HudUspsClient
from pipelines.extractors.hud_usps.config import HudUspsDataset


def _dataset() -> HudUspsDataset:
    return HudUspsDataset(
        dataset="zip_crosswalk",
        crosswalk_type="zip_county",
        api_type=2,
        filename="hud_usps_zip_county_2025_q4.json",
        description="Test ZIP County",
        expected_frequency="quarterly",
        year=2025,
        quarter=4,
        query="All",
    )


def test_hud_usps_client_requests_dataset(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "hud_usps_access_token", "test_token")
    monkeypatch.setattr(
        settings_module.settings,
        "hud_usps_api_base_url",
        "https://www.huduser.gov/hudapi/public/usps",
    )

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.url = (
        "https://www.huduser.gov/hudapi/public/usps"
        "?type=2&query=All&year=2025&quarter=4"
    )
    mock_response.json.return_value = {
        "data": {
            "year": "2025",
            "quarter": "Q4",
            "query": "All",
            "crosswalk_type": "zip-county",
            "results": [
                {
                    "zip": "48201",
                    "geoid": "26163",
                    "res_ratio": 1.0,
                    "bus_ratio": 1.0,
                    "oth_ratio": 1.0,
                    "tot_ratio": 1.0,
                }
            ],
        }
    }
    mock_response.raise_for_status.return_value = None
    mock_response.text = ""

    with patch(
        "pipelines.extractors.hud_usps.client.requests.get",
        return_value=mock_response,
    ) as mock_get:
        client = HudUspsClient()
        payload = client.get_dataset(_dataset())

    assert payload["data"]["crosswalk_type"] == "zip-county"
    assert client.get_result_count(payload) == 1

    called_headers = mock_get.call_args.kwargs["headers"]
    called_params = mock_get.call_args.kwargs["params"]

    assert called_headers["Authorization"] == "Bearer test_token"
    assert called_headers["Accept"] == "application/json"
    assert called_params["type"] == "2"
    assert called_params["query"] == "All"
    assert called_params["year"] == "2025"
    assert called_params["quarter"] == "4"


def test_hud_usps_client_rejects_missing_token(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "hud_usps_access_token", None)

    try:
        HudUspsClient()
    except ValueError as exc:
        assert "HUD_USPS_ACCESS_TOKEN is missing" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing HUD token")


def test_hud_usps_client_reports_401(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "hud_usps_access_token", "bad_token")

    mock_response = Mock()
    mock_response.status_code = 401
    mock_response.url = "https://www.huduser.gov/hudapi/public/usps"
    mock_response.text = "Unauthorized"

    with patch(
        "pipelines.extractors.hud_usps.client.requests.get",
        return_value=mock_response,
    ):
        client = HudUspsClient()

        try:
            client.get_dataset(_dataset())
        except ValueError as exc:
            assert "Authentication failed" in str(exc)
        else:
            raise AssertionError("Expected ValueError for 401 HUD response")

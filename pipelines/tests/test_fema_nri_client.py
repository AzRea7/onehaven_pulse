from unittest.mock import Mock, patch

from pipelines.extractors.fema_nri.client import FemaNriClient
from pipelines.extractors.fema_nri.config import FemaNriDataset


def _dataset(**overrides) -> FemaNriDataset:
    values = {
        "dataset": "county_risk",
        "filename": "fema_nri_counties_arcgis.json",
        "description": "Test FEMA NRI",
        "expected_frequency": "periodic",
        "source_mode": "arcgis",
        "arcgis_item_id": "39485e8035d446a5bff03259508ae355",
        "arcgis_service_url": "",
        "arcgis_layer_id": 0,
        "arcgis_page_size": 2000,
        "arcgis_where": "1=1",
        "arcgis_out_fields": "*",
        "version": "1.20",
        "release_label": "December 2025",
    }
    values.update(overrides)
    return FemaNriDataset(**values)


def test_effective_page_size_respects_layer_max_record_count():
    client = FemaNriClient()

    page_size = client._effective_page_size(
        configured_page_size=5000,
        layer_metadata={"maxRecordCount": 2000},
    )

    assert page_size == 2000


def test_layer_url_builds_with_layer_id():
    client = FemaNriClient()

    assert (
        client._layer_url("https://example.com/FeatureServer", 0)
        == "https://example.com/FeatureServer/0"
    )


def test_fema_nri_client_fetches_arcgis_records():
    item_response = Mock()
    item_response.raise_for_status.return_value = None
    item_response.json.return_value = {
        "id": "39485e8035d446a5bff03259508ae355",
        "title": "National Risk Index Counties",
        "url": "https://services.arcgis.com/example/ArcGIS/rest/services/NRI/FeatureServer",
    }

    layer_response = Mock()
    layer_response.raise_for_status.return_value = None
    layer_response.json.return_value = {
        "name": "National Risk Index Counties",
        "maxRecordCount": 2000,
        "fields": [{"name": "COUNTYFIPS"}, {"name": "RISK_SCORE"}],
    }

    query_response = Mock()
    query_response.raise_for_status.return_value = None
    query_response.url = (
        "https://services.arcgis.com/example/ArcGIS/rest/services/NRI/FeatureServer/0/query"
    )
    query_response.json.return_value = {
        "features": [
            {
                "attributes": {
                    "COUNTYFIPS": "01001",
                    "STATE": "Alabama",
                    "COUNTY": "Autauga",
                    "RISK_SCORE": 50.1,
                }
            }
        ]
    }

    with patch(
        "pipelines.extractors.fema_nri.client.requests.get",
        side_effect=[item_response, layer_response, query_response],
    ):
        client = FemaNriClient()
        payload = client.get_dataset(_dataset())

    assert payload["record_count"] == 1
    assert payload["page_count"] == 1
    assert payload["records"][0]["COUNTYFIPS"] == "01001"
    assert payload["request"]["return_geometry"] is False


def test_fema_nri_client_uses_configured_service_url_without_item_lookup():
    layer_response = Mock()
    layer_response.raise_for_status.return_value = None
    layer_response.json.return_value = {
        "name": "National Risk Index Counties",
        "maxRecordCount": 2000,
    }

    query_response = Mock()
    query_response.raise_for_status.return_value = None
    query_response.url = "https://example.com/FeatureServer/0/query"
    query_response.json.return_value = {
        "features": [
            {"attributes": {"COUNTYFIPS": "01001", "RISK_SCORE": 50.1}}
        ]
    }

    with patch(
        "pipelines.extractors.fema_nri.client.requests.get",
        side_effect=[layer_response, query_response],
    ):
        client = FemaNriClient()
        payload = client.get_dataset(
            _dataset(arcgis_service_url="https://example.com/FeatureServer")
        )

    assert payload["record_count"] == 1
    assert payload["request"]["service_url"] == "https://example.com/FeatureServer"

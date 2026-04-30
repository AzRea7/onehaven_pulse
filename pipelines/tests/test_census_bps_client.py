from pathlib import Path
from unittest.mock import Mock, patch

from pipelines.extractors.census_building_permits.client import CensusBpsClient
from pipelines.extractors.census_building_permits.config import CensusBpsDataset

XLS_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"


def test_census_bps_client_reads_local_xls_dataset(tmp_path, monkeypatch):
    file_path = tmp_path / "state_jan_2026.xls"
    file_path.write_bytes(XLS_MAGIC + b" fake xls content")

    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "census_bps_source_mode", "local")

    dataset = CensusBpsDataset(
        dataset="permits",
        geography_level="state",
        period_type="monthly",
        source_period_label="2026-01",
        filename="state_jan_2026.xls",
        description="Test state permits",
        expected_frequency="monthly",
        release_cadence_note="Test cadence",
        local_path=str(file_path),
        url=None,
        required=True,
    )

    client = CensusBpsClient()
    content = client.get_dataset_content(dataset)

    assert content is not None
    assert content.startswith(XLS_MAGIC)


def test_census_bps_client_downloads_xls_url_dataset(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "census_bps_source_mode", "url")

    dataset = CensusBpsDataset(
        dataset="permits",
        geography_level="state",
        period_type="monthly",
        source_period_label="2026-01",
        filename="state_jan_2026.xls",
        description="Test state permits",
        expected_frequency="monthly",
        release_cadence_note="Test cadence",
        local_path=None,
        url="https://example.com/state_jan_2026.xls",
        required=True,
    )

    mock_response = Mock()
    mock_response.content = XLS_MAGIC + b" fake xls content"
    mock_response.raise_for_status.return_value = None

    with patch(
        "pipelines.extractors.census_building_permits.client.requests.get",
        return_value=mock_response,
    ) as mock_get:
        client = CensusBpsClient()
        content = client.get_dataset_content(dataset)

    assert content is not None
    assert content.startswith(XLS_MAGIC)
    assert mock_get.call_args.args[0] == "https://example.com/state_jan_2026.xls"


def test_census_bps_client_skips_missing_optional_local_file(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "census_bps_source_mode", "local")

    dataset = CensusBpsDataset(
        dataset="permits",
        geography_level="state",
        period_type="annual",
        source_period_label="2025",
        filename="state_annual_2025.xls",
        description="Optional annual file",
        expected_frequency="annual",
        release_cadence_note="Test cadence",
        local_path=str(Path("missing_optional.xls")),
        url=None,
        required=False,
    )

    client = CensusBpsClient()
    content = client.get_dataset_content(dataset)

    assert content is None


def test_census_bps_client_rejects_missing_required_local_file(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "census_bps_source_mode", "local")

    dataset = CensusBpsDataset(
        dataset="permits",
        geography_level="state",
        period_type="monthly",
        source_period_label="2026-01",
        filename="state_jan_2026.xls",
        description="Required monthly file",
        expected_frequency="monthly",
        release_cadence_note="Test cadence",
        local_path=str(Path("missing_required.xls")),
        url=None,
        required=True,
    )

    client = CensusBpsClient()

    try:
        client.get_dataset_content(dataset)
    except FileNotFoundError as exc:
        assert "Required Census BPS local file does not exist" in str(exc)
    else:
        raise AssertionError("Expected FileNotFoundError for missing required BPS file")

from pathlib import Path
from unittest.mock import Mock, patch

from pipelines.extractors.redfin.client import RedfinClient
from pipelines.extractors.redfin.config import RedfinDataset


def test_redfin_client_reads_local_dataset(tmp_path, monkeypatch):
    csv_path = tmp_path / "market_tracker.csv"
    csv_path.write_text(
        "period_begin,period_end,region_type,region,property_type,median_sale_price\n"
        "2026-03-01,2026-03-31,metro,Detroit,All Residential,250000\n",
        encoding="utf-8",
    )

    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "redfin_source_mode", "local")

    dataset = RedfinDataset(
        dataset="market_tracker",
        metric_name="market_tracker",
        filename="market_tracker.csv",
        description="Test Redfin",
        expected_frequency="monthly",
        release_cadence_note="Test monthly cadence",
        local_path=str(csv_path),
        url=None,
    )

    client = RedfinClient()
    content = client.get_dataset_content(dataset)

    assert b"median_sale_price" in content


def test_redfin_client_downloads_url_dataset(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "redfin_source_mode", "url")

    dataset = RedfinDataset(
        dataset="market_tracker",
        metric_name="market_tracker",
        filename="market_tracker.csv",
        description="Test Redfin",
        expected_frequency="monthly",
        release_cadence_note="Test monthly cadence",
        local_path=None,
        url="https://example.com/redfin.csv",
    )

    mock_response = Mock()
    mock_response.content = (
        b"period_begin,period_end,region_type,region,property_type,median_sale_price\n"
        b"2026-03-01,2026-03-31,metro,Detroit,All Residential,250000\n"
    )
    mock_response.headers = {"content-type": "text/csv"}
    mock_response.raise_for_status.return_value = None

    with patch("pipelines.extractors.redfin.client.requests.get", return_value=mock_response) as mock_get:
        client = RedfinClient()
        content = client.get_dataset_content(dataset)

    assert b"median_sale_price" in content
    assert mock_get.call_args.args[0] == "https://example.com/redfin.csv"


def test_redfin_client_rejects_missing_local_file(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "redfin_source_mode", "local")

    dataset = RedfinDataset(
        dataset="market_tracker",
        metric_name="market_tracker",
        filename="market_tracker.csv",
        description="Test Redfin",
        expected_frequency="monthly",
        release_cadence_note="Test monthly cadence",
        local_path=str(Path("missing.csv")),
        url=None,
    )

    client = RedfinClient()

    try:
        client.get_dataset_content(dataset)
    except FileNotFoundError as exc:
        assert "does not exist" in str(exc)
    else:
        raise AssertionError("Expected FileNotFoundError for missing local Redfin file")

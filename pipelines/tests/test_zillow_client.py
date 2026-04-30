from pathlib import Path
from unittest.mock import Mock, patch

from pipelines.extractors.zillow.client import ZillowClient
from pipelines.extractors.zillow.config import ZillowDataset


def test_zillow_client_reads_local_dataset(tmp_path, monkeypatch):
    csv_path = tmp_path / "zhvi.csv"
    csv_path.write_text(
        "RegionID,SizeRank,RegionName,RegionType,StateName,2026-01-31\n"
        "1,0,United States,country,,300000\n",
        encoding="utf-8",
    )

    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "zillow_source_mode", "local")

    dataset = ZillowDataset(
        dataset="zhvi",
        metric_name="zhvi",
        filename="zhvi.csv",
        description="Test ZHVI",
        expected_frequency="monthly",
        local_path=str(csv_path),
        url=None,
    )

    client = ZillowClient()
    content = client.get_dataset_content(dataset)

    assert b"RegionID" in content


def test_zillow_client_downloads_url_dataset(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "zillow_source_mode", "url")

    dataset = ZillowDataset(
        dataset="zhvi",
        metric_name="zhvi",
        filename="zhvi.csv",
        description="Test ZHVI",
        expected_frequency="monthly",
        local_path=None,
        url="https://example.com/zhvi.csv",
    )

    mock_response = Mock()
    mock_response.content = (
        b"RegionID,SizeRank,RegionName,RegionType,StateName,2026-01-31\n"
        b"1,0,United States,country,,300000\n"
    )
    mock_response.headers = {"content-type": "text/csv"}
    mock_response.raise_for_status.return_value = None

    with patch("pipelines.extractors.zillow.client.requests.get", return_value=mock_response) as mock_get:
        client = ZillowClient()
        content = client.get_dataset_content(dataset)

    assert b"RegionID" in content
    assert mock_get.call_args.args[0] == "https://example.com/zhvi.csv"


def test_zillow_client_rejects_missing_local_file(monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "zillow_source_mode", "local")

    dataset = ZillowDataset(
        dataset="zhvi",
        metric_name="zhvi",
        filename="zhvi.csv",
        description="Test ZHVI",
        expected_frequency="monthly",
        local_path=str(Path("missing.csv")),
        url=None,
    )

    client = ZillowClient()

    try:
        client.get_dataset_content(dataset)
    except FileNotFoundError as exc:
        assert "does not exist" in str(exc)
    else:
        raise AssertionError("Expected FileNotFoundError for missing local Zillow file")

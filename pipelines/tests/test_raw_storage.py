from pathlib import Path

from pipelines.storage.local_raw_store import build_raw_file_path, write_raw_text


def test_build_raw_file_path():
    path = build_raw_file_path(
        source="fred",
        dataset="macro_series",
        filename="test.json",
        load_date="2026-04-30",
    )

    assert Path("data/raw/fred/macro_series/2026-04-30/test.json").as_posix() in path.as_posix()


def test_write_raw_text(tmp_path, monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "raw_data_dir", tmp_path / "raw")

    result = write_raw_text(
        source="test_source",
        dataset="test_dataset",
        filename="sample.txt",
        content="hello",
        load_date="2026-04-30",
    )

    file_path = Path(result["raw_file_path"])

    assert file_path.exists()
    assert file_path.read_text(encoding="utf-8") == "hello"
    assert result["file_size_bytes"] == 5
    assert len(result["checksum_sha256"]) == 64

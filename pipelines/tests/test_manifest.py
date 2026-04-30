from pathlib import Path

from pipelines.storage.manifest import build_manifest_path, write_manifest


def test_build_manifest_path():
    path = build_manifest_path(
        source="fred",
        dataset="macro_series",
        load_date="2026-04-30",
        manifest_id="test_manifest",
    )

    assert Path(
        "data/manifests/fred/macro_series/2026-04-30/test_manifest.json"
    ).as_posix() in path.as_posix()


def test_write_manifest(tmp_path, monkeypatch):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "manifest_dir", tmp_path / "manifests")

    result = write_manifest(
        source="test_source",
        dataset="test_dataset",
        raw_file_path="data/raw/test/file.csv",
        status="success",
        load_date="2026-04-30",
        file_format="csv",
        record_count=10,
        checksum_sha256="a" * 64,
        file_size_bytes=100,
    )

    manifest_path = Path(result["manifest_path"])

    assert manifest_path.exists()
    assert result["manifest"]["source"] == "test_source"
    assert result["manifest"]["dataset"] == "test_dataset"
    assert result["manifest"]["status"] == "success"
    assert result["manifest"]["record_count"] == 10

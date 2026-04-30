import json
from pathlib import Path
from typing import Any

from pipelines.common.settings import settings
from pipelines.common.time import today_iso, utc_now, utc_timestamp_slug


def build_manifest_path(
    source: str,
    dataset: str,
    load_date: str | None = None,
    manifest_id: str | None = None,
) -> Path:
    safe_load_date = load_date or today_iso()
    safe_manifest_id = manifest_id or f"{source}_{dataset}_{utc_timestamp_slug()}"

    return (
        settings.manifest_dir
        / source
        / dataset
        / safe_load_date
        / f"{safe_manifest_id}.json"
    )


def write_manifest(
    source: str,
    dataset: str,
    raw_file_path: str,
    status: str,
    load_date: str | None = None,
    source_url: str | None = None,
    file_format: str | None = None,
    record_count: int | None = None,
    checksum_sha256: str | None = None,
    file_size_bytes: int | None = None,
    source_period_start: str | None = None,
    source_period_end: str | None = None,
    error_message: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict:
    safe_load_date = load_date or today_iso()

    manifest = {
        "source": source,
        "dataset": dataset,
        "status": status,
        "load_date": safe_load_date,
        "source_url": source_url,
        "raw_file_path": raw_file_path,
        "storage_backend": settings.storage_backend,
        "file_format": file_format,
        "record_count": record_count,
        "checksum_sha256": checksum_sha256,
        "file_size_bytes": file_size_bytes,
        "source_period_start": source_period_start,
        "source_period_end": source_period_end,
        "error_message": error_message,
        "metadata": metadata or {},
        "created_at": utc_now().isoformat(),
    }

    manifest_path = build_manifest_path(
        source=source,
        dataset=dataset,
        load_date=safe_load_date,
    )

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return {
        "manifest_path": str(manifest_path),
        "manifest": manifest,
    }

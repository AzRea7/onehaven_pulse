from pathlib import Path

from pipelines.common.checksum import calculate_sha256
from pipelines.common.settings import settings
from pipelines.common.time import today_iso


def build_raw_file_path(
    source: str,
    dataset: str,
    filename: str,
    load_date: str | None = None,
) -> Path:
    safe_load_date = load_date or today_iso()

    return settings.raw_data_dir / source / dataset / safe_load_date / filename


def write_raw_bytes(
    source: str,
    dataset: str,
    filename: str,
    content: bytes,
    load_date: str | None = None,
    overwrite: bool = False,
) -> dict:
    file_path = build_raw_file_path(
        source=source,
        dataset=dataset,
        filename=filename,
        load_date=load_date,
    )

    file_path.parent.mkdir(parents=True, exist_ok=True)

    if file_path.exists() and not overwrite:
        raise FileExistsError(
            f"Raw file already exists and overwrite=False: {file_path}"
        )

    file_path.write_bytes(content)

    return {
        "raw_file_path": str(file_path),
        "file_size_bytes": file_path.stat().st_size,
        "checksum_sha256": calculate_sha256(file_path),
    }


def write_raw_text(
    source: str,
    dataset: str,
    filename: str,
    content: str,
    load_date: str | None = None,
    overwrite: bool = False,
    encoding: str = "utf-8",
) -> dict:
    return write_raw_bytes(
        source=source,
        dataset=dataset,
        filename=filename,
        content=content.encode(encoding),
        load_date=load_date,
        overwrite=overwrite,
    )

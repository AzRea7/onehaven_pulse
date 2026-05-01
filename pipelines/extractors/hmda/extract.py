from datetime import date
from typing import Any

from pipelines.common.time import today_iso
from pipelines.extractors.hmda import config as hmda_config
from pipelines.extractors.hmda.client import HmdaClient
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.loaders.hmda_loader import load_hmda_modified_lar
from pipelines.storage.local_raw_store import write_raw_bytes
from pipelines.storage.manifest import write_manifest

SOURCE = "hmda"
DATASET = "modified_lar"


def _get_hmda_dataset() -> Any:
    preferred_names = (
        "HMDA_DATASET",
        "HMDA_MODIFIED_LAR",
        "HMDA_MODIFIED_LAR_DATASET",
        "HMDA_MODIFIED_LAR_2024",
    )

    for name in preferred_names:
        dataset = getattr(hmda_config, name, None)

        if dataset is not None:
            return dataset

    candidates = []

    for value in hmda_config.__dict__.values():
        if hasattr(value, "dataset") and hasattr(value, "filename"):
            candidates.append(value)

    if len(candidates) == 1:
        return candidates[0]

    if candidates:
        for candidate in candidates:
            if getattr(candidate, "dataset", "") in {"modified_lar", "lar"}:
                return candidate

        return candidates[0]

    raise RuntimeError(
        "Could not find HMDA dataset object in pipelines.extractors.hmda.config. "
        "Expected an object with at least dataset and filename attributes."
    )


def _dataset_year(dataset: Any) -> int:
    year = getattr(dataset, "year", None) or getattr(dataset, "activity_year", None)

    if year is None:
        raise RuntimeError("HMDA dataset must expose year or activity_year.")

    return int(year)


def _dataset_filename(dataset: Any) -> str:
    return str(getattr(dataset, "filename", f"hmda_modified_lar_{_dataset_year(dataset)}.csv"))


def _dataset_description(dataset: Any) -> str:
    return str(getattr(dataset, "description", "HMDA Modified Loan/Application Register"))


def _dataset_frequency(dataset: Any) -> str:
    return str(getattr(dataset, "expected_frequency", "annual"))


def _dataset_filters(dataset: Any) -> Any:
    return getattr(dataset, "filters", None)


def _source_url(client: HmdaClient, dataset: Any) -> str:
    if hasattr(client, "build_url"):
        return str(client.build_url(dataset))

    if hasattr(client, "base_url"):
        return str(client.base_url)

    return "https://ffiec.cfpb.gov/v2/data-browser-api"


def _get_dataset_content(client: HmdaClient, dataset: Any) -> bytes:
    method_names = (
        "get_dataset_content",
        "get_dataset",
        "fetch_dataset",
        "download_dataset",
        "get_modified_lar_csv",
        "get_modified_lar",
    )

    for method_name in method_names:
        method = getattr(client, method_name, None)

        if method is None:
            continue

        result = method(dataset)

        if isinstance(result, bytes):
            return result

        if isinstance(result, str):
            return result.encode("utf-8")

        if isinstance(result, dict):
            # Some clients return {"content": "..."} or {"data": "..."}.
            for key in ("content", "csv", "data"):
                value = result.get(key)

                if isinstance(value, bytes):
                    return value

                if isinstance(value, str):
                    return value.encode("utf-8")

    available = [
        name
        for name in dir(client)
        if not name.startswith("_") and callable(getattr(client, name))
    ]

    raise RuntimeError(
        "Could not fetch HMDA CSV content. "
        f"Available HmdaClient methods: {available}"
    )


def _inspect_csv(content: bytes) -> dict:
    text = content.decode("utf-8-sig", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]

    if not lines:
        return {"row_count": 0, "columns": []}

    columns = lines[0].split(",")

    return {
        "row_count": max(len(lines) - 1, 0),
        "columns": columns,
    }


def extract_dataset(
    client: HmdaClient,
    dataset: Any,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    content = _get_dataset_content(client, dataset)
    inspection = _inspect_csv(content)
    year = _dataset_year(dataset)

    raw_result = write_raw_bytes(
        source=SOURCE,
        dataset=DATASET,
        filename=_dataset_filename(dataset),
        content=content,
        load_date=load_date,
        overwrite=True,
    )

    source_url = _source_url(client, dataset)

    manifest_result = write_manifest(
        source=SOURCE,
        dataset=DATASET,
        raw_file_path=raw_result["raw_file_path"],
        status="success",
        load_date=load_date,
        source_url=source_url,
        file_format="csv",
        record_count=inspection["row_count"],
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        source_period_start=date(year, 1, 1).isoformat(),
        source_period_end=date(year, 12, 31).isoformat(),
        metadata={
            "description": _dataset_description(dataset),
            "expected_frequency": _dataset_frequency(dataset),
            "columns": inspection["columns"],
            "filters": _dataset_filters(dataset),
        },
    )

    source_file_id = record_source_file(
        pipeline_run_id=pipeline_run_id,
        source=SOURCE,
        dataset=DATASET,
        source_url=source_url,
        raw_file_path=raw_result["raw_file_path"],
        file_format="csv",
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        record_count=inspection["row_count"],
        source_period_start=date(year, 1, 1),
        source_period_end=date(year, 12, 31),
        load_date=date.fromisoformat(load_date),
        status="success",
        metadata={
            "description": _dataset_description(dataset),
            "expected_frequency": _dataset_frequency(dataset),
            "manifest_path": manifest_result["manifest_path"],
            "filters": _dataset_filters(dataset),
        },
    )

    loaded_count = load_hmda_modified_lar(
        content=content,
        source_file_id=source_file_id,
        load_date=date.fromisoformat(load_date),
    )

    print(
        f"Extracted HMDA {DATASET}: "
        f"{inspection['row_count']} source rows, "
        f"{loaded_count} DB rows -> {raw_result['raw_file_path']}"
    )

    return loaded_count


def main() -> None:
    load_date = today_iso()
    dataset = _get_hmda_dataset()
    year = _dataset_year(dataset)

    pipeline_run_id = start_pipeline_run(
        pipeline_name="hmda_modified_lar_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "year": year,
            "filename": _dataset_filename(dataset),
            "filters": _dataset_filters(dataset),
        },
    )

    total_loaded = 0

    try:
        client = HmdaClient()

        total_loaded = extract_dataset(
            client=client,
            dataset=dataset,
            pipeline_run_id=pipeline_run_id,
            load_date=load_date,
        )

        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="success",
            records_extracted=total_loaded,
            records_loaded=total_loaded,
            records_failed=0,
        )

        update_source_freshness(
            source=SOURCE,
            dataset=DATASET,
            latest_source_period=date(year, 12, 31),
            last_successful_run_id=pipeline_run_id,
            last_status="success",
            record_count=total_loaded,
        )

        print(f"HMDA extraction complete. Total loaded rows: {total_loaded}")

    except Exception as exc:
        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="failed",
            records_extracted=total_loaded,
            records_loaded=None,
            records_failed=None,
            error_message=str(exc),
        )

        update_source_freshness(
            source=SOURCE,
            dataset=DATASET,
            latest_source_period=None,
            last_successful_run_id=None,
            last_status="failed",
            record_count=total_loaded,
            error_message=str(exc),
        )

        raise


if __name__ == "__main__":
    main()

import csv
from datetime import date
from io import StringIO

from pipelines.common.time import today_iso
from pipelines.extractors.zillow.client import ZillowClient
from pipelines.extractors.zillow.config import ZILLOW_DATASETS, ZillowDataset
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.storage.local_raw_store import write_raw_bytes
from pipelines.storage.manifest import write_manifest

SOURCE = "zillow"


def _decode_csv(content: bytes) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("latin-1")


def _date_columns(fieldnames: list[str]) -> list[str]:
    date_cols = []

    for fieldname in fieldnames:
        try:
            date.fromisoformat(fieldname)
        except ValueError:
            continue
        else:
            date_cols.append(fieldname)

    return date_cols


def _inspect_zillow_csv(content: bytes) -> dict:
    text = _decode_csv(content)
    reader = csv.DictReader(StringIO(text))

    fieldnames = reader.fieldnames or []
    date_cols = _date_columns(fieldnames)

    row_count = 0
    region_types: set[str] = set()
    states: set[str] = set()

    for row in reader:
        row_count += 1

        region_type = row.get("RegionType")
        state_name = row.get("StateName") or row.get("State")

        if region_type:
            region_types.add(region_type)

        if state_name:
            states.add(state_name)

    source_period_start = date.fromisoformat(min(date_cols)) if date_cols else None
    source_period_end = date.fromisoformat(max(date_cols)) if date_cols else None

    return {
        "row_count": row_count,
        "source_period_start": source_period_start,
        "source_period_end": source_period_end,
        "date_column_count": len(date_cols),
        "region_types": sorted(region_types),
        "state_count": len(states),
        "columns": fieldnames,
    }


def extract_dataset(
    client: ZillowClient,
    dataset: ZillowDataset,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    content = client.get_dataset_content(dataset)
    inspection = _inspect_zillow_csv(content)

    raw_result = write_raw_bytes(
        source=SOURCE,
        dataset=dataset.dataset,
        filename=dataset.filename,
        content=content,
        load_date=load_date,
        overwrite=True,
    )

    source_url_or_path = dataset.url if dataset.url else dataset.local_path

    manifest_result = write_manifest(
        source=SOURCE,
        dataset=dataset.dataset,
        raw_file_path=raw_result["raw_file_path"],
        status="success",
        load_date=load_date,
        source_url=source_url_or_path,
        file_format="csv",
        record_count=inspection["row_count"],
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        source_period_start=(
            inspection["source_period_start"].isoformat()
            if inspection["source_period_start"]
            else None
        ),
        source_period_end=(
            inspection["source_period_end"].isoformat()
            if inspection["source_period_end"]
            else None
        ),
        metadata={
            "metric_name": dataset.metric_name,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "date_column_count": inspection["date_column_count"],
            "region_types": inspection["region_types"],
            "state_count": inspection["state_count"],
            "columns": inspection["columns"],
        },
    )

    record_source_file(
        pipeline_run_id=pipeline_run_id,
        source=SOURCE,
        dataset=dataset.dataset,
        source_url=source_url_or_path,
        raw_file_path=raw_result["raw_file_path"],
        file_format="csv",
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        record_count=inspection["row_count"],
        source_period_start=inspection["source_period_start"],
        source_period_end=inspection["source_period_end"],
        load_date=date.fromisoformat(load_date),
        status="success",
        metadata={
            "metric_name": dataset.metric_name,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "manifest_path": manifest_result["manifest_path"],
            "date_column_count": inspection["date_column_count"],
            "region_types": inspection["region_types"],
            "state_count": inspection["state_count"],
        },
    )

    update_source_freshness(
        source=SOURCE,
        dataset=dataset.dataset,
        latest_source_period=inspection["source_period_end"],
        last_successful_run_id=pipeline_run_id,
        last_status="success",
        record_count=inspection["row_count"],
    )

    print(
        f"Extracted Zillow {dataset.dataset}: "
        f"{inspection['row_count']} rows -> {raw_result['raw_file_path']}"
    )

    return inspection["row_count"]


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="zillow_research_extract",
        source=SOURCE,
        dataset="zhvi_zori",
        metadata={
            "datasets": [dataset.dataset for dataset in ZILLOW_DATASETS],
        },
    )

    total_rows = 0
    loaded_files = 0

    try:
        client = ZillowClient()

        for dataset in ZILLOW_DATASETS:
            total_rows += extract_dataset(
                client=client,
                dataset=dataset,
                pipeline_run_id=pipeline_run_id,
                load_date=load_date,
            )
            loaded_files += 1

        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="success",
            records_extracted=total_rows,
            records_loaded=loaded_files,
            records_failed=0,
        )

        print(f"Zillow extraction complete. Total rows: {total_rows}")

    except Exception as exc:
        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="failed",
            records_extracted=total_rows,
            records_loaded=loaded_files,
            records_failed=None,
            error_message=str(exc),
        )

        raise


if __name__ == "__main__":
    main()

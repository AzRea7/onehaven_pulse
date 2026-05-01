import json
from datetime import date

from pipelines.common.time import today_iso
from pipelines.extractors.hud_usps.client import HudUspsClient
from pipelines.extractors.hud_usps.config import HUD_USPS_DATASETS, HudUspsDataset
from pipelines.loaders.hud_usps_loader import load_hud_usps_crosswalk
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.storage.local_raw_store import write_raw_text
from pipelines.storage.manifest import write_manifest

SOURCE = "hud_usps"
DATASET = "zip_crosswalk"


def _source_period_bounds(year: int, quarter: int) -> tuple[date, date]:
    if quarter == 1:
        return date(year, 1, 1), date(year, 3, 31)

    if quarter == 2:
        return date(year, 4, 1), date(year, 6, 30)

    if quarter == 3:
        return date(year, 7, 1), date(year, 9, 30)

    if quarter == 4:
        return date(year, 10, 1), date(year, 12, 31)

    raise ValueError(f"Invalid HUD-USPS quarter={quarter}. Expected 1, 2, 3, or 4.")


def extract_dataset(
    client: HudUspsClient,
    dataset: HudUspsDataset,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    payload = client.get_dataset(dataset)
    result_count = client.get_result_count(payload)
    response_metadata = client.get_response_metadata(payload)

    raw_content = json.dumps(payload, indent=2, sort_keys=False)

    raw_result = write_raw_text(
        source=SOURCE,
        dataset=DATASET,
        filename=dataset.filename,
        content=raw_content,
        load_date=load_date,
        overwrite=True,
    )

    source_period_start, source_period_end = _source_period_bounds(
        year=dataset.year,
        quarter=dataset.quarter,
    )

    source_url = (
        f"{client.base_url}"
        f"?type={dataset.api_type}"
        f"&query={dataset.query}"
        f"&year={dataset.year}"
        f"&quarter={dataset.quarter}"
    )

    manifest_result = write_manifest(
        source=SOURCE,
        dataset=DATASET,
        raw_file_path=raw_result["raw_file_path"],
        status="success",
        load_date=load_date,
        source_url=source_url,
        file_format="json",
        record_count=result_count,
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        source_period_start=source_period_start.isoformat(),
        source_period_end=source_period_end.isoformat(),
        metadata={
            "crosswalk_type": dataset.crosswalk_type,
            "api_type": dataset.api_type,
            "query": dataset.query,
            "year": dataset.year,
            "quarter": dataset.quarter,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "response_metadata": response_metadata,
        },
    )

    source_file_id = record_source_file(
        pipeline_run_id=pipeline_run_id,
        source=SOURCE,
        dataset=DATASET,
        source_url=source_url,
        raw_file_path=raw_result["raw_file_path"],
        file_format="json",
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        record_count=result_count,
        source_period_start=source_period_start,
        source_period_end=source_period_end,
        load_date=date.fromisoformat(load_date),
        status="success",
        metadata={
            "crosswalk_type": dataset.crosswalk_type,
            "api_type": dataset.api_type,
            "query": dataset.query,
            "year": dataset.year,
            "quarter": dataset.quarter,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "manifest_path": manifest_result["manifest_path"],
            "response_metadata": response_metadata,
        },
    )

    loaded_count = load_hud_usps_crosswalk(
        payload=payload,
        dataset=dataset,
        source_file_id=source_file_id,
        load_date=date.fromisoformat(load_date),
    )

    print(
        f"Extracted HUD-USPS {dataset.crosswalk_type}: "
        f"{result_count} source rows, {loaded_count} DB rows -> {raw_result['raw_file_path']}"
    )

    return loaded_count


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="hud_usps_zip_crosswalk_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "datasets": [
                {
                    "crosswalk_type": dataset.crosswalk_type,
                    "api_type": dataset.api_type,
                    "query": dataset.query,
                    "year": dataset.year,
                    "quarter": dataset.quarter,
                    "filename": dataset.filename,
                }
                for dataset in HUD_USPS_DATASETS
            ],
        },
    )

    total_records = 0
    loaded_files = 0
    source_period_ends: list[date] = []

    try:
        client = HudUspsClient()

        for dataset in HUD_USPS_DATASETS:
            total_records += extract_dataset(
                client=client,
                dataset=dataset,
                pipeline_run_id=pipeline_run_id,
                load_date=load_date,
            )
            loaded_files += 1

            _, source_period_end = _source_period_bounds(
                year=dataset.year,
                quarter=dataset.quarter,
            )
            source_period_ends.append(source_period_end)

        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="success",
            records_extracted=total_records,
            records_loaded=loaded_files,
            records_failed=0,
        )

        update_source_freshness(
            source=SOURCE,
            dataset=DATASET,
            latest_source_period=max(source_period_ends) if source_period_ends else None,
            last_successful_run_id=pipeline_run_id,
            last_status="success",
            record_count=total_records,
        )

        print(
            f"HUD-USPS ZIP Crosswalk extraction complete. "
            f"Files loaded: {loaded_files}. Records: {total_records}"
        )

    except Exception as exc:
        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="failed",
            records_extracted=total_records,
            records_loaded=loaded_files,
            records_failed=None,
            error_message=str(exc),
        )

        update_source_freshness(
            source=SOURCE,
            dataset=DATASET,
            latest_source_period=None,
            last_successful_run_id=None,
            last_status="failed",
            record_count=total_records,
            error_message=str(exc),
        )

        raise


if __name__ == "__main__":
    main()

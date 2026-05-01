from datetime import date

from pipelines.common.time import today_iso
from pipelines.extractors.census_building_permits.client import CensusBpsClient
from pipelines.extractors.census_building_permits.config import (
    CENSUS_BPS_DATASETS,
    CensusBpsDataset,
)
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.loaders.census_bps_loader import load_census_bps_permits
from pipelines.storage.local_raw_store import write_raw_bytes
from pipelines.storage.manifest import write_manifest

SOURCE = "census"
DATASET = "building_permits"


def _source_period_bounds(dataset: CensusBpsDataset) -> tuple[date | None, date | None]:
    if dataset.period_type == "monthly" and "-" in dataset.source_period_label:
        year_raw, month_raw = dataset.source_period_label.split("-", maxsplit=1)
        year = int(year_raw)
        month = int(month_raw)

        if month == 12:
            return date(year, 12, 1), date(year, 12, 31)

        return date(year, month, 1), date(year, month + 1, 1)

    if dataset.period_type == "annual":
        year = int(dataset.source_period_label)
        return date(year, 1, 1), date(year, 12, 31)

    return None, None


def extract_dataset(
    client: CensusBpsClient,
    dataset: CensusBpsDataset,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    content = client.get_dataset_content(dataset)

    if content is None:
        print(
            f"Skipped optional Census BPS {dataset.geography_level}/{dataset.period_type}: "
            "no content configured."
        )
        return 0

    source_period_start, source_period_end = _source_period_bounds(dataset)

    raw_result = write_raw_bytes(
        source=SOURCE,
        dataset=DATASET,
        filename=dataset.filename,
        content=content,
        load_date=load_date,
        overwrite=True,
    )

    source_url_or_path = dataset.url if dataset.url else dataset.local_path

    manifest_result = write_manifest(
        source=SOURCE,
        dataset=DATASET,
        raw_file_path=raw_result["raw_file_path"],
        status="success",
        load_date=load_date,
        source_url=source_url_or_path,
        file_format=dataset.filename.split(".")[-1],
        record_count=None,
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        source_period_start=source_period_start.isoformat() if source_period_start else None,
        source_period_end=source_period_end.isoformat() if source_period_end else None,
        metadata={
            "geography_level": dataset.geography_level,
            "period_type": dataset.period_type,
            "source_period_label": dataset.source_period_label,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "release_cadence_note": dataset.release_cadence_note,
        },
    )

    source_file_id = record_source_file(
        pipeline_run_id=pipeline_run_id,
        source=SOURCE,
        dataset=DATASET,
        source_url=source_url_or_path,
        raw_file_path=raw_result["raw_file_path"],
        file_format=dataset.filename.split(".")[-1],
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        record_count=None,
        source_period_start=source_period_start,
        source_period_end=source_period_end,
        load_date=date.fromisoformat(load_date),
        status="success",
        metadata={
            "geography_level": dataset.geography_level,
            "period_type": dataset.period_type,
            "source_period_label": dataset.source_period_label,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "release_cadence_note": dataset.release_cadence_note,
            "manifest_path": manifest_result["manifest_path"],
        },
    )

    loaded_count = load_census_bps_permits(
        content=content,
        dataset=dataset,
        source_file_id=source_file_id,
        load_date=date.fromisoformat(load_date),
    )

    print(
        f"Extracted Census BPS {dataset.geography_level}/{dataset.period_type}: "
        f"{loaded_count} DB rows -> {raw_result['raw_file_path']}"
    )

    return loaded_count


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="census_building_permits_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "datasets": [
                {
                    "geography_level": dataset.geography_level,
                    "period_type": dataset.period_type,
                    "source_period_label": dataset.source_period_label,
                    "filename": dataset.filename,
                    "required": dataset.required,
                }
                for dataset in CENSUS_BPS_DATASETS
            ],
        },
    )

    total_loaded = 0

    try:
        client = CensusBpsClient()

        for dataset in CENSUS_BPS_DATASETS:
            total_loaded += extract_dataset(
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
            latest_source_period=None,
            last_successful_run_id=pipeline_run_id,
            last_status="success",
            record_count=total_loaded,
        )

        print(f"Census BPS extraction complete. Total loaded rows: {total_loaded}")

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

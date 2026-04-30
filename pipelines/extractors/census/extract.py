from datetime import date

from pipelines.common.time import today_iso
from pipelines.extractors.census.client import CensusClient
from pipelines.extractors.census.config import CENSUS_GEOGRAPHY_DATASETS, CensusGeographyDataset
from pipelines.extractors.census.loader import load_geography_to_postgis
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.storage.local_raw_store import write_raw_bytes
from pipelines.storage.manifest import write_manifest

SOURCE = "census"
DATASET = "geography"


def extract_dataset(
    client: CensusClient,
    dataset: CensusGeographyDataset,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    content = client.download_dataset(dataset)

    raw_result = write_raw_bytes(
        source=SOURCE,
        dataset=DATASET,
        filename=dataset.filename,
        content=content,
        load_date=load_date,
        overwrite=True,
    )

    loaded_row_count = load_geography_to_postgis(
        dataset=dataset,
        raw_file_path=raw_result["raw_file_path"],
    )

    manifest_result = write_manifest(
        source=SOURCE,
        dataset=DATASET,
        raw_file_path=raw_result["raw_file_path"],
        status="success",
        load_date=load_date,
        source_url=dataset.url,
        file_format="zip",
        record_count=loaded_row_count,
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        source_period_start=f"{dataset.geometry_year}-01-01",
        source_period_end=f"{dataset.geometry_year}-12-31",
        metadata={
            "census_dataset": dataset.dataset,
            "geo_type": dataset.geo_type,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "geometry_source": dataset.geometry_source,
            "geometry_year": dataset.geometry_year,
        },
    )

    record_source_file(
        pipeline_run_id=pipeline_run_id,
        source=SOURCE,
        dataset=DATASET,
        source_url=dataset.url,
        raw_file_path=raw_result["raw_file_path"],
        file_format="zip",
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        record_count=loaded_row_count,
        source_period_start=date(dataset.geometry_year, 1, 1),
        source_period_end=date(dataset.geometry_year, 12, 31),
        load_date=date.fromisoformat(load_date),
        status="success",
        metadata={
            "census_dataset": dataset.dataset,
            "geo_type": dataset.geo_type,
            "description": dataset.description,
            "manifest_path": manifest_result["manifest_path"],
            "geometry_source": dataset.geometry_source,
            "geometry_year": dataset.geometry_year,
        },
    )

    print(
        f"Extracted Census {dataset.dataset}: "
        f"{loaded_row_count} {dataset.geo_type} rows -> {raw_result['raw_file_path']}"
    )

    return loaded_row_count


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="census_geography_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "datasets": [dataset.dataset for dataset in CENSUS_GEOGRAPHY_DATASETS],
        },
    )

    total_rows = 0
    loaded_files = 0

    try:
        client = CensusClient()

        for dataset in CENSUS_GEOGRAPHY_DATASETS:
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

        update_source_freshness(
            source=SOURCE,
            dataset=DATASET,
            latest_source_period=max(
                date(dataset.geometry_year, 12, 31)
                for dataset in CENSUS_GEOGRAPHY_DATASETS
            ),
            last_successful_run_id=pipeline_run_id,
            last_status="success",
            record_count=total_rows,
        )

        print(f"Census geography extraction complete. Total rows loaded: {total_rows}")

    except Exception as exc:
        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="failed",
            records_extracted=total_rows,
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
            record_count=total_rows,
            error_message=str(exc),
        )

        raise


if __name__ == "__main__":
    main()

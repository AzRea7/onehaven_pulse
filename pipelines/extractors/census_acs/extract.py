import json
from datetime import date

from pipelines.common.settings import settings
from pipelines.common.time import today_iso
from pipelines.extractors.census_acs.client import CensusAcsClient
from pipelines.extractors.census_acs.config import CENSUS_ACS_DATASETS, CensusAcsDataset
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.storage.local_raw_store import write_raw_text
from pipelines.storage.manifest import write_manifest

SOURCE = "census_acs"
DATASET = "profile"


def _api_key_configured() -> bool:
    return bool(
        settings.census_data_api_key
        and not settings.census_data_api_key.startswith("replace_with_")
    )


def extract_dataset(
    client: CensusAcsClient,
    dataset: CensusAcsDataset,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    payload = client.get_dataset(dataset)
    headers = payload[0]
    rows = payload[1:]

    raw_content = json.dumps(payload, indent=2, sort_keys=False)

    raw_result = write_raw_text(
        source=SOURCE,
        dataset=DATASET,
        filename=dataset.filename,
        content=raw_content,
        load_date=load_date,
        overwrite=True,
    )

    source_url = (
        f"{client.base_url.rstrip('/')}/{dataset.endpoint_path}"
        f"?get={','.join(dataset.variables)}"
    )

    source_period_start = date(dataset.year - 4, 1, 1)
    source_period_end = date(dataset.year, 12, 31)

    manifest_result = write_manifest(
        source=SOURCE,
        dataset=DATASET,
        raw_file_path=raw_result["raw_file_path"],
        status="success",
        load_date=load_date,
        source_url=source_url,
        file_format="json",
        record_count=len(rows),
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        source_period_start=source_period_start.isoformat(),
        source_period_end=source_period_end.isoformat(),
        metadata={
            "geography_level": dataset.geography_level,
            "year": dataset.year,
            "endpoint_path": dataset.endpoint_path,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "variables": dataset.variables,
            "headers": headers,
            "params": dataset.params,
            "api_key_configured": _api_key_configured(),
        },
    )

    record_source_file(
        pipeline_run_id=pipeline_run_id,
        source=SOURCE,
        dataset=DATASET,
        source_url=source_url,
        raw_file_path=raw_result["raw_file_path"],
        file_format="json",
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        record_count=len(rows),
        source_period_start=source_period_start,
        source_period_end=source_period_end,
        load_date=date.fromisoformat(load_date),
        status="success",
        metadata={
            "geography_level": dataset.geography_level,
            "year": dataset.year,
            "endpoint_path": dataset.endpoint_path,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "variables": dataset.variables,
            "manifest_path": manifest_result["manifest_path"],
            "api_key_configured": _api_key_configured(),
        },
    )

    print(
        f"Extracted Census ACS {dataset.geography_level}: "
        f"{len(rows)} rows -> {raw_result['raw_file_path']}"
    )

    return len(rows)


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="census_acs_profile_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "api_key_configured": _api_key_configured(),
            "datasets": [
                {
                    "geography_level": dataset.geography_level,
                    "year": dataset.year,
                    "filename": dataset.filename,
                }
                for dataset in CENSUS_ACS_DATASETS
            ],
        },
    )

    total_rows = 0
    loaded_files = 0
    latest_source_period = max(date(dataset.year, 12, 31) for dataset in CENSUS_ACS_DATASETS)

    try:
        client = CensusAcsClient()

        for dataset in CENSUS_ACS_DATASETS:
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
            latest_source_period=latest_source_period,
            last_successful_run_id=pipeline_run_id,
            last_status="success",
            record_count=total_rows,
        )

        print(f"Census ACS extraction complete. Total rows: {total_rows}")

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

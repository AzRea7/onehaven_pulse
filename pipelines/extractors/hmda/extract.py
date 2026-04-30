import csv
from datetime import date
from io import StringIO

from pipelines.common.time import today_iso
from pipelines.extractors.hmda.client import HmdaClient
from pipelines.extractors.hmda.config import HMDA_MODIFIED_LAR, HmdaDataset
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.storage.local_raw_store import write_raw_bytes
from pipelines.storage.manifest import write_manifest


SOURCE = "hmda"
DATASET = "modified_lar"


def _decode_text(content: bytes) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("latin-1")


def _inspect_csv(content: bytes) -> dict:
    text = _decode_text(content)
    reader = csv.reader(StringIO(text))

    row_count = 0
    header: list[str] = []

    for row in reader:
        if row_count == 0:
            header = row
        row_count += 1

    normalized_header = [value.strip().lower() for value in header]
    likely_has_header = any(
        token in normalized_header
        for token in [
            "activity_year",
            "lei",
            "state_code",
            "county_code",
            "census_tract",
            "loan_amount",
            "action_taken",
        ]
    )

    record_count = row_count - 1 if likely_has_header and row_count > 0 else row_count

    return {
        "row_count": record_count,
        "columns": header if likely_has_header else [],
        "likely_has_header": likely_has_header,
    }


def _source_period_bounds(year: int) -> tuple[date, date]:
    return date(year, 1, 1), date(year, 12, 31)


def extract_dataset(
    client: HmdaClient,
    dataset: HmdaDataset,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    content = client.get_modified_lar_csv(dataset)
    inspection = _inspect_csv(content)

    raw_result = write_raw_bytes(
        source=SOURCE,
        dataset=DATASET,
        filename=dataset.filename,
        content=content,
        load_date=load_date,
        overwrite=True,
    )

    source_period_start, source_period_end = _source_period_bounds(dataset.year)

    source_params = client._build_csv_params(dataset)
    source_url = (
        f"{client.base_url.rstrip('/')}/csv?"
        + "&".join(f"{key}={value}" for key, value in source_params.items())
    )

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
        source_period_start=source_period_start.isoformat(),
        source_period_end=source_period_end.isoformat(),
        metadata={
            "year": dataset.year,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "geography_type": dataset.geography_type,
            "geography_values": dataset.geography_values,
            "actions_taken": dataset.actions_taken,
            "loan_purposes": dataset.loan_purposes,
            "loan_types": dataset.loan_types,
            "lien_statuses": dataset.lien_statuses,
            "columns": inspection["columns"],
            "likely_has_header": inspection["likely_has_header"],
            "api_key_required": False,
            "privacy_note": "HMDA public data are modified to protect applicant and borrower privacy.",
        },
    )

    record_source_file(
        pipeline_run_id=pipeline_run_id,
        source=SOURCE,
        dataset=DATASET,
        source_url=source_url,
        raw_file_path=raw_result["raw_file_path"],
        file_format="csv",
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        record_count=inspection["row_count"],
        source_period_start=source_period_start,
        source_period_end=source_period_end,
        load_date=date.fromisoformat(load_date),
        status="success",
        metadata={
            "year": dataset.year,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "geography_type": dataset.geography_type,
            "geography_values": dataset.geography_values,
            "actions_taken": dataset.actions_taken,
            "loan_purposes": dataset.loan_purposes,
            "loan_types": dataset.loan_types,
            "lien_statuses": dataset.lien_statuses,
            "manifest_path": manifest_result["manifest_path"],
            "columns": inspection["columns"],
            "likely_has_header": inspection["likely_has_header"],
            "api_key_required": False,
            "privacy_note": "HMDA public data are modified to protect applicant and borrower privacy.",
        },
    )

    print(
        f"Extracted HMDA modified_lar: "
        f"{inspection['row_count']} rows -> {raw_result['raw_file_path']}"
    )

    return inspection["row_count"] or 1


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="hmda_modified_lar_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "year": HMDA_MODIFIED_LAR.year,
            "geography_type": HMDA_MODIFIED_LAR.geography_type,
            "geography_values": HMDA_MODIFIED_LAR.geography_values,
            "api_key_required": False,
        },
    )

    try:
        client = HmdaClient()
        record_count = extract_dataset(
            client=client,
            dataset=HMDA_MODIFIED_LAR,
            pipeline_run_id=pipeline_run_id,
            load_date=load_date,
        )

        _, source_period_end = _source_period_bounds(HMDA_MODIFIED_LAR.year)

        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="success",
            records_extracted=record_count,
            records_loaded=1,
            records_failed=0,
        )

        update_source_freshness(
            source=SOURCE,
            dataset=DATASET,
            latest_source_period=source_period_end,
            last_successful_run_id=pipeline_run_id,
            last_status="success",
            record_count=record_count,
        )

        print(f"HMDA Modified LAR extraction complete. Records: {record_count}")

    except Exception as exc:
        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="failed",
            records_extracted=None,
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
            record_count=None,
            error_message=str(exc),
        )

        raise


if __name__ == "__main__":
    main()

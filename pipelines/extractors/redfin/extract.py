import csv
from datetime import date, datetime
from io import StringIO

from pipelines.common.time import today_iso
from pipelines.extractors.redfin.client import RedfinClient
from pipelines.extractors.redfin.config import REDFIN_MARKET_TRACKER, RedfinDataset
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.storage.local_raw_store import write_raw_bytes
from pipelines.storage.manifest import write_manifest


SOURCE = "redfin"


def _decode_csv(content: bytes) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("latin-1")


def _normalize_column_name(value: str) -> str:
    return value.strip().lower().replace(" ", "_").replace("-", "_")


def _parse_date_value(value: str | None) -> date | None:
    if not value:
        return None

    clean_value = value.strip()

    if not clean_value:
        return None

    # ISO day format: 2026-03-31
    try:
        return date.fromisoformat(clean_value)
    except ValueError:
        pass

    # ISO month format: 2026-03
    if len(clean_value) == 7 and clean_value[4] == "-":
        try:
            return date.fromisoformat(f"{clean_value}-01")
        except ValueError:
            pass

    # Common US formats
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(clean_value, fmt).date()
        except ValueError:
            continue

    return None


def _detect_date_columns(fieldnames: list[str]) -> list[str]:
    normalized_lookup = {
        fieldname: _normalize_column_name(fieldname)
        for fieldname in fieldnames
        if fieldname
    }

    candidates = {
        "period_begin",
        "period_end",
        "month",
        "date",
        "period",
        "sold_month",
        "report_month",
    }

    return [
        original
        for original, normalized in normalized_lookup.items()
        if normalized in candidates
    ]


def _inspect_redfin_csv(content: bytes) -> dict:
    text = _decode_csv(content)
    reader = csv.DictReader(StringIO(text))

    fieldnames = reader.fieldnames or []
    date_columns = _detect_date_columns(fieldnames)

    row_count = 0
    region_types: set[str] = set()
    property_types: set[str] = set()
    states: set[str] = set()
    regions: set[str] = set()
    parsed_dates: list[date] = []

    for row in reader:
        row_count += 1

        normalized_row = {
            _normalize_column_name(key): value
            for key, value in row.items()
            if key
        }

        region_type = normalized_row.get("region_type") or normalized_row.get("regiontype")
        property_type = normalized_row.get("property_type") or normalized_row.get("propertytype")
        state = normalized_row.get("state") or normalized_row.get("state_code")
        region = (
            normalized_row.get("region")
            or normalized_row.get("region_name")
            or normalized_row.get("regionname")
        )

        if region_type:
            region_types.add(region_type)

        if property_type:
            property_types.add(property_type)

        if state:
            states.add(state)

        if region:
            regions.add(region)

        for date_column in date_columns:
            parsed = _parse_date_value(row.get(date_column))
            if parsed:
                parsed_dates.append(parsed)

    source_period_start = min(parsed_dates) if parsed_dates else None
    source_period_end = max(parsed_dates) if parsed_dates else None

    return {
        "row_count": row_count,
        "source_period_start": source_period_start,
        "source_period_end": source_period_end,
        "date_columns": date_columns,
        "region_types": sorted(region_types),
        "property_types": sorted(property_types),
        "state_count": len(states),
        "region_count": len(regions),
        "columns": fieldnames,
    }


def extract_dataset(
    client: RedfinClient,
    dataset: RedfinDataset,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    content = client.get_dataset_content(dataset)
    inspection = _inspect_redfin_csv(content)

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
            "release_cadence_note": dataset.release_cadence_note,
            "date_columns": inspection["date_columns"],
            "region_types": inspection["region_types"],
            "property_types": inspection["property_types"],
            "state_count": inspection["state_count"],
            "region_count": inspection["region_count"],
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
            "release_cadence_note": dataset.release_cadence_note,
            "manifest_path": manifest_result["manifest_path"],
            "date_columns": inspection["date_columns"],
            "region_types": inspection["region_types"],
            "property_types": inspection["property_types"],
            "state_count": inspection["state_count"],
            "region_count": inspection["region_count"],
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
        f"Extracted Redfin {dataset.dataset}: "
        f"{inspection['row_count']} rows -> {raw_result['raw_file_path']}"
    )

    return inspection["row_count"]


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="redfin_market_tracker_extract",
        source=SOURCE,
        dataset=REDFIN_MARKET_TRACKER.dataset,
        metadata={
            "dataset": REDFIN_MARKET_TRACKER.dataset,
            "filename": REDFIN_MARKET_TRACKER.filename,
            "release_cadence_note": REDFIN_MARKET_TRACKER.release_cadence_note,
        },
    )

    total_rows = 0
    loaded_files = 0

    try:
        client = RedfinClient()

        total_rows += extract_dataset(
            client=client,
            dataset=REDFIN_MARKET_TRACKER,
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

        print(f"Redfin extraction complete. Total rows: {total_rows}")

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
            dataset=REDFIN_MARKET_TRACKER.dataset,
            latest_source_period=None,
            last_successful_run_id=None,
            last_status="failed",
            record_count=total_rows,
            error_message=str(exc),
        )

        raise


if __name__ == "__main__":
    main()

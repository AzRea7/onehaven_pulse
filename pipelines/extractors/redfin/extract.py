import csv
from datetime import date
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
from pipelines.loaders.redfin_loader import load_redfin_market_tracker
from pipelines.storage.local_raw_store import write_raw_bytes
from pipelines.storage.manifest import write_manifest

SOURCE = "redfin"


def _decode_csv(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue

    return content.decode("latin-1", errors="replace")


def _detect_delimiter(text: str) -> str:
    first_line = text.splitlines()[0] if text.splitlines() else ""

    if "\t" in first_line:
        return "\t"

    return ","


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None

    raw = value.strip()

    try:
        parsed = date.fromisoformat(raw[:10])
        return date(parsed.year, parsed.month, 1)
    except ValueError:
        return None


def _inspect_redfin_csv(content: bytes) -> dict:
    text = _decode_csv(content)
    reader = csv.DictReader(StringIO(text), delimiter=_detect_delimiter(text))

    fieldnames = reader.fieldnames or []
    row_count = 0
    region_types: set[str] = set()
    property_types: set[str] = set()
    states: set[str] = set()
    source_periods: list[date] = []

    for row in reader:
        row_count += 1

        region_type = row.get("region_type") or row.get("Region Type")
        property_type = row.get("property_type") or row.get("Property Type")
        state_code = row.get("state_code") or row.get("State Code") or row.get("state")
        period_begin = row.get("period_begin") or row.get("Period Begin")
        period_end = row.get("period_end") or row.get("Period End")

        if region_type:
            region_types.add(region_type)

        if property_type:
            property_types.add(property_type)

        if state_code:
            states.add(state_code)

        parsed_period = _parse_date(period_begin) or _parse_date(period_end)

        if parsed_period:
            source_periods.append(parsed_period)

    return {
        "row_count": row_count,
        "source_period_start": min(source_periods) if source_periods else None,
        "source_period_end": max(source_periods) if source_periods else None,
        "region_types": sorted(region_types),
        "property_types": sorted(property_types),
        "state_count": len(states),
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
        source_period_start=inspection["source_period_start"].isoformat()
        if inspection["source_period_start"]
        else None,
        source_period_end=inspection["source_period_end"].isoformat()
        if inspection["source_period_end"]
        else None,
        metadata={
            "metric_name": dataset.metric_name,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "release_cadence_note": dataset.release_cadence_note,
            "region_types": inspection["region_types"],
            "property_types": inspection["property_types"],
            "state_count": inspection["state_count"],
            "columns": inspection["columns"],
        },
    )

    source_file_id = record_source_file(
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
            "region_types": inspection["region_types"],
            "property_types": inspection["property_types"],
            "state_count": inspection["state_count"],
        },
    )

    loaded_count = load_redfin_market_tracker(
        content=content,
        source_file_id=source_file_id,
        load_date=date.fromisoformat(load_date),
    )

    update_source_freshness(
        source=SOURCE,
        dataset=dataset.dataset,
        latest_source_period=inspection["source_period_end"],
        last_successful_run_id=pipeline_run_id,
        last_status="success",
        record_count=loaded_count,
    )

    print(
        f"Extracted Redfin {dataset.dataset}: "
        f"{inspection['row_count']} source rows, "
        f"{loaded_count} DB rows -> {raw_result['raw_file_path']}"
    )

    return loaded_count


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

    total_loaded = 0

    try:
        client = RedfinClient()

        total_loaded = extract_dataset(
            client=client,
            dataset=REDFIN_MARKET_TRACKER,
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

        print(f"Redfin extraction complete. Total loaded rows: {total_loaded}")

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
            dataset=REDFIN_MARKET_TRACKER.dataset,
            latest_source_period=None,
            last_successful_run_id=None,
            last_status="failed",
            record_count=total_loaded,
            error_message=str(exc),
        )

        raise


if __name__ == "__main__":
    main()

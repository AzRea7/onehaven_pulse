import csv
from datetime import date
from io import StringIO

from pipelines.common.time import today_iso
from pipelines.extractors.fhfa.client import FhfaClient
from pipelines.extractors.fhfa.config import FHFA_HPI_MASTER
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.loaders.fhfa_loader import load_fhfa_hpi_records
from pipelines.storage.local_raw_store import write_raw_bytes
from pipelines.storage.manifest import write_manifest

SOURCE = "fhfa"
DATASET = "hpi"


def _decode_csv(content: bytes) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("latin-1")


def _period_date(year_raw: str | None, period_raw: str | None) -> date | None:
    if not year_raw or not period_raw:
        return None

    try:
        year = int(year_raw)
        month = int(period_raw)
    except ValueError:
        return None

    if month < 1 or month > 12:
        return None

    return date(year, month, 1)


def _parse_hpi_master_csv(content: bytes) -> tuple[list[dict], dict]:
    text = _decode_csv(content)
    reader = csv.DictReader(StringIO(text))

    rows: list[dict] = []
    source_periods: list[date] = []
    levels: set[str] = set()
    frequencies: set[str] = set()
    hpi_types: set[str] = set()
    hpi_flavors: set[str] = set()

    for row in reader:
        rows.append(row)

        level = row.get("level")
        frequency = row.get("frequency")
        hpi_type = row.get("hpi_type")
        hpi_flavor = row.get("hpi_flavor")

        if level:
            levels.add(level)

        if frequency:
            frequencies.add(frequency)

        if hpi_type:
            hpi_types.add(hpi_type)

        if hpi_flavor:
            hpi_flavors.add(hpi_flavor)

        parsed_period = _period_date(row.get("yr"), row.get("period"))

        if parsed_period:
            source_periods.append(parsed_period)

    metadata = {
        "row_count": len(rows),
        "levels": sorted(levels),
        "frequencies": sorted(frequencies),
        "hpi_types": sorted(hpi_types),
        "hpi_flavors": sorted(hpi_flavors),
        "source_period_start": min(source_periods) if source_periods else None,
        "source_period_end": max(source_periods) if source_periods else None,
    }

    return rows, metadata


def extract_hpi_master(
    *,
    client: FhfaClient,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    content = client.download_dataset(FHFA_HPI_MASTER)
    rows, metadata = _parse_hpi_master_csv(content)

    source_period_start = metadata["source_period_start"]
    source_period_end = metadata["source_period_end"]

    raw_result = write_raw_bytes(
        source=SOURCE,
        dataset=DATASET,
        filename=FHFA_HPI_MASTER.filename,
        content=content,
        load_date=load_date,
        overwrite=True,
    )

    manifest_result = write_manifest(
        source=SOURCE,
        dataset=DATASET,
        raw_file_path=raw_result["raw_file_path"],
        status="success",
        load_date=load_date,
        source_url=FHFA_HPI_MASTER.url,
        file_format="csv",
        record_count=len(rows),
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        source_period_start=source_period_start.isoformat() if source_period_start else None,
        source_period_end=source_period_end.isoformat() if source_period_end else None,
        metadata={
            "dataset": FHFA_HPI_MASTER.dataset,
            "description": FHFA_HPI_MASTER.description,
            "expected_frequency": FHFA_HPI_MASTER.expected_frequency,
            "levels": metadata["levels"],
            "frequencies": metadata["frequencies"],
            "hpi_types": metadata["hpi_types"],
            "hpi_flavors": metadata["hpi_flavors"],
        },
    )

    source_file_id = record_source_file(
        pipeline_run_id=pipeline_run_id,
        source=SOURCE,
        dataset=DATASET,
        source_url=FHFA_HPI_MASTER.url,
        raw_file_path=raw_result["raw_file_path"],
        file_format="csv",
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        record_count=len(rows),
        source_period_start=source_period_start,
        source_period_end=source_period_end,
        load_date=date.fromisoformat(load_date),
        status="success",
        metadata={
            "dataset": FHFA_HPI_MASTER.dataset,
            "description": FHFA_HPI_MASTER.description,
            "expected_frequency": FHFA_HPI_MASTER.expected_frequency,
            "manifest_path": manifest_result["manifest_path"],
            "levels": metadata["levels"],
            "frequencies": metadata["frequencies"],
            "hpi_types": metadata["hpi_types"],
            "hpi_flavors": metadata["hpi_flavors"],
        },
    )

    loaded_count = load_fhfa_hpi_records(
        records=rows,
        source_file_id=source_file_id,
        load_date=date.fromisoformat(load_date),
    )

    print(
        f"Extracted FHFA HPI master: "
        f"{len(rows)} source rows, {loaded_count} DB rows "
        f"-> {raw_result['raw_file_path']}"
    )

    return loaded_count


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="fhfa_hpi_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "dataset": FHFA_HPI_MASTER.dataset,
            "url": FHFA_HPI_MASTER.url,
            "expected_frequency": FHFA_HPI_MASTER.expected_frequency,
        },
    )

    total_loaded = 0

    try:
        client = FhfaClient()

        total_loaded = extract_hpi_master(
            client=client,
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

        print(f"FHFA extraction complete. Total loaded records: {total_loaded}")

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

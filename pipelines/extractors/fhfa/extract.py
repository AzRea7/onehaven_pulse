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
from pipelines.storage.local_raw_store import write_raw_bytes
from pipelines.storage.manifest import write_manifest

SOURCE = "fhfa"
DATASET = "hpi"


def _decode_csv(content: bytes) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("latin-1")


def _inspect_hpi_master_csv(content: bytes) -> dict:
    text = _decode_csv(content)
    reader = csv.DictReader(StringIO(text))

    row_count = 0
    latest_year: int | None = None
    latest_period: int | None = None
    earliest_year: int | None = None
    earliest_period: int | None = None
    levels: set[str] = set()
    frequencies: set[str] = set()
    hpi_types: set[str] = set()
    hpi_flavors: set[str] = set()

    for row in reader:
        row_count += 1

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

        year_raw = row.get("yr")
        period_raw = row.get("period")

        if not year_raw or not period_raw:
            continue

        try:
            year = int(year_raw)
            period = int(period_raw)
        except ValueError:
            continue

        if earliest_year is None or (year, period) < (earliest_year, earliest_period or 0):
            earliest_year = year
            earliest_period = period

        if latest_year is None or (year, period) > (latest_year, latest_period or 0):
            latest_year = year
            latest_period = period

    source_period_start = date(earliest_year, 1, 1) if earliest_year else None
    source_period_end = date(latest_year, 12, 31) if latest_year else None

    return {
        "row_count": row_count,
        "source_period_start": source_period_start,
        "source_period_end": source_period_end,
        "levels": sorted(levels),
        "frequencies": sorted(frequencies),
        "hpi_types": sorted(hpi_types),
        "hpi_flavors": sorted(hpi_flavors),
    }


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="fhfa_hpi_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "source_url": FHFA_HPI_MASTER.url,
            "filename": FHFA_HPI_MASTER.filename,
            "description": FHFA_HPI_MASTER.description,
        },
    )

    try:
        client = FhfaClient()
        content = client.download_dataset(FHFA_HPI_MASTER)
        inspection = _inspect_hpi_master_csv(content)

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
                "filename": FHFA_HPI_MASTER.filename,
                "description": FHFA_HPI_MASTER.description,
                "levels": inspection["levels"],
                "frequencies": inspection["frequencies"],
                "hpi_types": inspection["hpi_types"],
                "hpi_flavors": inspection["hpi_flavors"],
            },
        )

        record_source_file(
            pipeline_run_id=pipeline_run_id,
            source=SOURCE,
            dataset=DATASET,
            source_url=FHFA_HPI_MASTER.url,
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
                "filename": FHFA_HPI_MASTER.filename,
                "description": FHFA_HPI_MASTER.description,
                "manifest_path": manifest_result["manifest_path"],
                "levels": inspection["levels"],
                "frequencies": inspection["frequencies"],
                "hpi_types": inspection["hpi_types"],
                "hpi_flavors": inspection["hpi_flavors"],
            },
        )

        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="success",
            records_extracted=inspection["row_count"],
            records_loaded=1,
            records_failed=0,
        )

        update_source_freshness(
            source=SOURCE,
            dataset=DATASET,
            latest_source_period=inspection["source_period_end"],
            last_successful_run_id=pipeline_run_id,
            last_status="success",
            record_count=inspection["row_count"],
        )

        print(
            f"FHFA HPI extraction complete. "
            f"Rows: {inspection['row_count']} -> {raw_result['raw_file_path']}"
        )

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

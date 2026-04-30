from datetime import date

from pipelines.common.time import today_iso
from pipelines.extractors.fred.client import FredClient
from pipelines.extractors.fred.config import FRED_SERIES, FredSeries
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.storage.local_raw_store import write_raw_text
from pipelines.storage.manifest import write_manifest

SOURCE = "fred"
DATASET = "macro_series"


def _parse_observation_date(value: str | None) -> date | None:
    if not value:
        return None

    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _source_period_bounds(observations: list[dict]) -> tuple[date | None, date | None]:
    parsed_dates = [
        parsed
        for parsed in (_parse_observation_date(row.get("date")) for row in observations)
        if parsed is not None
    ]

    if not parsed_dates:
        return None, None

    return min(parsed_dates), max(parsed_dates)


def extract_series(
    client: FredClient,
    series: FredSeries,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    raw_payload = client.get_series_observations(series.series_id)
    observations = raw_payload.get("observations", [])
    source_period_start, source_period_end = _source_period_bounds(observations)

    raw_result = write_raw_text(
        source=SOURCE,
        dataset=DATASET,
        filename=f"{series.series_id}.json",
        content=client.dumps(raw_payload),
        load_date=load_date,
        overwrite=True,
    )

    source_url = (
        f"{client.base_url}/series/observations"
        f"?series_id={series.series_id}"
    )

    manifest_result = write_manifest(
        source=SOURCE,
        dataset=DATASET,
        raw_file_path=raw_result["raw_file_path"],
        status="success",
        load_date=load_date,
        source_url=source_url,
        file_format="json",
        record_count=len(observations),
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        source_period_start=source_period_start.isoformat() if source_period_start else None,
        source_period_end=source_period_end.isoformat() if source_period_end else None,
        metadata={
            "series_id": series.series_id,
            "metric_name": series.metric_name,
            "description": series.description,
            "frequency_hint": series.frequency_hint,
            "category": series.category,
            "source_role": series.source_role,
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
        record_count=len(observations),
        source_period_start=source_period_start,
        source_period_end=source_period_end,
        load_date=date.fromisoformat(load_date),
        status="success",
        metadata={
            "series_id": series.series_id,
            "metric_name": series.metric_name,
            "description": series.description,
            "frequency_hint": series.frequency_hint,
            "category": series.category,
            "source_role": series.source_role,
            "manifest_path": manifest_result["manifest_path"],
        },
    )

    print(
        f"Extracted {series.series_id}: "
        f"{len(observations)} observations -> {raw_result['raw_file_path']}"
    )

    return len(observations)


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="fred_macro_series_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "series_ids": [series.series_id for series in FRED_SERIES],
            "series_count": len(FRED_SERIES),
            "categories": sorted({series.category for series in FRED_SERIES}),
        },
    )

    total_observations = 0

    try:
        client = FredClient()

        import time

        for series in FRED_SERIES:
            total_observations += extract_series(
                client=client,
                series=series,
                pipeline_run_id=pipeline_run_id,
                load_date=load_date,
            )

            # Be polite to the FRED API and reduce transient 5xx failures.
            time.sleep(0.5)

        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="success",
            records_extracted=total_observations,
            records_loaded=len(FRED_SERIES),
            records_failed=0,
        )

        update_source_freshness(
            source=SOURCE,
            dataset=DATASET,
            latest_source_period=None,
            last_successful_run_id=pipeline_run_id,
            last_status="success",
            record_count=total_observations,
        )

        print(f"FRED extraction complete. Total observations: {total_observations}")

    except Exception as exc:
        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="failed",
            records_extracted=total_observations,
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
            record_count=total_observations,
            error_message=str(exc),
        )

        raise


if __name__ == "__main__":
    main()

import json
from datetime import date

from pipelines.common.time import today_iso
from pipelines.extractors.bls_laus.client import BlsLausClient
from pipelines.extractors.bls_laus.config import BLS_LAUS_DATASET
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.storage.local_raw_store import write_raw_text
from pipelines.storage.manifest import write_manifest

SOURCE = "bls_laus"
DATASET = "labor_market"


def _period_to_date(year: int | None, period: str | None) -> date | None:
    if year is None or not period:
        return None

    if not period.startswith("M"):
        return date(year, 12, 31)

    try:
        month = int(period[1:])
    except ValueError:
        return None

    if month < 1 or month > 12:
        return None

    if month == 12:
        return date(year, 12, 31)

    return date(year, month + 1, 1)


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="bls_laus_labor_market_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "series": [
                {
                    "series_id": series.series_id,
                    "label": series.label,
                    "geography_level": series.geography_level,
                    "measure": series.measure,
                    "geo_reference": series.geo_reference,
                }
                for series in BLS_LAUS_DATASET.series
            ],
            "start_year": BLS_LAUS_DATASET.start_year,
            "end_year": BLS_LAUS_DATASET.end_year,
        },
    )

    try:
        client = BlsLausClient()
        payload = client.get_dataset(BLS_LAUS_DATASET)
        observation_count = client.get_observation_count(payload)
        latest_year, latest_period = client.get_latest_period(payload)
        latest_source_period = _period_to_date(latest_year, latest_period)

        raw_content = json.dumps(payload, indent=2, sort_keys=False)

        raw_result = write_raw_text(
            source=SOURCE,
            dataset=DATASET,
            filename=BLS_LAUS_DATASET.filename,
            content=raw_content,
            load_date=load_date,
            overwrite=True,
        )

        source_url = client.base_url

        manifest_result = write_manifest(
            source=SOURCE,
            dataset=DATASET,
            raw_file_path=raw_result["raw_file_path"],
            status="success",
            load_date=load_date,
            source_url=source_url,
            file_format="json",
            record_count=observation_count,
            checksum_sha256=raw_result["checksum_sha256"],
            file_size_bytes=raw_result["file_size_bytes"],
            source_period_start=date(BLS_LAUS_DATASET.start_year, 1, 1).isoformat(),
            source_period_end=latest_source_period.isoformat() if latest_source_period else None,
            metadata={
                "description": BLS_LAUS_DATASET.description,
                "expected_frequency": BLS_LAUS_DATASET.expected_frequency,
                "start_year": BLS_LAUS_DATASET.start_year,
                "end_year": BLS_LAUS_DATASET.end_year,
                "series_count": len(BLS_LAUS_DATASET.series),
                "series": payload["series_metadata"],
                "latest_year": latest_year,
                "latest_period": latest_period,
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
            record_count=observation_count,
            source_period_start=date(BLS_LAUS_DATASET.start_year, 1, 1),
            source_period_end=latest_source_period,
            load_date=date.fromisoformat(load_date),
            status="success",
            metadata={
                "description": BLS_LAUS_DATASET.description,
                "expected_frequency": BLS_LAUS_DATASET.expected_frequency,
                "start_year": BLS_LAUS_DATASET.start_year,
                "end_year": BLS_LAUS_DATASET.end_year,
                "series_count": len(BLS_LAUS_DATASET.series),
                "series": payload["series_metadata"],
                "manifest_path": manifest_result["manifest_path"],
                "latest_year": latest_year,
                "latest_period": latest_period,
            },
        )

        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="success",
            records_extracted=observation_count,
            records_loaded=1,
            records_failed=0,
        )

        update_source_freshness(
            source=SOURCE,
            dataset=DATASET,
            latest_source_period=latest_source_period,
            last_successful_run_id=pipeline_run_id,
            last_status="success",
            record_count=observation_count,
        )

        print(
            f"BLS LAUS extraction complete. "
            f"Observations: {observation_count} -> {raw_result['raw_file_path']}"
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

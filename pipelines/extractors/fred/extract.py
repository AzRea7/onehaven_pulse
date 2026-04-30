import json
from datetime import date
from typing import Any

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


def _clean_observations(payload: dict[str, Any]) -> list[dict[str, Any]]:
    observations = payload.get("observations", [])

    return [
        observation
        for observation in observations
        if observation.get("value") not in {None, "", "."}
    ]


def _get_source_period_bounds(observations: list[dict[str, Any]]) -> tuple[date | None, date | None]:
    if not observations:
        return None, None

    dates = [
        date.fromisoformat(observation["date"])
        for observation in observations
        if observation.get("date")
    ]

    if not dates:
        return None, None

    return min(dates), max(dates)


def extract_series(
    client: FredClient,
    fred_series: FredSeries,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    payload = client.get_series_observations(series_id=fred_series.series_id)
    observations = _clean_observations(payload)
    source_period_start, source_period_end = _get_source_period_bounds(observations)

    filename = f"{fred_series.series_id}.json"

    raw_content = json.dumps(payload, indent=2, sort_keys=True)

    raw_result = write_raw_text(
        source=SOURCE,
        dataset=DATASET,
        filename=filename,
        content=raw_content,
        load_date=load_date,
        overwrite=True,
    )

    source_url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={fred_series.series_id}&file_type=json"
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
            "series_id": fred_series.series_id,
            "metric_name": fred_series.metric_name,
            "description": fred_series.description,
            "frequency_hint": fred_series.frequency_hint,
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
            "series_id": fred_series.series_id,
            "metric_name": fred_series.metric_name,
            "description": fred_series.description,
            "frequency_hint": fred_series.frequency_hint,
            "manifest_path": manifest_result["manifest_path"],
        },
    )

    print(
        f"Extracted {fred_series.series_id}: "
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
        },
    )

    total_observations = 0
    latest_source_period: date | None = None

    try:
        client = FredClient()

        for fred_series in FRED_SERIES:
            payload = client.get_series_observations(series_id=fred_series.series_id)
            observations = _clean_observations(payload)
            _, series_latest_period = _get_source_period_bounds(observations)

            # Reuse already downloaded payload by monkey patching a tiny local client call result.
            class SinglePayloadClient(FredClient):
                def __init__(self, payload: dict[str, Any]) -> None:
                    self.payload = payload

                def get_series_observations(
                    self,
                    series_id: str,
                    observation_start: str | None = None,
                    observation_end: str | None = None,
                ) -> dict[str, Any]:
                    return self.payload

            total_observations += extract_series(
                client=SinglePayloadClient(payload),
                fred_series=fred_series,
                pipeline_run_id=pipeline_run_id,
                load_date=load_date,
            )

            if series_latest_period and (
                latest_source_period is None or series_latest_period > latest_source_period
            ):
                latest_source_period = series_latest_period

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
            latest_source_period=latest_source_period,
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
            latest_source_period=latest_source_period,
            last_successful_run_id=None,
            last_status="failed",
            record_count=total_observations,
            error_message=str(exc),
        )

        raise


if __name__ == "__main__":
    main()

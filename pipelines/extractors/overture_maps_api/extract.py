import json
from datetime import date

from pipelines.common.time import today_iso
from pipelines.extractors.overture_maps_api.client import OvertureMapsApiClient
from pipelines.extractors.overture_maps_api.config import (
    OVERTURE_MAPS_API_PLACES,
    OvertureMapsApiDataset,
)
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.storage.local_raw_store import write_raw_text
from pipelines.storage.manifest import write_manifest

SOURCE = "overture_maps"
DATASET = "places"


def _source_period_bounds(load_date: str) -> tuple[date, date]:
    parsed = date.fromisoformat(load_date)
    return parsed, parsed


def _infer_columns(records: list[dict]) -> list[str]:
    columns: set[str] = set()

    for record in records[:25]:
        if isinstance(record, dict):
            columns.update(record.keys())

    return sorted(columns)


def extract_dataset(
    client: OvertureMapsApiClient,
    dataset: OvertureMapsApiDataset,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    payload = client.get_places(dataset)
    records = payload["records"]
    record_count = payload["record_count"]
    columns = _infer_columns(records)

    raw_content = json.dumps(payload, indent=2, sort_keys=False, default=str)

    raw_result = write_raw_text(
        source=SOURCE,
        dataset=DATASET,
        filename=dataset.filename,
        content=raw_content,
        load_date=load_date,
        overwrite=True,
    )

    source_period_start, source_period_end = _source_period_bounds(load_date)

    source_url = f"{dataset.base_url.rstrip('/')}/{dataset.endpoint}"

    manifest_result = write_manifest(
        source=SOURCE,
        dataset=DATASET,
        raw_file_path=raw_result["raw_file_path"],
        status="success",
        load_date=load_date,
        source_url=source_url,
        file_format="json",
        record_count=record_count,
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        source_period_start=source_period_start.isoformat(),
        source_period_end=source_period_end.isoformat(),
        metadata={
            "endpoint": dataset.endpoint,
            "area_slug": dataset.area_slug,
            "area_name": dataset.area_name,
            "country": dataset.country,
            "lat": dataset.lat,
            "lng": dataset.lng,
            "radius": dataset.radius,
            "categories": dataset.categories,
            "brand_name": dataset.brand_name,
            "limit": dataset.limit,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "api_key_required": True,
            "columns": columns,
            "note": "Raw Overture Maps API response is stored only. Feature engineering happens in Epic 4.",
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
        record_count=record_count,
        source_period_start=source_period_start,
        source_period_end=source_period_end,
        load_date=date.fromisoformat(load_date),
        status="success",
        metadata={
            "endpoint": dataset.endpoint,
            "area_slug": dataset.area_slug,
            "area_name": dataset.area_name,
            "country": dataset.country,
            "lat": dataset.lat,
            "lng": dataset.lng,
            "radius": dataset.radius,
            "categories": dataset.categories,
            "brand_name": dataset.brand_name,
            "limit": dataset.limit,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "manifest_path": manifest_result["manifest_path"],
            "api_key_required": True,
            "columns": columns,
            "note": "Raw Overture Maps API response is stored only. Feature engineering happens in Epic 4.",
        },
    )

    print(
        f"Extracted Overture Maps API places: "
        f"{record_count} records -> {raw_result['raw_file_path']}"
    )

    return record_count


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="overture_maps_api_places_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "endpoint": OVERTURE_MAPS_API_PLACES.endpoint,
            "area_slug": OVERTURE_MAPS_API_PLACES.area_slug,
            "area_name": OVERTURE_MAPS_API_PLACES.area_name,
            "api_key_required": True,
        },
    )

    try:
        client = OvertureMapsApiClient()
        record_count = extract_dataset(
            client=client,
            dataset=OVERTURE_MAPS_API_PLACES,
            pipeline_run_id=pipeline_run_id,
            load_date=load_date,
        )

        _, source_period_end = _source_period_bounds(load_date)

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

        print(f"Overture Maps API extraction complete. Records: {record_count}")

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

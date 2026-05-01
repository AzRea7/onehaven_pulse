import json
from datetime import date
from typing import Any

from pipelines.common.time import today_iso
from pipelines.extractors.overture_maps_api.client import OvertureMapsApiClient
from pipelines.extractors.overture_maps_api.config import OVERTURE_MAPS_API_PLACES
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.loaders.overture_places_loader import load_overture_places
from pipelines.storage.local_raw_store import write_raw_text
from pipelines.storage.manifest import write_manifest

SOURCE = "overture_maps_api"
DATASET = "places"


def _dataset_attr(dataset: Any, name: str, default: Any = None) -> Any:
    return getattr(dataset, name, default)


def _dataset_filename(dataset: Any) -> str:
    return str(
        _dataset_attr(
            dataset,
            "filename",
            f"overture_places_{_dataset_attr(dataset, 'area_slug', 'area')}.json",
        )
    )


def _dataset_area_slug(dataset: Any) -> str:
    return str(_dataset_attr(dataset, "area_slug", "default_area"))


def _dataset_area_name(dataset: Any) -> str:
    return str(_dataset_attr(dataset, "area_name", _dataset_area_slug(dataset)))


def _source_url(client: OvertureMapsApiClient, dataset: Any) -> str:
    if hasattr(client, "build_url"):
        return str(client.build_url(dataset))

    if hasattr(client, "base_url"):
        return str(client.base_url)

    return "overture_maps_api"


def _get_payload(client: OvertureMapsApiClient, dataset: Any) -> dict:
    method_names = (
        "get_dataset",
        "get_places",
        "search_places",
        "fetch_dataset",
        "get_dataset_payload",
    )

    for method_name in method_names:
        method = getattr(client, method_name, None)

        if method is None:
            continue

        result = method(dataset)

        if isinstance(result, dict):
            return result

        if isinstance(result, bytes):
            return json.loads(result.decode("utf-8"))

        if isinstance(result, str):
            return json.loads(result)

    available = [
        name
        for name in dir(client)
        if not name.startswith("_") and callable(getattr(client, name))
    ]

    raise RuntimeError(
        "Could not fetch Overture places payload. "
        f"Available OvertureMapsApiClient methods: {available}"
    )


def _record_count(payload: dict) -> int:
    records = (
        payload.get("records")
        or payload.get("places")
        or payload.get("features")
        or payload.get("data")
        or []
    )

    if isinstance(records, dict):
        records = records.get("records") or records.get("places") or records.get("features") or []

    if isinstance(records, list):
        return len(records)

    return 0


def main() -> None:
    load_date = today_iso()
    dataset = OVERTURE_MAPS_API_PLACES

    pipeline_run_id = start_pipeline_run(
        pipeline_name="overture_places_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "area_slug": _dataset_area_slug(dataset),
            "area_name": _dataset_area_name(dataset),
        },
    )

    loaded_count = 0

    try:
        client = OvertureMapsApiClient()
        payload = _get_payload(client, dataset)
        record_count = _record_count(payload)

        raw_content = json.dumps(payload, indent=2, sort_keys=False, default=str)

        raw_result = write_raw_text(
            source=SOURCE,
            dataset=DATASET,
            filename=_dataset_filename(dataset),
            content=raw_content,
            load_date=load_date,
            overwrite=True,
        )

        source_url = _source_url(client, dataset)

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
            source_period_start=None,
            source_period_end=None,
            metadata={
                "area_slug": _dataset_area_slug(dataset),
                "area_name": _dataset_area_name(dataset),
            },
        )

        source_file_id = record_source_file(
            pipeline_run_id=pipeline_run_id,
            source=SOURCE,
            dataset=DATASET,
            source_url=source_url,
            raw_file_path=raw_result["raw_file_path"],
            file_format="json",
            checksum_sha256=raw_result["checksum_sha256"],
            file_size_bytes=raw_result["file_size_bytes"],
            record_count=record_count,
            source_period_start=None,
            source_period_end=None,
            load_date=date.fromisoformat(load_date),
            status="success",
            metadata={
                "area_slug": _dataset_area_slug(dataset),
                "area_name": _dataset_area_name(dataset),
                "manifest_path": manifest_result["manifest_path"],
            },
        )

        loaded_count = load_overture_places(
            payload=payload,
            dataset=dataset,
            source_file_id=source_file_id,
            load_date=date.fromisoformat(load_date),
        )

        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="success",
            records_extracted=record_count,
            records_loaded=loaded_count,
            records_failed=0,
        )

        update_source_freshness(
            source=SOURCE,
            dataset=DATASET,
            latest_source_period=None,
            last_successful_run_id=pipeline_run_id,
            last_status="success",
            record_count=loaded_count,
        )

        print(
            f"Overture places extraction complete. "
            f"Source records: {record_count}. DB rows: {loaded_count}"
        )

    except Exception as exc:
        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="failed",
            records_extracted=None,
            records_loaded=loaded_count,
            records_failed=None,
            error_message=str(exc),
        )

        update_source_freshness(
            source=SOURCE,
            dataset=DATASET,
            latest_source_period=None,
            last_successful_run_id=None,
            last_status="failed",
            record_count=loaded_count,
            error_message=str(exc),
        )

        raise


if __name__ == "__main__":
    main()

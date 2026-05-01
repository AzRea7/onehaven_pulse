import json
from datetime import date

from pipelines.common.time import today_iso
from pipelines.extractors.fema_nri.client import FemaNriClient
from pipelines.extractors.fema_nri.config import FEMA_NRI_COUNTY_RISK, FemaNriDataset
from pipelines.loaders.fema_nri_loader import load_fema_nri_county_risk
from pipelines.loaders.audit_loader import (
    finish_pipeline_run,
    record_source_file,
    start_pipeline_run,
    update_source_freshness,
)
from pipelines.storage.local_raw_store import write_raw_text
from pipelines.storage.manifest import write_manifest

SOURCE = "fema_nri"
DATASET = "county_risk"


def _source_period_bounds() -> tuple[date, date]:
    return date(2025, 12, 1), date(2025, 12, 31)


def _infer_columns(records: list[dict]) -> list[str]:
    columns: set[str] = set()

    for record in records[:25]:
        columns.update(record.keys())

    return sorted(columns)


def extract_dataset(
    client: FemaNriClient,
    dataset: FemaNriDataset,
    pipeline_run_id: str,
    load_date: str,
) -> int:
    payload = client.get_dataset(dataset)
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

    source_period_start, source_period_end = _source_period_bounds()
    source_url = payload["request"]["layer_url"]

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
            "source_mode": dataset.source_mode,
            "version": dataset.version,
            "release_label": dataset.release_label,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "arcgis_item_id": dataset.arcgis_item_id,
            "service_url": payload["request"]["service_url"],
            "layer_url": payload["request"]["layer_url"],
            "layer_id": dataset.arcgis_layer_id,
            "page_count": payload["page_count"],
            "page_size": payload["request"]["page_size"],
            "where": dataset.arcgis_where,
            "out_fields": dataset.arcgis_out_fields,
            "return_geometry": False,
            "api_key_required": False,
            "columns": columns,
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
        source_period_start=source_period_start,
        source_period_end=source_period_end,
        load_date=date.fromisoformat(load_date),
        status="success",
        metadata={
            "source_mode": dataset.source_mode,
            "version": dataset.version,
            "release_label": dataset.release_label,
            "description": dataset.description,
            "expected_frequency": dataset.expected_frequency,
            "manifest_path": manifest_result["manifest_path"],
            "arcgis_item_id": dataset.arcgis_item_id,
            "service_url": payload["request"]["service_url"],
            "layer_url": payload["request"]["layer_url"],
            "layer_id": dataset.arcgis_layer_id,
            "page_count": payload["page_count"],
            "page_size": payload["request"]["page_size"],
            "where": dataset.arcgis_where,
            "out_fields": dataset.arcgis_out_fields,
            "return_geometry": False,
            "api_key_required": False,
            "columns": columns,
        },
    )

    loaded_count = load_fema_nri_county_risk(
        payload=payload,
        dataset=dataset,
        source_file_id=source_file_id,
        load_date=date.fromisoformat(load_date),
    )

    print(
        f"Extracted FEMA NRI county_risk: "
        f"{record_count} source records, {loaded_count} DB rows "
        f"across {payload['page_count']} pages -> {raw_result['raw_file_path']}"
    )

    return loaded_count


def main() -> None:
    load_date = today_iso()

    pipeline_run_id = start_pipeline_run(
        pipeline_name="fema_nri_county_risk_extract",
        source=SOURCE,
        dataset=DATASET,
        metadata={
            "source_mode": FEMA_NRI_COUNTY_RISK.source_mode,
            "arcgis_item_id": FEMA_NRI_COUNTY_RISK.arcgis_item_id,
            "layer_id": FEMA_NRI_COUNTY_RISK.arcgis_layer_id,
            "api_key_required": False,
        },
    )

    try:
        client = FemaNriClient()
        record_count = extract_dataset(
            client=client,
            dataset=FEMA_NRI_COUNTY_RISK,
            pipeline_run_id=pipeline_run_id,
            load_date=load_date,
        )

        _, source_period_end = _source_period_bounds()

        finish_pipeline_run(
            run_id=pipeline_run_id,
            status="success",
            records_extracted=record_count,
            records_loaded=record_count,
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

        print(f"FEMA NRI extraction complete. Records: {record_count}")

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

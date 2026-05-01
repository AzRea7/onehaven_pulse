from datetime import date
from decimal import Decimal

from pipelines.transforms.common.market_metric_loader import upsert_market_metrics
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from pipelines.transforms.common.transform_audit import finish_transform_run, start_transform_run


TRANSFORM_NAME = "smoke_market_metric_transform"
SOURCE = "smoke"
DATASET = "market_metric"
TARGET_TABLE = "analytics.market_monthly_metrics"


def build_records(transform_run_id: str) -> list[MarketMetricRecord]:
    return [
        MarketMetricRecord(
            geo_id="us",
            period_month=date(2026, 1, 1),
            metric_name="home_price_index",
            metric_value=Decimal("100.123456"),
            metric_unit="index",
            source=SOURCE,
            dataset=DATASET,
            pipeline_run_id=transform_run_id,
            source_period=date(2026, 1, 1),
            transformation_notes="Smoke metric for Story 4.1 framework validation.",
            source_flags={"smoke": True},
            quality_flags={"validated": True},
        )
    ]


def main() -> None:
    run_id = start_transform_run(
        transform_name=TRANSFORM_NAME,
        source=SOURCE,
        dataset=DATASET,
        target_table=TARGET_TABLE,
        metadata={"story": "4.1"},
    )

    try:
        records = build_records(run_id)
        loaded_count = upsert_market_metrics(records)

        finish_transform_run(
            run_id=run_id,
            status="success",
            records_extracted=len(records),
            records_loaded=loaded_count,
            records_failed=0,
        )

        print(f"Loaded {loaded_count} smoke market metric record(s). Run ID: {run_id}")

    except Exception as exc:
        finish_transform_run(
            run_id=run_id,
            status="failed",
            records_extracted=0,
            records_loaded=0,
            records_failed=1,
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()

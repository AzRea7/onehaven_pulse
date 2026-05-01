from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.transforms.common.market_metric_loader import upsert_market_metrics
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from pipelines.transforms.common.transform_audit import finish_transform_run, start_transform_run


SOURCE = "overture_maps_api"
DATASET = "places"
TRANSFORM_NAME = "overture_places_amenity_transform"
TARGET_TABLE = "analytics.market_monthly_metrics"


@dataclass(frozen=True)
class OvertureAmenityAggregate:
    geo_id: str
    area_slug: str
    area_name: str
    total_count: Decimal
    school_count: Decimal
    healthcare_count: Decimal
    grocery_count: Decimal
    food_service_count: Decimal
    bank_count: Decimal


def _period_month() -> date:
    # Overture Places is a point-in-time amenity snapshot.
    # Store it in the monthly mart using the current Epic 4 build month.
    return date(2026, 5, 1)


def _ensure_area_geo(area_slug: str, area_name: str) -> str:
    geo_id = f"metro:overture_{area_slug}"

    sql = text(
        """
        INSERT INTO geo.dim_geo (
            geo_id,
            geo_type,
            name,
            display_name,
            country_code,
            is_active,
            created_at,
            updated_at
        )
        VALUES (
            :geo_id,
            'metro',
            :name,
            :display_name,
            'US',
            true,
            now(),
            now()
        )
        ON CONFLICT (geo_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            display_name = EXCLUDED.display_name,
            updated_at = now()
        """
    )

    with engine.begin() as connection:
        connection.execute(
            sql,
            {
                "geo_id": geo_id,
                "name": area_name,
                "display_name": area_name,
            },
        )

    return geo_id


def fetch_aggregates() -> list[OvertureAmenityAggregate]:
    sql = text(
        """
        SELECT
            area_slug,
            area_name,
            COUNT(*) AS total_count,
            COUNT(*) FILTER (
                WHERE lower(coalesce(primary_category, category, ''))
                    SIMILAR TO '%(school|education|university|college)%'
            ) AS school_count,
            COUNT(*) FILTER (
                WHERE lower(coalesce(primary_category, category, ''))
                    SIMILAR TO '%(hospital|clinic|doctor|health|pharmacy)%'
            ) AS healthcare_count,
            COUNT(*) FILTER (
                WHERE lower(coalesce(primary_category, category, ''))
                    SIMILAR TO '%(grocery|supermarket|market)%'
            ) AS grocery_count,
            COUNT(*) FILTER (
                WHERE lower(coalesce(primary_category, category, ''))
                    SIMILAR TO '%(restaurant|cafe|bar|food)%'
            ) AS food_service_count,
            COUNT(*) FILTER (
                WHERE lower(coalesce(primary_category, category, ''))
                    SIMILAR TO '%(bank|atm|financial)%'
            ) AS bank_count
        FROM raw.overture_places
        GROUP BY area_slug, area_name
        ORDER BY area_slug
        """
    )

    aggregates: list[OvertureAmenityAggregate] = []

    with engine.begin() as connection:
        result = connection.execute(sql)

        for row in result.mappings():
            geo_id = _ensure_area_geo(row["area_slug"], row["area_name"])

            aggregates.append(
                OvertureAmenityAggregate(
                    geo_id=geo_id,
                    area_slug=row["area_slug"],
                    area_name=row["area_name"],
                    total_count=Decimal(row["total_count"]),
                    school_count=Decimal(row["school_count"]),
                    healthcare_count=Decimal(row["healthcare_count"]),
                    grocery_count=Decimal(row["grocery_count"]),
                    food_service_count=Decimal(row["food_service_count"]),
                    bank_count=Decimal(row["bank_count"]),
                )
            )

    return aggregates


def build_records(
    aggregates: list[OvertureAmenityAggregate],
    run_id: str,
) -> list[MarketMetricRecord]:
    records: list[MarketMetricRecord] = []
    period_month = _period_month()

    for row in aggregates:
        values = {
            "amenity_place_count": row.total_count,
            "amenity_school_count": row.school_count,
            "amenity_healthcare_count": row.healthcare_count,
            "amenity_grocery_count": row.grocery_count,
            "amenity_food_service_count": row.food_service_count,
            "amenity_bank_count": row.bank_count,
        }

        for metric_name, value in values.items():
            records.append(
                MarketMetricRecord(
                    geo_id=row.geo_id,
                    period_month=period_month,
                    metric_name=metric_name,
                    metric_value=value,
                    metric_unit="count",
                    source=SOURCE,
                    dataset=DATASET,
                    source_file_id=None,
                    pipeline_run_id=run_id,
                    source_value=value,
                    source_period=period_month,
                    period_grain="monthly",
                    transformation_notes=f"Aggregated Overture Places {metric_name}.",
                    source_flags={
                        "area_slug": row.area_slug,
                        "area_name": row.area_name,
                        "derived_from": "raw.overture_places",
                    },
                    quality_flags={
                        "area_radius_based": True,
                        "provisional_overture_geo": True,
                    },
                )
            )

    return records


def main() -> None:
    run_id = start_transform_run(
        transform_name=TRANSFORM_NAME,
        source=SOURCE,
        dataset=DATASET,
        target_table=TARGET_TABLE,
        metadata={
            "target_metrics": [
                "amenity_place_count",
                "amenity_school_count",
                "amenity_healthcare_count",
                "amenity_grocery_count",
                "amenity_food_service_count",
                "amenity_bank_count",
            ],
        },
    )

    try:
        aggregates = fetch_aggregates()
        records = build_records(aggregates, run_id)
        loaded = upsert_market_metrics(records)

        finish_transform_run(
            run_id=run_id,
            status="success",
            records_extracted=len(aggregates),
            records_loaded=loaded,
            records_failed=0,
        )

        print(
            f"Overture amenities transform complete. "
            f"Aggregates: {len(aggregates)}. "
            f"Loaded metrics: {loaded}. "
            f"Run ID: {run_id}"
        )

    except Exception as exc:
        finish_transform_run(
            run_id=run_id,
            status="failed",
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()

import json
from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import text
from psycopg2.extras import execute_values

from pipelines.common.db import engine
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from functools import lru_cache


METRIC_COLUMN_MAP: dict[str, str] = {
    "amenity_place_count": "amenity_place_count",
    "amenity_school_count": "amenity_school_count",
    "amenity_healthcare_count": "amenity_healthcare_count",
    "amenity_grocery_count": "amenity_grocery_count",
    "amenity_food_service_count": "amenity_food_service_count",
    "amenity_bank_count": "amenity_bank_count",
    "hmda_applications": "hmda_applications",
    "hmda_originations": "hmda_originations",
    "hmda_denials": "hmda_denials",
    "hmda_denial_rate": "hmda_denial_rate",
    "hmda_median_loan_amount": "hmda_median_loan_amount",
    "hazard_risk_score": "hazard_risk_score",
    "expected_annual_loss": "expected_annual_loss",
    "social_vulnerability_score": "social_vulnerability_score",
    "community_resilience_score": "community_resilience_score",
    # FHFA / price index
    "home_price_index": "home_price_index",
    "home_price_index_yoy": "home_price_index_yoy",
    "home_price_index_mom": "home_price_index_mom",
    "real_home_price_index": "real_home_price_index",

    # Zillow value/rent
    "zhvi": "zhvi",
    "zhvi_yoy": "zhvi_yoy",
    "zhvi_mom": "zhvi_mom",
    "zori": "zori",
    "zori_yoy": "zori_yoy",
    "zori_mom": "zori_mom",

    # Redfin
    "median_sale_price": "median_sale_price",
    "median_sale_price_yoy": "median_sale_price_yoy",
    "median_sale_price_mom": "median_sale_price_mom",
    "active_listings": "active_listings",
    "active_listings_yoy": "active_listings_yoy",
    "new_listings": "new_listings",
    "new_listings_yoy": "new_listings_yoy",
    "homes_sold": "homes_sold",
    "pending_sales": "pending_sales",
    "homes_sold_yoy": "homes_sold_yoy",
    "months_supply": "months_supply",
    "median_days_on_market": "median_days_on_market",
    "sale_to_list_ratio": "sale_to_list_ratio",
    "price_drops_pct": "price_drops_pct",

    # FRED / macro
    "mortgage_rate_30y": "mortgage_rate_30y",
    "fed_funds_rate": "fed_funds_rate",
    "cpi": "cpi",
    "unemployment_rate": "unemployment_rate",
    "labor_force": "labor_force",
    "employment": "employment",
    "unemployment_count": "unemployment_count",

        # FRED / Treasury curve
    "treasury_2yr_rate": "treasury_2yr_rate",
    "treasury_5yr_rate": "treasury_5yr_rate",
    "treasury_10yr_rate": "treasury_10yr_rate",
    "treasury_30yr_rate": "treasury_30yr_rate",
    "treasury_10yr_2yr_spread": "treasury_10yr_2yr_spread",
    "treasury_10yr_3mo_spread": "treasury_10yr_3mo_spread",
    "recession_indicator": "recession_indicator",

    # Derived affordability
    "estimated_monthly_payment": "estimated_monthly_payment",
    "payment_to_income_ratio": "payment_to_income_ratio",
    "price_to_income_ratio": "price_to_income_ratio",
    "rent_to_price_ratio": "rent_to_price_ratio",

    # Census / permits
    "building_permits": "building_permits",
    "single_family_permits": "single_family_permits",
    "multi_family_permits": "multi_family_permits",
    "permit_units": "permit_units",
    "permits_per_1000_people": "permits_per_1000_people",
    "population": "population",
    "population_yoy": "population_yoy",
    "median_household_income": "median_household_income",
    "households": "households",
    "housing_units": "housing_units",
    "occupied_housing_units": "occupied_housing_units",
    "vacant_housing_units": "vacant_housing_units",
    "owner_occupied_housing_units": "owner_occupied_housing_units",
    "renter_occupied_housing_units": "renter_occupied_housing_units",
    "owner_occupied_share": "owner_occupied_share",
    "renter_occupied_share": "renter_occupied_share",
    "median_gross_rent": "median_gross_rent",
    "rent_burden_pct": "rent_burden_pct",

    # Rent fallback
    "median_rent": "median_rent",
    "median_rent_yoy": "median_rent_yoy",
}


def _metric_column(metric_name: str) -> str:
    try:
        return METRIC_COLUMN_MAP[metric_name]
    except KeyError as exc:
        allowed = ", ".join(sorted(METRIC_COLUMN_MAP))
        raise ValueError(
            f"Unsupported metric_name '{metric_name}'. Add it to METRIC_COLUMN_MAP. "
            f"Allowed metrics: {allowed}"
        ) from exc


@lru_cache(maxsize=None)
def _upsert_metric_sql(metric_column: str):
    return text(
        f"""
        INSERT INTO analytics.market_monthly_metrics (
            geo_id,
            period_month,
            {metric_column},
            source_flags,
            quality_flags,
            created_at,
            updated_at
        )
        VALUES (
            :geo_id,
            :period_month,
            :metric_value,
            CAST(:source_flags AS JSON),
            CAST(:quality_flags AS JSON),
            :created_at,
            :updated_at
        )
        ON CONFLICT (geo_id, period_month)
        DO UPDATE SET
            {metric_column} = EXCLUDED.{metric_column},
            source_flags = (
                COALESCE(analytics.market_monthly_metrics.source_flags, '{{}}'::json)::jsonb
                || COALESCE(EXCLUDED.source_flags, '{{}}'::json)::jsonb
            )::json,
            quality_flags = (
                COALESCE(analytics.market_monthly_metrics.quality_flags, '{{}}'::json)::jsonb
                || COALESCE(EXCLUDED.quality_flags, '{{}}'::json)::jsonb
            )::json,
            updated_at = EXCLUDED.updated_at
        """
    )


UPSERT_SOURCE_TRACE_SQL = text(
    """
    INSERT INTO analytics.market_metric_sources (
        geo_id,
        period_month,
        metric_name,
        source,
        dataset,
        source_file_id,
        pipeline_run_id,
        source_value,
        normalized_value,
        source_period,
        transformation_notes,
        created_at
    )
    VALUES (
        :geo_id,
        :period_month,
        :metric_name,
        :source,
        :dataset,
        :source_file_id,
        :pipeline_run_id,
        :source_value,
        :normalized_value,
        :source_period,
        :transformation_notes,
        :created_at
    )
    ON CONFLICT (
        geo_id,
        period_month,
        metric_name,
        source,
        dataset
    )
    DO UPDATE SET
        source_file_id = EXCLUDED.source_file_id,
        pipeline_run_id = EXCLUDED.pipeline_run_id,
        source_value = EXCLUDED.source_value,
        normalized_value = EXCLUDED.normalized_value,
        source_period = EXCLUDED.source_period,
        transformation_notes = EXCLUDED.transformation_notes,
        created_at = EXCLUDED.created_at
    """
)



def _mart_metric_value(record: MarketMetricRecord):
    if record.metric_name == "recession_indicator":
        return bool(record.metric_value)

    return record.metric_value


def _record_to_params(record: MarketMetricRecord) -> dict:
    record.validate()

    now = datetime.now(UTC)
    source_value = record.source_value if record.source_value is not None else record.metric_value

    return {
        "geo_id": record.geo_id,
        "period_month": record.period_month,
        "metric_name": record.metric_name,
        "metric_value": _mart_metric_value(record),
        "source": record.source,
        "dataset": record.dataset,
        "source_file_id": record.source_file_id,
        "pipeline_run_id": record.pipeline_run_id,
        "source_value": source_value,
        "normalized_value": record.metric_value,
        "source_period": record.source_period,
        "transformation_notes": record.transformation_notes,
        "source_flags": json.dumps(
            {
                record.metric_name: {
                    "source": record.source,
                    "dataset": record.dataset,
                    "unit": record.metric_unit,
                    **record.source_flags,
                }
            }
        ),
        "quality_flags": json.dumps(
            {
                record.metric_name: record.quality_flags,
            }
        ),
        "created_at": now,
        "updated_at": now,
    }



def _bulk_upsert_source_traces(connection, params: list[dict], *, page_size: int = 10000) -> None:
    if not params:
        return

    sql = """
        INSERT INTO analytics.market_metric_sources (
            geo_id,
            period_month,
            metric_name,
            source,
            dataset,
            source_file_id,
            pipeline_run_id,
            source_value,
            normalized_value,
            source_period,
            transformation_notes,
            created_at
        )
        VALUES %s
        ON CONFLICT (
            geo_id,
            period_month,
            metric_name,
            source,
            dataset
        )
        DO UPDATE SET
            source_file_id = EXCLUDED.source_file_id,
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            source_value = EXCLUDED.source_value,
            normalized_value = EXCLUDED.normalized_value,
            source_period = EXCLUDED.source_period,
            transformation_notes = EXCLUDED.transformation_notes,
            created_at = EXCLUDED.created_at
    """

    # Postgres cannot update the same ON CONFLICT target row twice
    # in one INSERT statement. Deduplicate each source trace batch
    # by the unique index key.
    deduped: dict[tuple, dict] = {}

    for item in params:
        key = (
            item["geo_id"],
            item["period_month"],
            item["metric_name"],
            item["source"],
            item["dataset"],
        )
        deduped[key] = item

    rows = [
        (
            item["geo_id"],
            item["period_month"],
            item["metric_name"],
            item["source"],
            item["dataset"],
            item["source_file_id"],
            item["pipeline_run_id"],
            item["source_value"],
            item["normalized_value"],
            item["source_period"],
            item["transformation_notes"],
            item["created_at"],
        )
        for item in deduped.values()
    ]

    with _raw_psycopg_connection(connection).cursor() as cursor:
        execute_values(
            cursor,
            sql,
            rows,
            page_size=page_size,
        )



def _raw_psycopg_connection(connection):
    raw_connection = getattr(connection.connection, "driver_connection", None)

    if raw_connection is None:
        raw_connection = connection.connection.connection

    return raw_connection


def _bulk_upsert_metric_values(
    connection,
    metric_column: str,
    params: list[dict],
    *,
    page_size: int = 10000,
) -> None:
    if not params:
        return

    sql = f"""
        INSERT INTO analytics.market_monthly_metrics (
            geo_id,
            period_month,
            {metric_column},
            source_flags,
            quality_flags,
            created_at,
            updated_at
        )
        VALUES %s
        ON CONFLICT (geo_id, period_month)
        DO UPDATE SET
            {metric_column} = EXCLUDED.{metric_column},
            source_flags = (
                COALESCE(analytics.market_monthly_metrics.source_flags, '{{}}'::json)::jsonb
                || COALESCE(EXCLUDED.source_flags, '{{}}'::json)::jsonb
            )::json,
            quality_flags = (
                COALESCE(analytics.market_monthly_metrics.quality_flags, '{{}}'::json)::jsonb
                || COALESCE(EXCLUDED.quality_flags, '{{}}'::json)::jsonb
            )::json,
            updated_at = EXCLUDED.updated_at
    """

    # Postgres cannot update the same ON CONFLICT target row twice
    # in one INSERT statement. Deduplicate each metric batch by the
    # market_monthly_metrics conflict key.
    deduped: dict[tuple, dict] = {}

    for item in params:
        key = (item["geo_id"], item["period_month"])
        deduped[key] = item

    rows = [
        (
            item["geo_id"],
            item["period_month"],
            item["metric_value"],
            item["source_flags"],
            item["quality_flags"],
            item["created_at"],
            item["updated_at"],
        )
        for item in deduped.values()
    ]

    with _raw_psycopg_connection(connection).cursor() as cursor:
        execute_values(
            cursor,
            sql,
            rows,
            template="(%s, %s, %s, CAST(%s AS JSON), CAST(%s AS JSON), %s, %s)",
            page_size=page_size,
        )


def upsert_market_metrics(records: Sequence[MarketMetricRecord]) -> int:
    if not records:
        return 0

    metric_batches: dict[str, list[dict]] = defaultdict(list)
    source_trace_params: list[dict] = []

    for record in records:
        metric_column = _metric_column(record.metric_name)
        params = _record_to_params(record)

        metric_batches[metric_column].append(params)
        source_trace_params.append(params)

    with engine.begin() as connection:
        for metric_column, batch_params in metric_batches.items():
            _bulk_upsert_metric_values(connection, metric_column, batch_params)

        if source_trace_params:
            _bulk_upsert_source_traces(connection, source_trace_params)

    return len(records)


def count_market_metric_sources(
    metric_name: str | None = None,
    source: str | None = None,
    dataset: str | None = None,
) -> int:
    clauses = []
    params = {}

    if metric_name:
        clauses.append("metric_name = :metric_name")
        params["metric_name"] = metric_name

    if source:
        clauses.append("source = :source")
        params["source"] = source

    if dataset:
        clauses.append("dataset = :dataset")
        params["dataset"] = dataset

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    sql = text(
        f"""
        SELECT COUNT(*) AS count
        FROM analytics.market_metric_sources
        {where_clause}
        """
    )

    with engine.begin() as connection:
        return int(connection.execute(sql, params).scalar_one())

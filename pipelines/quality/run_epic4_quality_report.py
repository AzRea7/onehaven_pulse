import argparse
from dataclasses import asdict
from decimal import Decimal

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.quality.epic4_metric_catalog import EPIC4_METRIC_CATALOG


def _query(sql: str, params: dict | None = None) -> list[dict]:
    with engine.begin() as connection:
        return [dict(row) for row in connection.execute(text(sql), params or {}).mappings()]


def metric_coverage() -> list[dict]:
    return _query(
        """
        SELECT
            metric_name,
            source,
            dataset,
            COUNT(*) AS source_rows,
            COUNT(DISTINCT geo_id) AS geo_count,
            MIN(period_month) AS min_period,
            MAX(period_month) AS max_period,
            COUNT(DISTINCT period_month) AS period_count
        FROM analytics.market_metric_sources
        GROUP BY metric_name, source, dataset
        ORDER BY source, dataset, metric_name
        """
    )


def required_metric_status() -> list[dict]:
    coverage_by_key = {
        (row["metric_name"], row["source"], row["dataset"]): row
        for row in metric_coverage()
    }

    status_rows: list[dict] = []

    for definition in EPIC4_METRIC_CATALOG:
        key = (definition.metric_name, definition.source, definition.dataset)
        coverage = coverage_by_key.get(key)
        source_rows = coverage["source_rows"] if coverage else 0

        if source_rows > 0:
            status = "ok"
        elif definition.required:
            status = "missing_required"
        else:
            status = "missing_optional"

        status_rows.append(
            {
                **asdict(definition),
                "status": status,
                "source_rows": source_rows,
                "geo_count": coverage["geo_count"] if coverage else 0,
                "min_period": coverage["min_period"] if coverage else None,
                "max_period": coverage["max_period"] if coverage else None,
                "period_count": coverage["period_count"] if coverage else 0,
            }
        )

    return status_rows


def latest_transform_runs() -> list[dict]:
    return _query(
        """
        WITH ranked_runs AS (
            SELECT
                pipeline_name,
                source,
                dataset,
                status,
                records_extracted,
                records_loaded,
                records_failed,
                error_message,
                started_at,
                finished_at,
                ROW_NUMBER() OVER (
                    PARTITION BY pipeline_name
                    ORDER BY started_at DESC
                ) AS row_number
            FROM audit.pipeline_runs
            WHERE pipeline_name LIKE '%transform%'
        )
        SELECT
            pipeline_name,
            source,
            dataset,
            status,
            records_extracted,
            records_loaded,
            records_failed,
            error_message,
            started_at,
            finished_at
        FROM ranked_runs
        WHERE row_number = 1
        ORDER BY pipeline_name
        """
    )


def source_trace_duplicates() -> list[dict]:
    return _query(
        """
        SELECT
            geo_id,
            period_month,
            metric_name,
            source,
            dataset,
            COUNT(*) AS duplicate_count
        FROM analytics.market_metric_sources
        GROUP BY geo_id, period_month, metric_name, source, dataset
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC, source, dataset, metric_name
        LIMIT 100
        """
    )


def mart_completeness() -> list[dict]:
    return _query(
        """
        SELECT
            COUNT(*) AS mart_rows,
            COUNT(DISTINCT geo_id) AS geo_count,
            MIN(period_month) AS min_period,
            MAX(period_month) AS max_period,
            COUNT(*) FILTER (WHERE median_sale_price IS NOT NULL OR zhvi IS NOT NULL) AS price_rows,
            COUNT(*) FILTER (WHERE median_rent IS NOT NULL OR zori IS NOT NULL) AS rent_rows,
            COUNT(*) FILTER (WHERE unemployment_rate IS NOT NULL) AS unemployment_rows,
            COUNT(*) FILTER (WHERE population IS NOT NULL) AS population_rows,
            COUNT(*) FILTER (WHERE building_permits IS NOT NULL OR permit_units IS NOT NULL) AS permit_rows,
            COUNT(*) FILTER (WHERE hazard_risk_score IS NOT NULL) AS hazard_rows,
            COUNT(*) FILTER (WHERE hmda_applications IS NOT NULL) AS hmda_rows,
            COUNT(*) FILTER (WHERE amenity_place_count IS NOT NULL) AS amenity_rows,
            COUNT(*) FILTER (WHERE estimated_monthly_payment IS NOT NULL) AS payment_rows
        FROM analytics.market_monthly_metrics
        """
    )


def overture_category_health() -> list[dict]:
    return _query(
        """
        SELECT
            area_slug,
            area_name,
            COUNT(*) AS raw_rows,
            COUNT(primary_category) AS categorized_rows,
            ROUND(
                COUNT(primary_category)::numeric / NULLIF(COUNT(*)::numeric, 0),
                4
            ) AS categorized_ratio
        FROM raw.overture_places
        GROUP BY area_slug, area_name
        ORDER BY raw_rows DESC
        """
    )


def hmda_health() -> list[dict]:
    return _query(
        """
        SELECT
            activity_year,
            COUNT(*) AS raw_rows,
            COUNT(loan_amount) AS loan_amount_rows,
            COUNT(*) FILTER (WHERE action_taken = '1') AS originations,
            COUNT(*) FILTER (WHERE action_taken = '3') AS denials,
            ROUND(
                COUNT(*) FILTER (WHERE action_taken = '3')::numeric
                / NULLIF(COUNT(*)::numeric, 0),
                4
            ) AS raw_denial_ratio
        FROM raw.hmda_modified_lar
        GROUP BY activity_year
        ORDER BY activity_year DESC
        """
    )


def print_table(title: str, rows: list[dict], *, limit: int | None = None) -> None:
    print()
    print("=" * 100)
    print(title)
    print("=" * 100)

    if not rows:
        print("(no rows)")
        return

    displayed = rows[:limit] if limit else rows

    for row in displayed:
        print(row)

    if limit and len(rows) > limit:
        print(f"... {len(rows) - limit} additional rows omitted")


def _decimal(value: object) -> Decimal:
    if value is None:
        return Decimal("0")

    return Decimal(str(value))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Epic 4 metric coverage and quality report.")
    parser.add_argument(
        "--fail-on-missing-required",
        action="store_true",
        help="Exit nonzero if any required metric has zero source rows.",
    )
    parser.add_argument(
        "--fail-on-duplicates",
        action="store_true",
        help="Exit nonzero if source trace duplicates exist.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Print full metric coverage instead of compact missing/problem rows.",
    )

    args = parser.parse_args()

    status_rows = required_metric_status()
    missing_required = [row for row in status_rows if row["status"] == "missing_required"]
    missing_optional = [row for row in status_rows if row["status"] == "missing_optional"]
    latest_runs = latest_transform_runs()
    duplicates = source_trace_duplicates()
    completeness = mart_completeness()
    overture_health = overture_category_health()
    hmda_rows = hmda_health()

    print_table("Mart completeness", completeness)
    print_table("Latest transform runs", latest_runs)

    if args.full:
        print_table("Metric catalog coverage", status_rows)
    else:
        problem_rows = [
            row
            for row in status_rows
            if row["status"] in {"missing_required", "missing_optional"}
        ]
        print_table("Missing metric coverage", problem_rows)

    print_table("Source trace duplicates", duplicates)
    print_table("Overture category health", overture_health)
    print_table("HMDA raw health", hmda_rows)

    failures: list[str] = []
    warnings: list[str] = []

    if missing_required:
        failures.append(f"{len(missing_required)} required metrics are missing.")

    if duplicates:
        failures.append(f"{len(duplicates)} source trace duplicate groups found.")

    for row in overture_health:
        ratio = _decimal(row["categorized_ratio"])

        if row["raw_rows"] and ratio < Decimal("0.25"):
            warnings.append(
                f"Overture area {row['area_slug']} has low categorized_ratio={ratio}."
            )

    for row in hmda_rows:
        if row["raw_rows"] and row["denials"] == 0:
            warnings.append(
                "HMDA raw data has zero denials. "
                "Denial-rate metrics are loaded but analytically incomplete; "
                "check HMDA actions_taken filter."
            )

    if args.fail_on_missing_required and missing_required:
        print()
        print("Required missing metrics:")
        for row in missing_required:
            print(row)

    if warnings:
        print()
        print("Epic 4 quality report warnings:")
        for warning in warnings:
            print(f"- {warning}")

    if failures:
        print()
        print("Epic 4 quality report found issues:")
        for failure in failures:
            print(f"- {failure}")

        if args.fail_on_missing_required or args.fail_on_duplicates:
            return 1

    print()
    print("Epic 4 quality report completed.")
    print(f"Missing required metrics: {len(missing_required)}")
    print(f"Missing optional metrics: {len(missing_optional)}")
    print(f"Duplicate source trace groups: {len(duplicates)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

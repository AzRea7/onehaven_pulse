import argparse
import sys
from dataclasses import asdict

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.orchestration.epic4_transform_plan import (
    EPIC4_TRANSFORM_PLAN,
    Epic4TransformStep,
)
from pipelines.transforms.registry.transform_registry import TRANSFORMS


def _latest_audit_run(pipeline_name: str) -> dict | None:
    sql = text(
        """
        SELECT
            id,
            pipeline_name,
            source,
            dataset,
            status,
            started_at,
            finished_at,
            records_extracted,
            records_loaded,
            records_failed,
            error_message
        FROM audit.pipeline_runs
        WHERE pipeline_name = :pipeline_name
        ORDER BY started_at DESC
        LIMIT 1
        """
    )

    with engine.begin() as connection:
        row = connection.execute(sql, {"pipeline_name": pipeline_name}).mappings().first()

    return dict(row) if row else None


def _source_trace_duplicates() -> list[dict]:
    sql = text(
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
        LIMIT 50
        """
    )

    with engine.begin() as connection:
        return [dict(row) for row in connection.execute(sql).mappings()]


def _metric_coverage() -> list[dict]:
    sql = text(
        """
        SELECT
            metric_name,
            source,
            dataset,
            COUNT(*) AS rows
        FROM analytics.market_metric_sources
        GROUP BY metric_name, source, dataset
        ORDER BY source, dataset, metric_name
        """
    )

    with engine.begin() as connection:
        return [dict(row) for row in connection.execute(sql).mappings()]


def _run_step(step: Epic4TransformStep) -> None:
    definition = TRANSFORMS.get(step.name)

    if definition is None:
        raise RuntimeError(f"Transform '{step.name}' is not registered.")

    print()
    print("=" * 100)
    print(f"Running Epic 4 transform: {step.name}")
    print(f"Target table: {definition.target_table}")
    print(f"Reason: {step.reason}")
    print("=" * 100)

    definition.runner()

    print(f"Finished Epic 4 transform: {step.name}")


def _validate_step(step: Epic4TransformStep) -> None:
    run = _latest_audit_run(step.audit_pipeline_name)

    if run is None:
        raise RuntimeError(
            f"No audit.pipeline_runs row found for '{step.audit_pipeline_name}' "
            f"after running transform '{step.name}'."
        )

    if run["status"] != "success":
        raise RuntimeError(
            f"Transform '{step.name}' did not finish successfully. "
            f"status={run['status']} error={run['error_message']}"
        )

    records_loaded = run["records_loaded"] or 0

    if records_loaded == 0 and not step.allow_zero_loaded:
        raise RuntimeError(
            f"Transform '{step.name}' loaded zero records. "
            "This is not allowed by the Epic 4 validation gate."
        )

    print(
        f"Validated {step.name}: "
        f"status={run['status']}, "
        f"records_extracted={run['records_extracted']}, "
        f"records_loaded={run['records_loaded']}, "
        f"records_failed={run['records_failed']}"
    )


def _selected_plan(only: str | None, skip_optional: bool) -> list[Epic4TransformStep]:
    plan = list(EPIC4_TRANSFORM_PLAN)

    if only:
        plan = [step for step in plan if step.name == only]

    if skip_optional:
        plan = [step for step in plan if step.required]

    return plan


def main() -> int:
    parser = argparse.ArgumentParser(description="Run and validate the Epic 4 transform stack.")
    parser.add_argument(
        "--only",
        choices=[step.name for step in EPIC4_TRANSFORM_PLAN],
        help="Run only one Epic 4 transform.",
    )
    parser.add_argument(
        "--skip-optional",
        action="store_true",
        help="Skip optional transforms such as Overture Places.",
    )
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Print metric coverage after validation.",
    )
    parser.add_argument(
        "--no-duplicate-check",
        action="store_true",
        help="Skip analytics.market_metric_sources duplicate check.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate latest audit runs without executing transforms.",
    )

    args = parser.parse_args()
    plan = _selected_plan(args.only, args.skip_optional)

    print("Epic 4 transform plan:")
    for step in plan:
        print(asdict(step))

    failures: list[str] = []

    for step in plan:
        try:
            if not args.validate_only:
                _run_step(step)

            _validate_step(step)

        except Exception as exc:
            message = f"{step.name}: {exc}"

            if step.required:
                print(f"FAILED REQUIRED STEP: {message}", file=sys.stderr)
                failures.append(message)
                break

            print(f"FAILED OPTIONAL STEP: {message}", file=sys.stderr)
            failures.append(message)

    if not args.no_duplicate_check:
        duplicates = _source_trace_duplicates()

        if duplicates:
            print()
            print("Source trace duplicates found:")
            for duplicate in duplicates:
                print(duplicate)

            failures.append("Source trace duplicate check failed.")
        else:
            print()
            print("Source trace duplicate check passed.")

    if args.coverage:
        print()
        print("Metric coverage:")
        for row in _metric_coverage():
            print(row)

    if failures:
        print()
        print("Epic 4 transform validation failed:")
        for failure in failures:
            print(f"- {failure}")

        return 1

    print()
    print("Epic 4 transform orchestration completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

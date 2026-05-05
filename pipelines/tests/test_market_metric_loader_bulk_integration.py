from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy import create_engine, text

from pipelines.common.settings import settings
from pipelines.transforms.common.market_metric_loader import (
    _bulk_upsert_metric_values,
    _bulk_upsert_source_traces,
)


def test_bulk_metric_upsert_handles_duplicate_geo_period_keys():
    engine = create_engine(settings.database_url)

    params = [
        {
            "geo_id": "metro_19820",
            "period_month": date(2026, 3, 1),
            "metric_value": Decimal("1.0"),
            "source_flags": "{}",
            "quality_flags": "{}",
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        },
        {
            "geo_id": "metro_19820",
            "period_month": date(2026, 3, 1),
            "metric_value": Decimal("2.0"),
            "source_flags": "{}",
            "quality_flags": "{}",
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        },
    ]

    with engine.begin() as connection:
        _bulk_upsert_metric_values(connection, "zhvi", params, page_size=100)

        value = connection.execute(
            text(
                """
                SELECT zhvi
                FROM analytics.market_monthly_metrics
                WHERE geo_id = 'metro_19820'
                  AND period_month = DATE '2026-03-01'
                """
            )
        ).scalar_one()

    assert value == Decimal("2.0")


def test_bulk_source_trace_upsert_handles_duplicate_identity_keys():
    engine = create_engine(settings.database_url)
    now = datetime.now(UTC)

    params = [
        {
            "geo_id": "metro_19820",
            "period_month": date(2026, 3, 1),
            "metric_name": "zhvi",
            "source": "test_loader",
            "dataset": "unit_test",
            "source_file_id": "file_a",
            "pipeline_run_id": "run_a",
            "source_value": Decimal("1.0"),
            "normalized_value": Decimal("1.0"),
            "source_period": date(2026, 3, 1),
            "transformation_notes": "first",
            "created_at": now,
        },
        {
            "geo_id": "metro_19820",
            "period_month": date(2026, 3, 1),
            "metric_name": "zhvi",
            "source": "test_loader",
            "dataset": "unit_test",
            "source_file_id": "file_b",
            "pipeline_run_id": "run_b",
            "source_value": Decimal("2.0"),
            "normalized_value": Decimal("2.0"),
            "source_period": date(2026, 3, 1),
            "transformation_notes": "second",
            "created_at": now,
        },
    ]

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO audit.source_files (
                    source_file_id,
                    source,
                    dataset,
                    file_name,
                    file_path,
                    file_hash,
                    loaded_at
                )
                VALUES
                    ('file_a', 'test_loader', 'unit_test', 'file_a.csv', 'test/file_a.csv', 'hash_file_a', now()),
                    ('file_b', 'test_loader', 'unit_test', 'file_b.csv', 'test/file_b.csv', 'hash_file_b', now())
                ON CONFLICT (source_file_id) DO NOTHING
                """
            )
        )

        _bulk_upsert_source_traces(connection, params, page_size=100)

        row = connection.execute(
            text(
                """
                SELECT
                    source_file_id,
                    pipeline_run_id,
                    source_value,
                    normalized_value,
                    transformation_notes,
                    COUNT(*) OVER () AS rows
                FROM analytics.market_metric_sources
                WHERE geo_id = 'metro_19820'
                  AND period_month = DATE '2026-03-01'
                  AND metric_name = 'zhvi'
                  AND source = 'test_loader'
                  AND dataset = 'unit_test'
                """
            )
        ).mappings().one()

    assert row["source_file_id"] == "file_b"
    assert row["pipeline_run_id"] == "run_b"
    assert row["source_value"] == Decimal("2.0")
    assert row["normalized_value"] == Decimal("2.0")
    assert row["transformation_notes"] == "second"
    assert row["rows"] == 1

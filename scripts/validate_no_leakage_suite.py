from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine, text

from pipelines.ml.feature_contract import (
    EXPORT_COLUMNS,
    FEATURE_INPUT_COLUMNS,
    REQUIRED_QUALITY_FLAGS,
    TARGET_COLUMNS,
    assert_contract_valid,
)


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market",
)


SQL_FILES_TO_SCAN = [
    Path("pipelines/sql/story_11_1_market_features_monthly.sql"),
]


MANIFEST_PATHS = [
    Path("data/exports/features/latest_v1_trainable_only.manifest.json"),
    Path("data/exports/features/latest_v1_all_rows.manifest.json"),
]


def _assert_sql_generation_contract() -> None:
    for path in SQL_FILES_TO_SCAN:
        assert path.exists(), f"Missing SQL file: {path}"

        sql = path.read_text(encoding="utf-8").lower()

        assert "lead(" in sql, f"Expected target generation to use lead() in {path}"
        assert "target_price_growth_12m" in sql, path
        assert "point_in_time_safe" in sql, path
        assert "target_columns_separated" in sql, path

        feature_insert_section = sql.split("insert into analytics.market_features_monthly", 1)[-1]

        forbidden_input_patterns = [
            " as price_growth_12m\n        from",
        ]

        for pattern in forbidden_input_patterns:
            assert pattern not in feature_insert_section, (
                f"Suspicious feature generation pattern found in {path}: {pattern}"
            )

        for target_column in TARGET_COLUMNS:
            assert target_column in sql, f"Target column missing from SQL: {target_column}"


def _assert_manifest_contract() -> None:
    for path in MANIFEST_PATHS:
        assert path.exists(), f"Missing export manifest: {path}"

        manifest = json.loads(path.read_text(encoding="utf-8"))

        feature_inputs = set(manifest["feature_input_columns"])
        targets = set(manifest["target_columns"])

        assert feature_inputs == set(FEATURE_INPUT_COLUMNS), path
        assert targets == set(TARGET_COLUMNS), path
        assert feature_inputs.isdisjoint(targets), path
        assert manifest["leakage_guard"]["feature_inputs_exclude_targets"] is True, path
        assert manifest["leakage_guard"]["target_columns_prefixed"] is True, path
        assert manifest["duplicate_key_count"] == 0, path
        assert manifest["row_count"] > 0, path

        if manifest["export_mode"] == "trainable_only":
            assert manifest["trainable_rows"] == manifest["row_count"], path


def _assert_database_contract() -> None:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with engine.connect() as connection:
        feature_rows = connection.execute(
            text("SELECT COUNT(*) FROM analytics.market_features_monthly")
        ).scalar_one()
        assert feature_rows > 0, "market_features_monthly is empty"

        duplicate_key_groups = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT geo_id, period_month, feature_version, COUNT(*) AS rows
                    FROM analytics.market_features_monthly
                    GROUP BY geo_id, period_month, feature_version
                    HAVING COUNT(*) > 1
                ) duplicates
                """
            )
        ).scalar_one()
        assert duplicate_key_groups == 0, f"Duplicate feature key groups: {duplicate_key_groups}"

        unsafe_rows = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM analytics.market_features_monthly
                WHERE quality_flags ->> 'point_in_time_safe' <> 'true'
                   OR quality_flags ->> 'target_columns_separated' <> 'true'
                """
            )
        ).scalar_one()
        assert unsafe_rows == 0, f"Rows missing safety flags: {unsafe_rows}"

        source_after_feature_period = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM analytics.market_features_monthly
                WHERE source_period_max > period_month
                """
            )
        ).scalar_one()
        assert source_after_feature_period == 0, (
            f"Rows have source_period_max after period_month: {source_after_feature_period}"
        )

        missing_features_include_targets = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM analytics.market_features_monthly
                WHERE missing_feature_names::text ILIKE '%target_%'
                """
            )
        ).scalar_one()
        assert missing_features_include_targets == 0, (
            f"missing_feature_names includes target columns: {missing_features_include_targets}"
        )

        target_without_flag = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM analytics.market_features_monthly
                WHERE (
                    target_price_growth_12m IS NOT NULL
                    OR target_rent_growth_12m IS NOT NULL
                    OR target_drawdown_12m IS NOT NULL
                    OR target_cycle_phase_12m IS NOT NULL
                )
                AND target_available = false
                """
            )
        ).scalar_one()
        assert target_without_flag == 0, f"Rows have targets but target_available=false: {target_without_flag}"

        trainable_without_target = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM analytics.market_features_monthly
                WHERE is_trainable = true
                  AND target_price_growth_12m IS NULL
                """
            )
        ).scalar_one()
        assert trainable_without_target == 0, (
            f"Trainable rows missing target_price_growth_12m: {trainable_without_target}"
        )

        safety_flag_counts = connection.execute(
            text(
                """
                SELECT
                  COUNT(*) FILTER (WHERE quality_flags ->> 'point_in_time_safe' = 'true') AS point_in_time_safe_rows,
                  COUNT(*) FILTER (WHERE quality_flags ->> 'target_columns_separated' = 'true') AS target_separated_rows
                FROM analytics.market_features_monthly
                """
            )
        ).mappings().one()

        assert safety_flag_counts["point_in_time_safe_rows"] == feature_rows
        assert safety_flag_counts["target_separated_rows"] == feature_rows


def main() -> int:
    assert_contract_valid()

    assert set(FEATURE_INPUT_COLUMNS).isdisjoint(set(TARGET_COLUMNS))
    assert all(column.startswith("target_") for column in TARGET_COLUMNS)
    assert all(column in EXPORT_COLUMNS for column in FEATURE_INPUT_COLUMNS)
    assert all(column in EXPORT_COLUMNS for column in TARGET_COLUMNS)
    assert REQUIRED_QUALITY_FLAGS["point_in_time_safe"] is True
    assert REQUIRED_QUALITY_FLAGS["target_columns_separated"] is True

    _assert_sql_generation_contract()
    _assert_manifest_contract()
    _assert_database_contract()

    print("No-leakage validation suite passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

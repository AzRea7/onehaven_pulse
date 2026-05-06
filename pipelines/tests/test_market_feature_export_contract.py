from __future__ import annotations

from pipelines.ml.feature_contract import EXPORT_COLUMNS, FEATURE_INPUT_COLUMNS, TARGET_COLUMNS


def test_export_contains_primary_key_columns() -> None:
    assert "geo_id" in EXPORT_COLUMNS
    assert "period_month" in EXPORT_COLUMNS
    assert "feature_version" in EXPORT_COLUMNS


def test_export_contains_target_columns() -> None:
    for column in TARGET_COLUMNS:
        assert column in EXPORT_COLUMNS


def test_feature_inputs_exclude_targets() -> None:
    assert set(FEATURE_INPUT_COLUMNS).isdisjoint(set(TARGET_COLUMNS))


def test_target_columns_are_prefixed() -> None:
    assert all(column.startswith("target_") for column in TARGET_COLUMNS)


def test_export_contains_quality_columns() -> None:
    assert "is_trainable" in EXPORT_COLUMNS
    assert "target_available" in EXPORT_COLUMNS
    assert "feature_completeness_score" in EXPORT_COLUMNS
    assert "missing_feature_names" in EXPORT_COLUMNS

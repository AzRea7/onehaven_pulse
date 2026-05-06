from __future__ import annotations

from pipelines.ml.feature_contract import (
    EXPORT_COLUMNS,
    FEATURE_INPUT_COLUMNS,
    FORBIDDEN_FEATURE_PREFIXES,
    FORBIDDEN_FEATURE_SUBSTRINGS,
    PRIMARY_KEY_COLUMNS,
    REQUIRED_QUALITY_FLAGS,
    TARGET_COLUMNS,
    assert_contract_valid,
)


def test_feature_contract_is_valid() -> None:
    assert_contract_valid()


def test_feature_inputs_do_not_overlap_targets() -> None:
    assert set(FEATURE_INPUT_COLUMNS).isdisjoint(set(TARGET_COLUMNS))


def test_target_columns_are_explicitly_prefixed() -> None:
    assert all(column.startswith("target_") for column in TARGET_COLUMNS)


def test_feature_inputs_do_not_use_forbidden_prefixes() -> None:
    for column in FEATURE_INPUT_COLUMNS:
        for prefix in FORBIDDEN_FEATURE_PREFIXES:
            assert not column.startswith(prefix)


def test_feature_inputs_do_not_use_forbidden_substrings() -> None:
    for column in FEATURE_INPUT_COLUMNS:
        for substring in FORBIDDEN_FEATURE_SUBSTRINGS:
            assert substring not in column


def test_export_contains_keys_features_quality_and_targets() -> None:
    export_columns = set(EXPORT_COLUMNS)

    for column in PRIMARY_KEY_COLUMNS:
        assert column in export_columns

    for column in FEATURE_INPUT_COLUMNS:
        assert column in export_columns

    for column in TARGET_COLUMNS:
        assert column in export_columns


def test_quality_flags_contract() -> None:
    assert REQUIRED_QUALITY_FLAGS["point_in_time_safe"] is True
    assert REQUIRED_QUALITY_FLAGS["target_columns_separated"] is True

from __future__ import annotations


PRIMARY_KEY_COLUMNS = [
    "geo_id",
    "period_month",
    "feature_version",
]


IDENTITY_COLUMNS = [
    "geo_id",
    "period_month",
    "geo_type",
    "feature_version",
]


FEATURE_INPUT_COLUMNS = [
    "price_growth_1m",
    "price_growth_3m",
    "price_growth_12m",
    "rent_growth_1m",
    "rent_growth_3m",
    "rent_growth_12m",
    "inventory_change_3m",
    "inventory_change_12m",
    "days_on_market_change_3m",
    "mortgage_rate_30y",
    "rate_change_3m",
    "unemployment_rate",
    "unemployment_change_3m",
    "price_to_income_ratio",
    "payment_to_income_ratio",
    "affordability_score",
    "cycle_score",
    "cycle_phase_encoded",
    "confidence_score",
    "has_price",
    "has_rent",
    "has_inventory",
    "has_affordability",
    "has_labor",
    "has_permits",
    "feature_completeness_score",
]


TARGET_COLUMNS = [
    "target_price_growth_12m",
    "target_rent_growth_12m",
    "target_drawdown_12m",
    "target_cycle_phase_12m",
]


QUALITY_COLUMNS = [
    "missing_feature_names",
    "source_period_max",
    "is_trainable",
    "quality_flags",
    "target_available",
]


EXPORT_COLUMNS = [
    *IDENTITY_COLUMNS,
    *FEATURE_INPUT_COLUMNS,
    *QUALITY_COLUMNS,
    *TARGET_COLUMNS,
]


FORBIDDEN_FEATURE_PREFIXES = [
    "target_",
    "future_",
    "lead_",
    "next_",
]


FORBIDDEN_FEATURE_SUBSTRINGS = [
    "_lead_",
    "forward_",
    "future",
    "target",
]


REQUIRED_QUALITY_FLAGS = {
    "point_in_time_safe": True,
    "target_columns_separated": True,
}


def assert_contract_valid() -> None:
    feature_set = set(FEATURE_INPUT_COLUMNS)
    target_set = set(TARGET_COLUMNS)
    export_set = set(EXPORT_COLUMNS)

    overlap = feature_set & target_set
    assert not overlap, f"Feature inputs overlap targets: {sorted(overlap)}"

    missing_export_features = feature_set - export_set
    assert not missing_export_features, f"Export missing feature inputs: {sorted(missing_export_features)}"

    missing_export_targets = target_set - export_set
    assert not missing_export_targets, f"Export missing targets: {sorted(missing_export_targets)}"

    assert all(column.startswith("target_") for column in TARGET_COLUMNS), TARGET_COLUMNS

    for column in FEATURE_INPUT_COLUMNS:
        for prefix in FORBIDDEN_FEATURE_PREFIXES:
            assert not column.startswith(prefix), f"Feature column has forbidden prefix: {column}"

        for substring in FORBIDDEN_FEATURE_SUBSTRINGS:
            assert substring not in column, f"Feature column has forbidden leakage substring: {column}"

    assert len(EXPORT_COLUMNS) == len(set(EXPORT_COLUMNS)), "Duplicate export columns found"

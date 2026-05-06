from __future__ import annotations


FEATURE_COLUMNS = {
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
}


TARGET_COLUMNS = {
    "target_price_growth_12m",
    "target_rent_growth_12m",
    "target_drawdown_12m",
    "target_cycle_phase_12m",
}


def test_features_and_targets_do_not_overlap() -> None:
    assert FEATURE_COLUMNS.isdisjoint(TARGET_COLUMNS)


def test_all_target_columns_are_prefixed() -> None:
    assert all(column.startswith("target_") for column in TARGET_COLUMNS)


def test_no_feature_column_uses_target_prefix() -> None:
    assert not [column for column in FEATURE_COLUMNS if column.startswith("target_")]

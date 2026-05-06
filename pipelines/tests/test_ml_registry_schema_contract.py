from __future__ import annotations


MODEL_STATUSES = {
    "placeholder",
    "training",
    "validated",
    "active",
    "deprecated",
    "failed",
}


MODEL_REGISTRY_REQUIRED_COLUMNS = {
    "model_id",
    "model_name",
    "model_version",
    "model_type",
    "prediction_target",
    "prediction_horizon_months",
    "feature_version",
    "metrics",
    "feature_columns",
    "target_columns",
    "status",
}


PREDICTION_REQUIRED_COLUMNS = {
    "prediction_id",
    "model_id",
    "geo_id",
    "period_month",
    "prediction_target",
    "prediction_horizon_months",
    "predicted_value",
    "prediction_interval_low",
    "prediction_interval_high",
    "confidence_score",
    "feature_version",
    "input_feature_period",
}


def test_model_status_contract() -> None:
    assert "placeholder" in MODEL_STATUSES
    assert "active" in MODEL_STATUSES
    assert "deprecated" in MODEL_STATUSES


def test_registry_has_feature_and_target_contract_columns() -> None:
    assert "feature_columns" in MODEL_REGISTRY_REQUIRED_COLUMNS
    assert "target_columns" in MODEL_REGISTRY_REQUIRED_COLUMNS
    assert "feature_version" in MODEL_REGISTRY_REQUIRED_COLUMNS


def test_prediction_schema_has_traceability_columns() -> None:
    assert "model_id" in PREDICTION_REQUIRED_COLUMNS
    assert "geo_id" in PREDICTION_REQUIRED_COLUMNS
    assert "input_feature_period" in PREDICTION_REQUIRED_COLUMNS
    assert "feature_version" in PREDICTION_REQUIRED_COLUMNS


def test_prediction_columns_do_not_replace_feature_table() -> None:
    assert "price_growth_12m" not in PREDICTION_REQUIRED_COLUMNS
    assert "rent_growth_12m" not in PREDICTION_REQUIRED_COLUMNS

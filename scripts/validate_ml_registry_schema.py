from __future__ import annotations

import os

from sqlalchemy import create_engine, text


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market",
)


REQUIRED_TABLES = {
    "ml_model_registry",
    "ml_predictions",
}


REQUIRED_MODEL_REGISTRY_COLUMNS = {
    "model_id",
    "model_name",
    "model_version",
    "model_type",
    "prediction_target",
    "prediction_horizon_months",
    "feature_version",
    "training_start_period",
    "training_end_period",
    "validation_start_period",
    "validation_end_period",
    "metrics",
    "feature_columns",
    "target_columns",
    "training_row_count",
    "validation_row_count",
    "status",
    "artifact_uri",
    "notes",
    "created_at",
    "updated_at",
}


REQUIRED_PREDICTION_COLUMNS = {
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
    "input_feature_completeness_score",
    "input_target_available",
    "explanation",
    "created_at",
}


def _columns(connection, table_name: str) -> set[str]:
    rows = connection.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'analytics'
              AND table_name = :table_name
            """
        ),
        {"table_name": table_name},
    ).fetchall()

    return {row[0] for row in rows}


def main() -> int:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with engine.connect() as connection:
        tables = connection.execute(
            text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'analytics'
                  AND table_name IN ('ml_model_registry', 'ml_predictions')
                """
            )
        ).fetchall()

        found_tables = {row[0] for row in tables}
        missing_tables = REQUIRED_TABLES - found_tables
        assert not missing_tables, f"Missing ML tables: {sorted(missing_tables)}"

        model_registry_columns = _columns(connection, "ml_model_registry")
        prediction_columns = _columns(connection, "ml_predictions")

        missing_registry_columns = REQUIRED_MODEL_REGISTRY_COLUMNS - model_registry_columns
        missing_prediction_columns = REQUIRED_PREDICTION_COLUMNS - prediction_columns

        assert not missing_registry_columns, f"Missing registry columns: {sorted(missing_registry_columns)}"
        assert not missing_prediction_columns, f"Missing prediction columns: {sorted(missing_prediction_columns)}"

        feature_rows = connection.execute(
            text("SELECT COUNT(*) FROM analytics.market_features_monthly")
        ).scalar_one()

        trainable_rows = connection.execute(
            text("SELECT COUNT(*) FROM analytics.market_features_monthly WHERE is_trainable")
        ).scalar_one()

        assert feature_rows > 0, "Feature table must contain rows before ML registry is useful."
        assert trainable_rows > 0, "Feature table must contain trainable rows before ML registry is useful."

        prediction_rows = connection.execute(
            text("SELECT COUNT(*) FROM analytics.ml_predictions")
        ).scalar_one()

        # Story 11.2 is schema-only. Predictions should remain empty until future model stories.
        assert prediction_rows == 0, f"Expected no prediction rows in Story 11.2, found {prediction_rows}"

    print("ML registry schema validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

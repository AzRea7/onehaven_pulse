"""add performance guardrail indexes

Revision ID: 0021_performance_guardrails
Revises: 0020_pipeline_observability
Create Date: 2026-05-05
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0021_performance_guardrails"
down_revision: str | None = "0020_pipeline_observability"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_analytics_market_monthly_metrics_geo_period_desc
            ON analytics.market_monthly_metrics (geo_id, period_month DESC);

        CREATE INDEX IF NOT EXISTS ix_analytics_market_monthly_metrics_period_desc
            ON analytics.market_monthly_metrics (period_month DESC);

        CREATE INDEX IF NOT EXISTS ix_analytics_market_monthly_metrics_geo_latest
            ON analytics.market_monthly_metrics (geo_id, period_month DESC)
            INCLUDE (
                home_price_index,
                zhvi,
                median_sale_price,
                zori,
                median_rent,
                mortgage_rate_30y,
                unemployment_rate,
                payment_to_income_ratio,
                price_to_income_ratio
            );

        CREATE INDEX IF NOT EXISTS ix_analytics_market_metric_sources_geo_period_metric
            ON analytics.market_metric_sources (geo_id, period_month DESC, metric_name);

        CREATE INDEX IF NOT EXISTS ix_analytics_market_metric_sources_metric_period
            ON analytics.market_metric_sources (metric_name, period_month DESC);

        CREATE INDEX IF NOT EXISTS ix_geo_dim_geo_geo_type_active
            ON geo.dim_geo (geo_type, is_active);

        CREATE INDEX IF NOT EXISTS ix_geo_dim_geo_name_search
            ON geo.dim_geo USING gin (
                to_tsvector(
                    'simple',
                    coalesce(name, '') || ' ' ||
                    coalesce(display_name, '') || ' ' ||
                    coalesce(state_code, '')
                )
            );

        CREATE INDEX IF NOT EXISTS ix_geo_source_geo_crosswalk_geo_id
            ON geo.source_geo_crosswalk (geo_id);

        CREATE INDEX IF NOT EXISTS ix_geo_source_geo_crosswalk_source_dataset_region
            ON geo.source_geo_crosswalk (source, dataset, source_region_id);

        CREATE INDEX IF NOT EXISTS ix_audit_source_freshness_source_dataset
            ON audit.source_freshness (source, dataset);

        CREATE INDEX IF NOT EXISTS ix_audit_pipeline_runs_source_dataset_status_started
            ON audit.pipeline_runs (source, dataset, status, started_at DESC);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS audit.ix_audit_pipeline_runs_source_dataset_status_started;
        DROP INDEX IF EXISTS audit.ix_audit_source_freshness_source_dataset;

        DROP INDEX IF EXISTS geo.ix_geo_source_geo_crosswalk_source_dataset_region;
        DROP INDEX IF EXISTS geo.ix_geo_source_geo_crosswalk_geo_id;
        DROP INDEX IF EXISTS geo.ix_geo_dim_geo_name_search;
        DROP INDEX IF EXISTS geo.ix_geo_dim_geo_geo_type_active;

        DROP INDEX IF EXISTS analytics.ix_analytics_market_metric_sources_metric_period;
        DROP INDEX IF EXISTS analytics.ix_analytics_market_metric_sources_geo_period_metric;
        DROP INDEX IF EXISTS analytics.ix_analytics_market_monthly_metrics_geo_latest;
        DROP INDEX IF EXISTS analytics.ix_analytics_market_monthly_metrics_period_desc;
        DROP INDEX IF EXISTS analytics.ix_analytics_market_monthly_metrics_geo_period_desc;
        """
    )

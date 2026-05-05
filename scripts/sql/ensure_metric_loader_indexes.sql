-- Metric loader safety indexes.
-- Required for bulk ON CONFLICT upserts.

CREATE UNIQUE INDEX IF NOT EXISTS ux_market_monthly_metrics_geo_period
ON analytics.market_monthly_metrics (
    geo_id,
    period_month
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_market_metric_sources_identity
ON analytics.market_metric_sources (
    geo_id,
    period_month,
    metric_name,
    source,
    dataset
);

CREATE INDEX IF NOT EXISTS ix_market_metric_sources_geo_source_metric_period
ON analytics.market_metric_sources (
    geo_id,
    source,
    metric_name,
    period_month
);

CREATE INDEX IF NOT EXISTS ix_market_metric_sources_source_dataset_metric
ON analytics.market_metric_sources (
    source,
    dataset,
    metric_name
);

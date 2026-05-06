select
    geo_id,
    period_month,
    metric_name,
    source,
    dataset
from {{ source('analytics', 'market_metric_sources') }}

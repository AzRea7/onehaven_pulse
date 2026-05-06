select *
from {{ source('analytics', 'market_metric_sources') }}

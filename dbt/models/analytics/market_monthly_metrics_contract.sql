select *
from {{ source('analytics', 'market_monthly_metrics') }}

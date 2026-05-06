

select *
from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
where home_price_index is not null
  and home_price_index < 0


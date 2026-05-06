

select *
from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
where zori is not null
  and zori < 0


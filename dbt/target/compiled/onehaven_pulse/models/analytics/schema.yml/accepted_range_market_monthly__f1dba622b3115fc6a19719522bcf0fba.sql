

select *
from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
where mortgage_rate_30y is not null

  and mortgage_rate_30y < 0


  and mortgage_rate_30y > 25



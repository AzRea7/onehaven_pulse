

select *
from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
where unemployment_rate is not null

  and unemployment_rate < 0


  and unemployment_rate > 40



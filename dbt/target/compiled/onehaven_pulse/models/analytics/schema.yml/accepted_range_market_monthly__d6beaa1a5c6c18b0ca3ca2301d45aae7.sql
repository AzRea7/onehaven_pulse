

select *
from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
where price_to_income_ratio is not null

  and price_to_income_ratio < 0


  and price_to_income_ratio > 50





select *
from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
where payment_to_income_ratio is not null

  and payment_to_income_ratio < 0


  and payment_to_income_ratio > 3



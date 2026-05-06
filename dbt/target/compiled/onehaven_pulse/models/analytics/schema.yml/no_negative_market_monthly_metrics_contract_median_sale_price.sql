

select *
from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
where median_sale_price is not null
  and median_sale_price < 0


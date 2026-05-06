

select *
from "onehaven_market"."dbt_dbt_staging"."stg_market_monthly_metrics"
where home_price_index is not null
  and home_price_index < 0


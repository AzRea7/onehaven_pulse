

select *
from "onehaven_market"."dbt_dbt_staging"."stg_market_monthly_metrics"
where median_sale_price is not null
  and median_sale_price < 0


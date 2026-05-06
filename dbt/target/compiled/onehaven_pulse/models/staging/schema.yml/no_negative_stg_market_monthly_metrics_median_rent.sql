

select *
from "onehaven_market"."dbt_dbt_staging"."stg_market_monthly_metrics"
where median_rent is not null
  and median_rent < 0


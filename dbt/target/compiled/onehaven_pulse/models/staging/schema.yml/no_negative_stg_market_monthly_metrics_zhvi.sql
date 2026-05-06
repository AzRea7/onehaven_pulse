

select *
from "onehaven_market"."dbt_dbt_staging"."stg_market_monthly_metrics"
where zhvi is not null
  and zhvi < 0


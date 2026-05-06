

select *
from "onehaven_market"."dbt_dbt_staging"."stg_market_monthly_metrics"
where zori is not null
  and zori < 0


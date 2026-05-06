

select *
from "onehaven_market"."dbt_dbt_staging"."stg_market_monthly_metrics"
where mortgage_rate_30y is not null

  and mortgage_rate_30y < 0


  and mortgage_rate_30y > 25



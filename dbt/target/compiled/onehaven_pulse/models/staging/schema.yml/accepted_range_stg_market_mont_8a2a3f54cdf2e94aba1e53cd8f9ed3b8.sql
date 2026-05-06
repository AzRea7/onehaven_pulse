

select *
from "onehaven_market"."dbt_dbt_staging"."stg_market_monthly_metrics"
where unemployment_rate is not null

  and unemployment_rate < 0


  and unemployment_rate > 40



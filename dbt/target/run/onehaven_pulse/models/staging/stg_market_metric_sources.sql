
  create view "onehaven_market"."dbt_dbt_staging"."stg_market_metric_sources__dbt_tmp"
    
    
  as (
    select
    geo_id,
    period_month,
    metric_name,
    source,
    dataset
from "onehaven_market"."analytics"."market_metric_sources"
  );
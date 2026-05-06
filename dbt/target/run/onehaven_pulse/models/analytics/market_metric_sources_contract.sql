
  create view "onehaven_market"."dbt_dbt_analytics_contract"."market_metric_sources_contract__dbt_tmp"
    
    
  as (
    select *
from "onehaven_market"."analytics"."market_metric_sources"
  );

  create view "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract__dbt_tmp"
    
    
  as (
    select *
from "onehaven_market"."analytics"."market_monthly_metrics"
  );
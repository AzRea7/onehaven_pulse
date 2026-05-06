
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period_month
from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
where period_month is null



  
  
      
    ) dbt_internal_test
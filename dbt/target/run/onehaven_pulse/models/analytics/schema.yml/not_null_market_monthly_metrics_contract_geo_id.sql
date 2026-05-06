
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select geo_id
from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
where geo_id is null



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dataset
from "onehaven_market"."dbt_dbt_analytics_contract"."market_metric_sources_contract"
where dataset is null



  
  
      
    ) dbt_internal_test
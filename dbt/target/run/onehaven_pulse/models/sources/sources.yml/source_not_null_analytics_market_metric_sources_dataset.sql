
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dataset
from "onehaven_market"."analytics"."market_metric_sources"
where dataset is null



  
  
      
    ) dbt_internal_test
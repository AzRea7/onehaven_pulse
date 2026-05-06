
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source
from "onehaven_market"."analytics"."market_metric_sources"
where source is null



  
  
      
    ) dbt_internal_test
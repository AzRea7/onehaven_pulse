
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select geo_id
from "onehaven_market"."analytics"."market_metric_sources"
where geo_id is null



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select metric_name
from "onehaven_market"."dbt_dbt_staging"."stg_market_metric_sources"
where metric_name is null



  
  
      
    ) dbt_internal_test
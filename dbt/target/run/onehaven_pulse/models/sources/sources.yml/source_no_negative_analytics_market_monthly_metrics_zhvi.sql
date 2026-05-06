
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "onehaven_market"."analytics"."market_monthly_metrics"
where zhvi is not null
  and zhvi < 0


  
  
      
    ) dbt_internal_test
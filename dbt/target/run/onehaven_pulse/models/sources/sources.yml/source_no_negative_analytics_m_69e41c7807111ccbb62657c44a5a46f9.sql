
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "onehaven_market"."analytics"."market_monthly_metrics"
where home_price_index is not null
  and home_price_index < 0


  
  
      
    ) dbt_internal_test
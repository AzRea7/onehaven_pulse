
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "onehaven_market"."analytics"."market_monthly_metrics"
where mortgage_rate_30y is not null

  and mortgage_rate_30y < 0


  and mortgage_rate_30y > 25



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "onehaven_market"."analytics"."market_monthly_metrics"
where unemployment_rate is not null

  and unemployment_rate < 0


  and unemployment_rate > 40



  
  
      
    ) dbt_internal_test
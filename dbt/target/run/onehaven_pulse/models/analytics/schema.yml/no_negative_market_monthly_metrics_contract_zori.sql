
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
where zori is not null
  and zori < 0


  
  
      
    ) dbt_internal_test
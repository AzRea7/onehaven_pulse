
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
where unemployment_rate is not null

  and unemployment_rate < 0


  and unemployment_rate > 40



  
  
      
    ) dbt_internal_test
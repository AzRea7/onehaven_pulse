
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "onehaven_market"."dbt_dbt_staging"."stg_market_monthly_metrics"
where price_to_income_ratio is not null

  and price_to_income_ratio < 0


  and price_to_income_ratio > 50



  
  
      
    ) dbt_internal_test
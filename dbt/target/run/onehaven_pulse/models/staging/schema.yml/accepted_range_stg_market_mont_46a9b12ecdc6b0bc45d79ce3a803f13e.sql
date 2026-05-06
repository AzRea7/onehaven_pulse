
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "onehaven_market"."dbt_dbt_staging"."stg_market_monthly_metrics"
where payment_to_income_ratio is not null

  and payment_to_income_ratio < 0


  and payment_to_income_ratio > 3



  
  
      
    ) dbt_internal_test
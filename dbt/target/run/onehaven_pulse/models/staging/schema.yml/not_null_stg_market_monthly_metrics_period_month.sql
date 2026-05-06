
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period_month
from "onehaven_market"."dbt_dbt_staging"."stg_market_monthly_metrics"
where period_month is null



  
  
      
    ) dbt_internal_test
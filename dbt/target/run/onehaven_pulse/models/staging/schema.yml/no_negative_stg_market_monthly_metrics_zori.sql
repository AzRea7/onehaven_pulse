
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "onehaven_market"."dbt_dbt_staging"."stg_market_monthly_metrics"
where zori is not null
  and zori < 0


  
  
      
    ) dbt_internal_test
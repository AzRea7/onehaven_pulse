
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "onehaven_market"."dbt_dbt_staging"."stg_market_monthly_metrics"
where median_sale_price is not null
  and median_sale_price < 0


  
  
      
    ) dbt_internal_test
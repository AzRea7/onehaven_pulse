
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period_month
from "onehaven_market"."raw"."zillow_zori"
where period_month is null



  
  
      
    ) dbt_internal_test
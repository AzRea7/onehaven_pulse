
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period
from "onehaven_market"."raw"."fhfa_hpi"
where period is null



  
  
      
    ) dbt_internal_test
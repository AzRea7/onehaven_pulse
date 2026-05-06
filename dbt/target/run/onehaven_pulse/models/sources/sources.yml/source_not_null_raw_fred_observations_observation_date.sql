
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select observation_date
from "onehaven_market"."raw"."fred_observations"
where observation_date is null



  
  
      
    ) dbt_internal_test
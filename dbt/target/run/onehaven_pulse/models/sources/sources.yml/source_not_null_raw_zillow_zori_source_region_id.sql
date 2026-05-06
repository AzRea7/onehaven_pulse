
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_region_id
from "onehaven_market"."raw"."zillow_zori"
where source_region_id is null



  
  
      
    ) dbt_internal_test
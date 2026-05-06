
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "onehaven_market"."raw"."fhfa_hpi"
where hpi is not null
  and hpi < 0


  
  
      
    ) dbt_internal_test
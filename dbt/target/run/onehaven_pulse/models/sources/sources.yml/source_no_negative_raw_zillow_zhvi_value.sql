
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "onehaven_market"."raw"."zillow_zhvi"
where value is not null
  and value < 0


  
  
      
    ) dbt_internal_test
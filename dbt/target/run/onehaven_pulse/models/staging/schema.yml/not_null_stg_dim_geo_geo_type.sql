
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select geo_type
from "onehaven_market"."dbt_dbt_staging"."stg_dim_geo"
where geo_type is null



  
  
      
    ) dbt_internal_test
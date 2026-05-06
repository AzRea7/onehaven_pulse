
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select geo_id as from_field
    from "onehaven_market"."dbt_dbt_analytics_contract"."market_metric_sources_contract"
    where geo_id is not null
),

parent as (
    select geo_id as to_field
    from "onehaven_market"."dbt_dbt_staging"."stg_dim_geo"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test

    
    

with child as (
    select geo_id as from_field
    from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
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



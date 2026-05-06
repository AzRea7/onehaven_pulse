
    
    

with child as (
    select geo_id as from_field
    from "onehaven_market"."analytics"."market_metric_sources"
    where geo_id is not null
),

parent as (
    select geo_id as to_field
    from "onehaven_market"."geo"."dim_geo"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



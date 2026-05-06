
    
    

with child as (
    select metric_name as from_field
    from "onehaven_market"."dbt_dbt_analytics_contract"."market_metric_sources_contract"
    where metric_name is not null
),

parent as (
    select metric_name as to_field
    from "onehaven_market"."dbt_dbt_reference"."valid_metric_names"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



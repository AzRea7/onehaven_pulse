

with validation as (
    select
        geo_id, period_month,
        count(*) as row_count
    from "onehaven_market"."dbt_dbt_analytics_contract"."market_monthly_metrics_contract"
    group by geo_id, period_month
)

select *
from validation
where row_count > 1


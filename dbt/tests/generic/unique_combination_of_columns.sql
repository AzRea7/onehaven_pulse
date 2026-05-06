{% test unique_combination_of_columns(model, combination_of_columns) %}

with validation as (
    select
        {{ combination_of_columns | join(', ') }},
        count(*) as row_count
    from {{ model }}
    group by {{ combination_of_columns | join(', ') }}
)

select *
from validation
where row_count > 1

{% endtest %}

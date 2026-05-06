select
    geo_id,
    geo_type,
    name,
    display_name,
    state_code,
    is_active
from {{ source('geo', 'dim_geo') }}

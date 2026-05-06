
  create view "onehaven_market"."dbt_dbt_staging"."stg_dim_geo__dbt_tmp"
    
    
  as (
    select
    geo_id,
    geo_type,
    name,
    display_name,
    state_code,
    is_active
from "onehaven_market"."geo"."dim_geo"
  );
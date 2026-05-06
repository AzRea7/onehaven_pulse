
    
    

select
    geo_id as unique_field,
    count(*) as n_records

from "onehaven_market"."geo"."dim_geo"
where geo_id is not null
group by geo_id
having count(*) > 1



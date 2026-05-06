

select *
from "onehaven_market"."raw"."fhfa_hpi"
where hpi is not null
  and hpi < 0


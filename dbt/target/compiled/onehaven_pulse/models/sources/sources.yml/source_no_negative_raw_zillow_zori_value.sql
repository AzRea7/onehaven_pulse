

select *
from "onehaven_market"."raw"."zillow_zori"
where value is not null
  and value < 0


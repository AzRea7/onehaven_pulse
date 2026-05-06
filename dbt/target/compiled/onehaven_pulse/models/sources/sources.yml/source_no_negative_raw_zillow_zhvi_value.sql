

select *
from "onehaven_market"."raw"."zillow_zhvi"
where value is not null
  and value < 0


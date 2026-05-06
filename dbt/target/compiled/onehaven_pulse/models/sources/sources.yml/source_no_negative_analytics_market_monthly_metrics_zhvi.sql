

select *
from "onehaven_market"."analytics"."market_monthly_metrics"
where zhvi is not null
  and zhvi < 0


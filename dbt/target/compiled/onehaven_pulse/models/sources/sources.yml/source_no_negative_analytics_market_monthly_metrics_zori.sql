

select *
from "onehaven_market"."analytics"."market_monthly_metrics"
where zori is not null
  and zori < 0




select *
from "onehaven_market"."analytics"."market_monthly_metrics"
where median_rent is not null
  and median_rent < 0




select *
from "onehaven_market"."analytics"."market_monthly_metrics"
where median_sale_price is not null
  and median_sale_price < 0


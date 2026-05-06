

select *
from "onehaven_market"."analytics"."market_monthly_metrics"
where price_to_income_ratio is not null

  and price_to_income_ratio < 0


  and price_to_income_ratio > 50



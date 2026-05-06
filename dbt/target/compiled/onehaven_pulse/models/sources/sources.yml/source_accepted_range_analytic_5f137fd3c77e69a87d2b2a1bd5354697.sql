

select *
from "onehaven_market"."analytics"."market_monthly_metrics"
where payment_to_income_ratio is not null

  and payment_to_income_ratio < 0


  and payment_to_income_ratio > 3



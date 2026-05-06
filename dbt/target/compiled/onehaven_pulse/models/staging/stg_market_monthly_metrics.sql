select
    geo_id,
    period_month,
    home_price_index,
    zhvi,
    median_sale_price,
    zori,
    median_rent,
    mortgage_rate_30y,
    unemployment_rate,
    estimated_monthly_payment,
    payment_to_income_ratio,
    price_to_income_ratio
from "onehaven_market"."analytics"."market_monthly_metrics"
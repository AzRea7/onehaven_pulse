\echo '== Story 9.4 latest relevant transform runs =='

SELECT
  id,
  pipeline_name,
  source,
  dataset,
  status,
  records_extracted,
  records_loaded,
  records_failed,
  unmatched_count,
  error_message,
  started_at,
  finished_at
FROM audit.pipeline_runs
WHERE pipeline_name IN (
    'census_acs_profile_transform',
    'fred_macro_monthly_transform',
    'zillow_value_rent_transform',
    'derived_market_ratios_transform'
)
OR pipeline_name ILIKE '%acs%'
OR pipeline_name ILIKE '%fred%'
OR pipeline_name ILIKE '%derived%'
OR pipeline_name ILIKE '%ratio%'
ORDER BY started_at DESC
LIMIT 20;

\echo '== Story 9.4 income / mortgage / affordability metric coverage =='

SELECT
  source,
  dataset,
  metric_name,
  COUNT(*) AS metric_rows,
  COUNT(DISTINCT geo_id) AS geo_count,
  MIN(period_month) AS min_period,
  MAX(period_month) AS max_period
FROM analytics.market_metric_sources
WHERE metric_name IN (
  'median_household_income',
  'mortgage_rate_30y',
  'price_to_income_ratio',
  'estimated_monthly_payment',
  'payment_to_income_ratio'
)
OR metric_name ILIKE '%income%'
OR metric_name ILIKE '%mortgage%'
OR metric_name ILIKE '%payment%'
OR metric_name ILIKE '%afford%'
GROUP BY source, dataset, metric_name
ORDER BY metric_name, source, dataset;

\echo '== Story 9.4 priority affordability coverage =='

WITH priority_markets AS (
    SELECT *
    FROM (
        VALUES
            ('metro_19820', 'Detroit-Warren-Dearborn, MI'),
            ('metro_16980', 'Chicago-Naperville-Elgin, IL-IN-WI'),
            ('metro_19100', 'Dallas-Fort Worth-Arlington, TX'),
            ('metro_12420', 'Austin-Round Rock-Georgetown, TX'),
            ('metro_45300', 'Tampa-St. Petersburg-Clearwater, FL'),
            ('metro_38060', 'Phoenix-Mesa-Chandler, AZ'),
            ('metro_12060', 'Atlanta-Sandy Springs-Alpharetta, GA'),
            ('metro_42660', 'Seattle-Tacoma-Bellevue, WA'),
            ('metro_14460', 'Boston-Cambridge-Newton, MA-NH'),
            ('metro_31080', 'Los Angeles-Long Beach-Anaheim, CA'),
            ('metro_37980', 'Philadelphia-Camden-Wilmington, PA-NJ-DE-MD')
    ) AS t(geo_id, market_name)
),
coverage AS (
    SELECT
        geo_id,
        COUNT(*) FILTER (WHERE metric_name = 'zhvi') AS zhvi_rows,
        COUNT(*) FILTER (WHERE metric_name = 'median_household_income') AS income_rows,
        COUNT(*) FILTER (WHERE metric_name = 'mortgage_rate_30y') AS mortgage_rate_rows,
        COUNT(*) FILTER (WHERE metric_name = 'price_to_income_ratio') AS price_to_income_rows,
        COUNT(*) FILTER (WHERE metric_name = 'estimated_monthly_payment') AS estimated_payment_rows,
        COUNT(*) FILTER (WHERE metric_name = 'payment_to_income_ratio') AS payment_to_income_rows,
        MAX(period_month) FILTER (WHERE metric_name = 'payment_to_income_ratio') AS latest_payment_to_income_period
    FROM analytics.market_metric_sources
    GROUP BY geo_id
)
SELECT
    p.geo_id,
    p.market_name,
    COALESCE(c.zhvi_rows, 0) AS zhvi_rows,
    COALESCE(c.income_rows, 0) AS income_rows,
    COALESCE(c.mortgage_rate_rows, 0) AS mortgage_rate_rows,
    COALESCE(c.price_to_income_rows, 0) AS price_to_income_rows,
    COALESCE(c.estimated_payment_rows, 0) AS estimated_payment_rows,
    COALESCE(c.payment_to_income_rows, 0) AS payment_to_income_rows,
    c.latest_payment_to_income_period
FROM priority_markets p
LEFT JOIN coverage c
  ON c.geo_id = p.geo_id
ORDER BY p.geo_id;

\echo '== Story 9.4 latest priority affordability values =='

SELECT
  geo_id,
  period_month,
  zhvi,
  median_household_income,
  mortgage_rate_30y,
  price_to_income_ratio,
  estimated_monthly_payment,
  payment_to_income_ratio,
  source_flags -> 'median_household_income' AS income_source,
  source_flags -> 'mortgage_rate_30y' AS mortgage_source,
  source_flags -> 'price_to_income_ratio' AS price_to_income_source,
  source_flags -> 'estimated_monthly_payment' AS estimated_payment_source,
  source_flags -> 'payment_to_income_ratio' AS payment_to_income_source
FROM analytics.market_monthly_metrics
WHERE geo_id IN (
    'metro_19820',
    'metro_16980',
    'metro_19100',
    'metro_12420',
    'metro_45300',
    'metro_38060',
    'metro_12060',
    'metro_42660',
    'metro_14460',
    'metro_31080',
    'metro_37980'
)
ORDER BY geo_id, period_month DESC
LIMIT 60;

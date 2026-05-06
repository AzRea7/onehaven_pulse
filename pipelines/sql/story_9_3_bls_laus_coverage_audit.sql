\echo '== Story 9.3 BLS LAUS latest transform runs =='

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
WHERE source = 'bls_laus'
   OR pipeline_name ILIKE '%laus%'
   OR pipeline_name ILIKE '%bls%'
ORDER BY started_at DESC
LIMIT 10;

\echo '== Story 9.3 current labor metric coverage =='

SELECT
  metric_name,
  COUNT(*) AS metric_rows,
  COUNT(DISTINCT geo_id) AS geo_count,
  MIN(period_month) AS min_period,
  MAX(period_month) AS max_period
FROM analytics.market_metric_sources
WHERE source = 'bls_laus'
GROUP BY metric_name
ORDER BY metric_name;

\echo '== Story 9.3 metro labor coverage count =='

SELECT
  g.geo_type,
  COUNT(DISTINCT m.geo_id) AS geos_with_unemployment_rate
FROM analytics.market_metric_sources m
JOIN geo.dim_geo g
  ON g.geo_id = m.geo_id
WHERE m.source = 'bls_laus'
  AND m.metric_name = 'unemployment_rate'
GROUP BY g.geo_type
ORDER BY g.geo_type;

\echo '== Story 9.3 priority labor coverage =='

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
        BOOL_OR(metric_name = 'unemployment_rate') AS has_unemployment_rate,
        MIN(period_month) AS min_labor_period,
        MAX(period_month) AS max_labor_period
    FROM analytics.market_metric_sources
    WHERE source = 'bls_laus'
      AND metric_name = 'unemployment_rate'
    GROUP BY geo_id
)
SELECT
    p.geo_id,
    p.market_name,
    COALESCE(c.has_unemployment_rate, false) AS has_unemployment_rate,
    c.min_labor_period,
    c.max_labor_period
FROM priority_markets p
LEFT JOIN coverage c
  ON c.geo_id = p.geo_id
ORDER BY p.geo_id;

\echo '== Story 9.3 BLS crosswalk rows =='

SELECT
  source_geo_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT canonical_geo_id) AS canonical_geos
FROM geo.geo_crosswalk
WHERE source = 'bls_laus'
  AND is_active = true
GROUP BY source_geo_type
ORDER BY source_geo_type;

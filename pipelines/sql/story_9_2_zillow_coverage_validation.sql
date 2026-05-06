\echo '== Story 9.2 Zillow transform audit =='

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
WHERE source = 'zillow'
   OR pipeline_name ILIKE '%zillow%'
ORDER BY started_at DESC
LIMIT 10;

\echo '== Story 9.2 Zillow metric coverage by dataset =='

SELECT
  dataset,
  metric_name,
  COUNT(*) AS metric_rows,
  COUNT(DISTINCT geo_id) AS geo_count,
  MIN(period_month) AS min_period,
  MAX(period_month) AS max_period
FROM analytics.market_metric_sources
WHERE source = 'zillow'
GROUP BY dataset, metric_name
ORDER BY dataset, metric_name;

\echo '== Story 9.2 priority market Zillow coverage =='

WITH priority_markets AS (
    SELECT *
    FROM (
        VALUES
            ('us', 'United States'),
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
        BOOL_OR(source = 'zillow' AND dataset = 'zhvi') AS has_zhvi,
        BOOL_OR(source = 'zillow' AND dataset = 'zori') AS has_zori,
        MIN(period_month) FILTER (WHERE source = 'zillow') AS min_zillow_period,
        MAX(period_month) FILTER (WHERE source = 'zillow') AS max_zillow_period
    FROM analytics.market_metric_sources
    WHERE source = 'zillow'
    GROUP BY geo_id
)
SELECT
    p.geo_id,
    p.market_name,
    COALESCE(c.has_zhvi, false) AS has_zhvi,
    COALESCE(c.has_zori, false) AS has_zori,
    c.min_zillow_period,
    c.max_zillow_period
FROM priority_markets p
LEFT JOIN coverage c
    ON c.geo_id = p.geo_id
ORDER BY p.geo_id;

\echo '== Story 9.2 Zillow crosswalk count =='

SELECT
  source_geo_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT canonical_geo_id) AS canonical_geos
FROM geo.geo_crosswalk
WHERE source = 'zillow'
  AND is_active = true
GROUP BY source_geo_type
ORDER BY source_geo_type;

\echo '== Story 9.2 Zillow raw table row counts =='

SELECT
  table_schema,
  table_name,
  pg_total_relation_size(format('%I.%I', table_schema, table_name)) AS total_bytes
FROM information_schema.tables
WHERE table_schema = 'raw'
  AND table_name ILIKE '%zillow%'
ORDER BY table_name;

\echo '== Story 9.2 Zillow transformed metric coverage =='

SELECT
  source,
  dataset,
  metric_name,
  COUNT(*) AS metric_rows,
  COUNT(DISTINCT geo_id) AS geo_count,
  MIN(period_month) AS min_period,
  MAX(period_month) AS max_period
FROM analytics.market_metric_sources
WHERE source = 'zillow'
GROUP BY source, dataset, metric_name
ORDER BY dataset, metric_name;

\echo '== Story 9.2 Zillow crosswalk rows =='

SELECT
  source,
  source_geo_type,
  COUNT(*) AS rows,
  COUNT(DISTINCT canonical_geo_id) AS canonical_geos
FROM geo.geo_crosswalk
WHERE source = 'zillow'
  AND is_active = true
GROUP BY source, source_geo_type
ORDER BY source_geo_type;

\echo '== Story 9.2 top canonical metros missing Zillow ZHVI/ZORI =='

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
        BOOL_OR(source = 'zillow' AND dataset = 'zori') AS has_zori
    FROM analytics.market_metric_sources
    WHERE source = 'zillow'
    GROUP BY geo_id
)
SELECT
    p.geo_id,
    p.market_name,
    COALESCE(c.has_zhvi, false) AS has_zhvi,
    COALESCE(c.has_zori, false) AS has_zori
FROM priority_markets p
LEFT JOIN coverage c
    ON c.geo_id = p.geo_id
ORDER BY p.geo_id;

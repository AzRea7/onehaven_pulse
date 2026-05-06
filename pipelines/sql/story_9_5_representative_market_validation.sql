\echo '== Story 9.5 representative market canonical geography check =='

WITH representative_markets AS (
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
    ) AS t(geo_id, expected_name)
)
SELECT
    r.geo_id,
    r.expected_name,
    g.geo_type,
    g.name,
    g.display_name,
    CASE WHEN g.geo_id IS NULL THEN false ELSE true END AS exists_in_dim_geo
FROM representative_markets r
LEFT JOIN geo.dim_geo g
  ON g.geo_id = r.geo_id
ORDER BY r.geo_id;

\echo '== Story 9.5 representative metric coverage from market_monthly_metrics =='

WITH representative_markets AS (
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
        COUNT(*) FILTER (WHERE zhvi IS NOT NULL OR zhvi_yoy IS NOT NULL) AS price_rows,
        COUNT(*) FILTER (WHERE zori IS NOT NULL OR zori_yoy IS NOT NULL) AS rent_rows,
        COUNT(*) FILTER (
            WHERE active_listings IS NOT NULL
               OR median_days_on_market IS NOT NULL
               OR months_supply IS NOT NULL
        ) AS inventory_rows,
        COUNT(*) FILTER (
            WHERE payment_to_income_ratio IS NOT NULL
               OR price_to_income_ratio IS NOT NULL
               OR estimated_monthly_payment IS NOT NULL
        ) AS affordability_rows,
        COUNT(*) FILTER (WHERE unemployment_rate IS NOT NULL) AS labor_rows,
        COUNT(*) FILTER (WHERE building_permits IS NOT NULL) AS permits_rows,
        MIN(period_month) AS min_period,
        MAX(period_month) AS max_period
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM representative_markets)
    GROUP BY geo_id
)
SELECT
    r.geo_id,
    r.market_name,
    COALESCE(c.price_rows, 0) AS price_rows,
    COALESCE(c.rent_rows, 0) AS rent_rows,
    COALESCE(c.inventory_rows, 0) AS inventory_rows,
    COALESCE(c.affordability_rows, 0) AS affordability_rows,
    COALESCE(c.labor_rows, 0) AS labor_rows,
    COALESCE(c.permits_rows, 0) AS permits_rows,
    c.min_period,
    c.max_period
FROM representative_markets r
LEFT JOIN coverage c
  ON c.geo_id = r.geo_id
ORDER BY r.geo_id;

\echo '== Story 9.5 expected incomplete categories are explainable =='

WITH representative_markets AS (
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
        BOOL_OR(zhvi IS NOT NULL OR zhvi_yoy IS NOT NULL) AS has_price,
        BOOL_OR(zori IS NOT NULL OR zori_yoy IS NOT NULL) AS has_rent,
        BOOL_OR(active_listings IS NOT NULL OR median_days_on_market IS NOT NULL OR months_supply IS NOT NULL) AS has_inventory,
        BOOL_OR(payment_to_income_ratio IS NOT NULL OR price_to_income_ratio IS NOT NULL) AS has_affordability,
        BOOL_OR(unemployment_rate IS NOT NULL) AS has_labor,
        BOOL_OR(building_permits IS NOT NULL) AS has_permits
    FROM analytics.market_monthly_metrics
    WHERE geo_id IN (SELECT geo_id FROM representative_markets)
    GROUP BY geo_id
)
SELECT
    r.geo_id,
    r.market_name,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN NOT COALESCE(c.has_price, false) THEN 'price' END,
        CASE WHEN NOT COALESCE(c.has_rent, false) THEN 'rent' END,
        CASE WHEN NOT COALESCE(c.has_inventory, false) THEN 'inventory: missing Redfin inventory metrics for non-Detroit priority metros' END,
        CASE WHEN NOT COALESCE(c.has_affordability, false) THEN 'affordability' END,
        CASE WHEN NOT COALESCE(c.has_labor, false) THEN 'labor' END,
        CASE WHEN NOT COALESCE(c.has_permits, false) THEN 'permits: building permit coverage not part of Story 9.1-9.4' END
    ], NULL) AS missing_data_explanations
FROM representative_markets r
LEFT JOIN coverage c
  ON c.geo_id = r.geo_id
ORDER BY r.geo_id;

-- Epic 4 validation report
-- This report validates latest transform state, not stale historical failures.

\echo '== latest transform runs =='

WITH ranked_runs AS (
    SELECT
        pipeline_name,
        source,
        dataset,
        status,
        records_extracted,
        records_loaded,
        records_failed,
        error_message,
        started_at,
        finished_at,
        ROW_NUMBER() OVER (
            PARTITION BY pipeline_name
            ORDER BY started_at DESC
        ) AS row_number
    FROM audit.pipeline_runs
    WHERE pipeline_name IN (
        'fred_macro_monthly_transform',
        'zillow_value_rent_transform',
        'fhfa_hpi_transform',
        'redfin_market_tracker_transform',
        'census_acs_profile_transform',
        'bls_laus_labor_market_transform',
        'census_building_permits_transform',
        'fema_nri_hazard_risk_transform',
        'hmda_mortgage_credit_transform',
        'overture_places_amenity_transform',
        'derived_market_ratios_transform'
    )
)
SELECT
    pipeline_name,
    source,
    dataset,
    status,
    records_extracted,
    records_loaded,
    records_failed,
    error_message,
    started_at,
    finished_at
FROM ranked_runs
WHERE row_number = 1
ORDER BY pipeline_name;

\echo '== latest failed transform runs =='

WITH ranked_runs AS (
    SELECT
        pipeline_name,
        source,
        dataset,
        status,
        records_extracted,
        records_loaded,
        records_failed,
        error_message,
        started_at,
        finished_at,
        ROW_NUMBER() OVER (
            PARTITION BY pipeline_name
            ORDER BY started_at DESC
        ) AS row_number
    FROM audit.pipeline_runs
    WHERE pipeline_name IN (
        'fred_macro_monthly_transform',
        'zillow_value_rent_transform',
        'fhfa_hpi_transform',
        'redfin_market_tracker_transform',
        'census_acs_profile_transform',
        'bls_laus_labor_market_transform',
        'census_building_permits_transform',
        'fema_nri_hazard_risk_transform',
        'hmda_mortgage_credit_transform',
        'overture_places_amenity_transform',
        'derived_market_ratios_transform'
    )
)
SELECT
    pipeline_name,
    source,
    dataset,
    status,
    records_extracted,
    records_loaded,
    records_failed,
    error_message,
    started_at
FROM ranked_runs
WHERE row_number = 1
  AND status <> 'success'
ORDER BY pipeline_name;

\echo '== latest zero-loaded required transform runs =='

WITH required_transform_names AS (
    SELECT *
    FROM (
        VALUES
            ('fred_macro_monthly_transform'),
            ('zillow_value_rent_transform'),
            ('fhfa_hpi_transform'),
            ('redfin_market_tracker_transform'),
            ('census_acs_profile_transform'),
            ('bls_laus_labor_market_transform'),
            ('census_building_permits_transform'),
            ('fema_nri_hazard_risk_transform'),
            ('hmda_mortgage_credit_transform'),
            ('derived_market_ratios_transform')
    ) AS transform_names(pipeline_name)
),
ranked_runs AS (
    SELECT
        pipeline_name,
        source,
        dataset,
        status,
        records_extracted,
        records_loaded,
        records_failed,
        error_message,
        started_at,
        ROW_NUMBER() OVER (
            PARTITION BY pipeline_name
            ORDER BY started_at DESC
        ) AS row_number
    FROM audit.pipeline_runs
    WHERE pipeline_name IN (
        SELECT pipeline_name
        FROM required_transform_names
    )
)
SELECT
    pipeline_name,
    source,
    dataset,
    status,
    records_extracted,
    records_loaded,
    records_failed,
    error_message,
    started_at
FROM ranked_runs
WHERE row_number = 1
  AND status = 'success'
  AND COALESCE(records_loaded, 0) = 0
ORDER BY pipeline_name;

\echo '== metric coverage =='

SELECT
    metric_name,
    source,
    dataset,
    COUNT(*) AS rows
FROM analytics.market_metric_sources
GROUP BY metric_name, source, dataset
ORDER BY source, dataset, metric_name;

\echo '== source trace duplicates =='

SELECT
    geo_id,
    period_month,
    metric_name,
    source,
    dataset,
    COUNT(*) AS duplicate_count
FROM analytics.market_metric_sources
GROUP BY geo_id, period_month, metric_name, source, dataset
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, source, dataset, metric_name;

\echo '== mart row counts =='

SELECT
    COUNT(*) AS market_monthly_metrics_rows
FROM analytics.market_monthly_metrics;

SELECT
    COUNT(*) AS market_metric_sources_rows
FROM analytics.market_metric_sources;

\echo '== overture raw category health =='

SELECT
    area_slug,
    area_name,
    COUNT(*) AS rows,
    COUNT(primary_category) AS categorized_rows
FROM raw.overture_places
GROUP BY area_slug, area_name
ORDER BY rows DESC;

\echo '== hmda raw health =='

SELECT
    activity_year,
    COUNT(*) AS rows,
    COUNT(loan_amount) AS loan_amount_rows,
    COUNT(*) FILTER (WHERE action_taken = '1') AS originations,
    COUNT(*) FILTER (WHERE action_taken = '3') AS denials
FROM raw.hmda_modified_lar
GROUP BY activity_year
ORDER BY activity_year DESC;

\echo '== pipeline observability summary =='

SELECT
    COUNT(*) AS total_runs,
    COUNT(*) FILTER (WHERE status = 'running') AS running_runs,
    COUNT(*) FILTER (WHERE status = 'success') AS successful_runs,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
    COALESCE(SUM(records_extracted), 0) AS records_extracted,
    COALESCE(SUM(records_loaded), 0) AS records_loaded,
    COALESCE(SUM(records_failed), 0) AS records_failed,
    COALESCE(SUM(unmatched_count), 0) AS unmatched_count,
    MAX(started_at) AS latest_started_at
FROM audit.pipeline_runs;

\echo '== latest pipeline runs by source/dataset =='

WITH ranked AS (
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
        duration_seconds,
        started_at,
        finished_at,
        ROW_NUMBER() OVER (
            PARTITION BY source, dataset
            ORDER BY started_at DESC
        ) AS row_number
    FROM audit.pipeline_runs
)
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
    duration_seconds,
    started_at,
    finished_at
FROM ranked
WHERE row_number = 1
ORDER BY source, dataset;

\echo '== failed pipeline runs =='

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
    duration_seconds,
    started_at,
    finished_at
FROM audit.pipeline_runs
WHERE status <> 'success'
ORDER BY started_at DESC
LIMIT 50;

\echo '== runs with unmatched records =='

SELECT
    id,
    pipeline_name,
    source,
    dataset,
    status,
    unmatched_count,
    error_message,
    started_at,
    finished_at
FROM audit.pipeline_runs
WHERE COALESCE(unmatched_count, 0) > 0
ORDER BY unmatched_count DESC, started_at DESC
LIMIT 50;

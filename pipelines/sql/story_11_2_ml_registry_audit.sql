\echo '== Story 11.2 ML table existence =='

SELECT
  table_schema,
  table_name
FROM information_schema.tables
WHERE table_schema = 'analytics'
  AND table_name IN ('ml_model_registry', 'ml_predictions')
ORDER BY table_name;

\echo '== Story 11.2 ML model registry columns =='

SELECT
  ordinal_position,
  column_name,
  data_type,
  is_nullable
FROM information_schema.columns
WHERE table_schema = 'analytics'
  AND table_name = 'ml_model_registry'
ORDER BY ordinal_position;

\echo '== Story 11.2 ML predictions columns =='

SELECT
  ordinal_position,
  column_name,
  data_type,
  is_nullable
FROM information_schema.columns
WHERE table_schema = 'analytics'
  AND table_name = 'ml_predictions'
ORDER BY ordinal_position;

\echo '== Story 11.2 ML table row counts =='

SELECT
  'analytics.ml_model_registry' AS table_name,
  COUNT(*) AS rows
FROM analytics.ml_model_registry
UNION ALL
SELECT
  'analytics.ml_predictions' AS table_name,
  COUNT(*) AS rows
FROM analytics.ml_predictions
ORDER BY table_name;

\echo '== Story 11.2 ML constraints =='

SELECT
  tc.table_name,
  tc.constraint_name,
  tc.constraint_type
FROM information_schema.table_constraints tc
WHERE tc.table_schema = 'analytics'
  AND tc.table_name IN ('ml_model_registry', 'ml_predictions')
ORDER BY tc.table_name, tc.constraint_name;

\echo '== Story 11.2 feature table dependency sanity =='

SELECT
  feature_version,
  COUNT(*) AS feature_rows,
  COUNT(*) FILTER (WHERE is_trainable) AS trainable_rows,
  COUNT(*) FILTER (WHERE target_available) AS target_available_rows
FROM analytics.market_features_monthly
GROUP BY feature_version
ORDER BY feature_version;

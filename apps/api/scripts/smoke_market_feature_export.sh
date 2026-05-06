#!/usr/bin/env bash
set -euo pipefail

echo "== Story 11.3 market feature export smoke =="
echo

mkdir -p .smoke/features

echo "-- Export contract tests"
PYTHONPATH=. pytest pipelines/tests/test_market_feature_export_contract.py -q

echo
echo "-- Ensure feature table is built"
python pipelines/features/build_market_features_monthly.py

echo
echo "-- Export trainable rows"
python scripts/export_market_features.py --feature-version v1

echo
echo "-- Validate trainable export"
python scripts/validate_market_feature_export.py data/exports/features/latest_v1_trainable_only.manifest.json

echo
echo "-- Export all rows"
python scripts/export_market_features.py --feature-version v1 --include-non-trainable

echo
echo "-- Validate all-rows export"
python scripts/validate_market_feature_export.py data/exports/features/latest_v1_all_rows.manifest.json

echo
echo "-- Manifest sanity"
python - <<'PY'
import json
from pathlib import Path

trainable = json.loads(Path("data/exports/features/latest_v1_trainable_only.manifest.json").read_text())
all_rows = json.loads(Path("data/exports/features/latest_v1_all_rows.manifest.json").read_text())

assert trainable["export_mode"] == "trainable_only"
assert all_rows["export_mode"] == "all_rows"

assert trainable["row_count"] > 0
assert all_rows["row_count"] >= trainable["row_count"]

assert trainable["geo_count"] > 0
assert all_rows["geo_count"] >= trainable["geo_count"]

assert trainable["duplicate_key_count"] == 0
assert all_rows["duplicate_key_count"] == 0

assert trainable["leakage_guard"]["feature_inputs_exclude_targets"] is True
assert all_rows["leakage_guard"]["feature_inputs_exclude_targets"] is True

print(
    "Manifest sanity passed: "
    f"trainable_rows={trainable['row_count']}, all_rows={all_rows['row_count']}"
)
PY

echo
echo "Story 11.3 market feature export smoke passed."

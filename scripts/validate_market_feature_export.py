from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.ml.feature_contract import TARGET_COLUMNS


PRIMARY_KEY_COLUMNS = ["geo_id", "period_month", "feature_version"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate exported market feature artifacts.")
    parser.add_argument("manifest_path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest_path = Path(args.manifest_path)

    if not manifest_path.exists():
        raise SystemExit(f"Missing manifest: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    csv_path = Path(manifest["csv_path"])

    if not csv_path.exists():
        raise SystemExit(f"Missing CSV export: {csv_path}")

    expected_rows = int(manifest["row_count"])
    seen_keys: set[tuple[str, str, str]] = set()
    row_count = 0

    with csv_path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)

        assert reader.fieldnames is not None, "CSV has no header"

        missing_pk = [column for column in PRIMARY_KEY_COLUMNS if column not in reader.fieldnames]
        assert not missing_pk, f"CSV missing primary key columns: {missing_pk}"

        missing_targets = [column for column in TARGET_COLUMNS if column not in reader.fieldnames]
        assert not missing_targets, f"CSV missing target columns: {missing_targets}"

        feature_input_columns = set(manifest["feature_input_columns"])
        target_columns = set(manifest["target_columns"])

        overlap = feature_input_columns & target_columns
        assert not overlap, f"Feature input columns overlap target columns: {sorted(overlap)}"

        for row in reader:
            row_count += 1

            for column in PRIMARY_KEY_COLUMNS:
                assert row.get(column), f"Missing primary key value for {column} at row {row_count}"

            key = tuple(row[column] for column in PRIMARY_KEY_COLUMNS)
            assert key not in seen_keys, f"Duplicate export key found: {key}"
            seen_keys.add(key)

            if manifest["export_mode"] == "trainable_only":
                assert row.get("is_trainable") in {"True", "true", "t", "1"}, (
                    f"Non-trainable row found in trainable-only export at row {row_count}: "
                    f"{row.get('geo_id')} {row.get('period_month')}"
                )

    assert row_count == expected_rows, f"CSV row count {row_count} != manifest row_count {expected_rows}"
    assert manifest["duplicate_key_count"] == 0, manifest["duplicate_key_count"]
    assert manifest["leakage_guard"]["feature_inputs_exclude_targets"] is True
    assert manifest["leakage_guard"]["target_columns_prefixed"] is True
    assert manifest["row_count"] > 0
    assert manifest["geo_count"] > 0

    print(f"Market feature export validation passed: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

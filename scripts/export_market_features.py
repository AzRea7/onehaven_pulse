from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine, text

from pipelines.ml.feature_contract import (
    EXPORT_COLUMNS,
    FEATURE_INPUT_COLUMNS,
    TARGET_COLUMNS,
    assert_contract_valid,
)


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market",
)

DEFAULT_EXPORT_ROOT = Path("data/exports/features")






def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export market feature rows for offline modeling.")
    parser.add_argument("--feature-version", default="v1")
    parser.add_argument("--output-root", default=str(DEFAULT_EXPORT_ROOT))
    parser.add_argument(
        "--include-non-trainable",
        action="store_true",
        help="Export all feature rows instead of only is_trainable rows.",
    )
    parser.add_argument(
        "--format",
        choices=["csv"],
        default="csv",
        help="Export format. CSV is supported without extra dependencies.",
    )
    return parser.parse_args()


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _export_query(include_non_trainable: bool) -> str:
    where = "feature_version = :feature_version"
    if not include_non_trainable:
        where += " AND is_trainable = true"

    columns = ",\n        ".join(EXPORT_COLUMNS)

    return f"""
    SELECT
        {columns}
    FROM analytics.market_features_monthly
    WHERE {where}
    ORDER BY geo_id, period_month
    """


def _summary_query(include_non_trainable: bool) -> str:
    where = "feature_version = :feature_version"
    if not include_non_trainable:
        where += " AND is_trainable = true"

    return f"""
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT geo_id) AS geo_count,
        MIN(period_month) AS min_period,
        MAX(period_month) AS max_period,
        COUNT(*) FILTER (WHERE is_trainable) AS trainable_rows,
        COUNT(*) FILTER (WHERE target_available) AS target_available_rows,
        COUNT(*) FILTER (WHERE target_price_growth_12m IS NOT NULL) AS target_price_growth_12m_rows
    FROM analytics.market_features_monthly
    WHERE {where}
    """


def main() -> int:
    assert_contract_valid()

    args = parse_args()

    feature_version = args.feature_version
    include_non_trainable = bool(args.include_non_trainable)
    export_mode = "all_rows" if include_non_trainable else "trainable_only"

    generated_at = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_root)
    export_dir = output_root / feature_version / generated_at
    export_dir.mkdir(parents=True, exist_ok=True)

    csv_path = export_dir / f"market_features_{feature_version}_{export_mode}_{generated_at}.csv"
    manifest_path = export_dir / f"market_features_{feature_version}_{export_mode}_{generated_at}.manifest.json"

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with engine.connect() as connection:
        summary_row = connection.execute(
            text(_summary_query(include_non_trainable)),
            {"feature_version": feature_version},
        ).mappings().one()

        row_count = int(summary_row["row_count"] or 0)
        if row_count <= 0:
            raise SystemExit(
                f"No rows found for feature_version={feature_version}, export_mode={export_mode}"
            )

        result = connection.execute(
            text(_export_query(include_non_trainable)),
            {"feature_version": feature_version},
        ).mappings()

        with csv_path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=EXPORT_COLUMNS)
            writer.writeheader()

            written = 0
            for row in result:
                writer.writerow({column: _json_safe(row[column]) for column in EXPORT_COLUMNS})
                written += 1

        if written != row_count:
            raise SystemExit(f"Export row mismatch: wrote {written}, expected {row_count}")

        duplicate_count = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT geo_id, period_month, feature_version, COUNT(*) AS rows
                    FROM analytics.market_features_monthly
                    WHERE feature_version = :feature_version
                    GROUP BY geo_id, period_month, feature_version
                    HAVING COUNT(*) > 1
                ) duplicates
                """
            ),
            {"feature_version": feature_version},
        ).scalar_one()

    manifest = {
        "story": "11.3",
        "artifact_type": "market_feature_export",
        "generated_at": datetime.now(UTC).isoformat(),
        "feature_version": feature_version,
        "export_mode": export_mode,
        "format": args.format,
        "csv_path": str(csv_path),
        "manifest_path": str(manifest_path),
        "row_count": row_count,
        "geo_count": int(summary_row["geo_count"] or 0),
        "min_period": _json_safe(summary_row["min_period"]),
        "max_period": _json_safe(summary_row["max_period"]),
        "trainable_rows": int(summary_row["trainable_rows"] or 0),
        "target_available_rows": int(summary_row["target_available_rows"] or 0),
        "target_price_growth_12m_rows": int(summary_row["target_price_growth_12m_rows"] or 0),
        "duplicate_key_count": int(duplicate_count or 0),
        "feature_input_columns": FEATURE_INPUT_COLUMNS,
        "target_columns": TARGET_COLUMNS,
        "leakage_guard": {
            "target_columns_prefixed": all(column.startswith("target_") for column in TARGET_COLUMNS),
            "feature_inputs_exclude_targets": set(FEATURE_INPUT_COLUMNS).isdisjoint(TARGET_COLUMNS),
            "exports_targets_separately": True,
        },
    }

    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    latest_manifest = output_root / f"latest_{feature_version}_{export_mode}.manifest.json"
    latest_manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"Exported {row_count} rows to {csv_path}")
    print(f"Wrote manifest to {manifest_path}")
    print(f"Wrote latest manifest to {latest_manifest}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

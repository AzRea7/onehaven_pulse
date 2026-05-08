from __future__ import annotations

import json
import sys
from pathlib import Path


REPORT_PATH = Path("data/diagnostics/michigan/michigan_jurisdiction_status_latest.json")


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: python scripts/check_michigan_jurisdiction_status.py <geo_id_or_name_fragment>"
        )

    query = sys.argv[1].lower()

    if not REPORT_PATH.exists():
        raise SystemExit(
            f"Missing report: {REPORT_PATH}. "
            "Run: python scripts/report_michigan_jurisdiction_status.py"
        )

    payload = json.loads(REPORT_PATH.read_text(encoding="utf-8"))

    if "rows" not in payload:
        raise SystemExit(
            f"Invalid report shape in {REPORT_PATH}. "
            "Delete it and rerun: python scripts/report_michigan_jurisdiction_status.py"
        )

    rows = payload["rows"]

    matches = [
        row for row in rows
        if query in row["geo_id"].lower()
        or query in row["market_name"].lower()
    ]

    if not matches:
        raise SystemExit(f"No Michigan jurisdiction matched: {query}")

    for row in matches:
        print("=" * 88)
        print(f"{row['market_name']} ({row['geo_id']})")
        print(f"Type: {row['geo_type']}")
        print(f"Status: {row['overall_status_score']} | {row['status_label']}")
        print(f"Coverage: {row['coverage_score']}")
        print(f"Freshness: {row['freshness_score']}")
        print(f"Validity: {row['validity_score']}")
        print(f"Geometry: {row['geometry_score']}")
        print(f"Surface: {row['surface_score']}")
        print(f"Latest period: {row['latest_period']}")
        print(f"Reasons: {', '.join(row['reasons']) if row['reasons'] else 'none'}")
        print()
        print("Families:")
        for family in ["price", "rent", "inventory", "affordability", "labor", "permits"]:
            print(
                f"  {family:<14} "
                f"has={row[f'has_{family}']} "
                f"fresh={row[f'{family}_fresh']} "
                f"latest={row[f'{family}_latest_period']}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

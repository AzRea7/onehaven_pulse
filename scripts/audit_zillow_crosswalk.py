from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text


OUTPUT_DIR = Path("data/diagnostics/zillow")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def normalize(value: str | None) -> str:
    if not value:
        return ""

    value = value.lower()
    value = value.replace(" metropolitan statistical area", "")
    value = value.replace(" metro area", "")
    value = value.replace(" msa", "")
    value = value.replace(" county", "")
    value = value.replace(" city", "")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def detect_raw_zillow_tables() -> list[str]:
    sql = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw'
          AND table_name ILIKE '%zillow%'
        ORDER BY table_name
        """
    )
    with engine.connect() as connection:
        return [row[0] for row in connection.execute(sql).all()]


def get_columns(table_name: str) -> set[str]:
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'raw'
          AND table_name = :table_name
        """
    )
    with engine.connect() as connection:
        return {row[0] for row in connection.execute(sql, {"table_name": table_name}).all()}


def first_existing(columns: set[str], candidates: list[str]) -> str | None:
    lowered = {column.lower(): column for column in columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


@dataclass(frozen=True)
class ZillowRegion:
    dataset: str
    source_geo_id: str
    source_geo_name: str
    source_geo_type: str
    state_code: str | None
    row_count: int


def load_raw_zillow_regions() -> list[ZillowRegion]:
    regions: list[ZillowRegion] = []

    for table in detect_raw_zillow_tables():
        columns = get_columns(table)

        id_col = first_existing(
            columns,
            [
                "region_id",
                "RegionID",
                "source_region_id",
                "source_geo_id",
                "zillow_region_id",
            ],
        )
        name_col = first_existing(
            columns,
            [
                "region_name",
                "RegionName",
                "source_region_name",
                "source_geo_name",
                "region",
            ],
        )
        type_col = first_existing(
            columns,
            [
                "region_type",
                "RegionType",
                "source_region_type",
                "source_geo_type",
            ],
        )
        state_col = first_existing(
            columns,
            [
                "state_code",
                "State",
                "state",
                "state_name",
            ],
        )

        if not id_col or not name_col:
            print(f"Skipping raw.{table}: no obvious region id/name columns.")
            continue

        dataset = table.replace("zillow_", "").replace("raw_", "")
        if "zhvi" in table.lower():
            dataset = "zhvi"
        elif "zori" in table.lower():
            dataset = "zori"

        select_type = f'"{type_col}"' if type_col else "'unknown'"
        select_state = f'"{state_col}"' if state_col else "NULL"

        sql = text(
            f"""
            SELECT
                {select_type}::text AS source_geo_type,
                "{id_col}"::text AS source_geo_id,
                "{name_col}"::text AS source_geo_name,
                {select_state}::text AS state_code,
                COUNT(*)::int AS row_count
            FROM raw.{table}
            WHERE "{id_col}" IS NOT NULL
              AND "{name_col}" IS NOT NULL
            GROUP BY 1, 2, 3, 4
            ORDER BY row_count DESC
            """
        )

        with engine.connect() as connection:
            for row in connection.execute(sql).mappings().all():
                regions.append(
                    ZillowRegion(
                        dataset=dataset,
                        source_geo_id=str(row["source_geo_id"]),
                        source_geo_name=str(row["source_geo_name"]),
                        source_geo_type=str(row["source_geo_type"] or "unknown"),
                        state_code=row["state_code"],
                        row_count=int(row["row_count"]),
                    )
                )

    return regions


def load_existing_zillow_crosswalk_keys() -> set[tuple[str, str]]:
    sql = text(
        """
        SELECT source_geo_id, canonical_geo_id
        FROM geo.geo_crosswalk
        WHERE source = 'zillow'
          AND is_active = true
        """
    )
    with engine.connect() as connection:
        return {
            (str(row["source_geo_id"]), str(row["canonical_geo_id"]))
            for row in connection.execute(sql).mappings().all()
        }


def load_dim_geo_index() -> dict[str, dict[str, Any]]:
    sql = text(
        """
        SELECT
            geo_id,
            geo_type,
            name,
            display_name,
            state_code
        FROM geo.dim_geo
        WHERE is_active = true
          AND geo_id NOT LIKE 'metro_redfin_%'
          AND geo_id NOT LIKE 'metro:overture_%'
        """
    )

    index: dict[str, dict[str, Any]] = {}

    with engine.connect() as connection:
        rows = connection.execute(sql).mappings().all()

    for row in rows:
        aliases = {
            normalize(row["name"]),
            normalize(row["display_name"]),
        }

        # Common Zillow metro abbreviation pattern: "Detroit, MI".
        name = str(row["name"] or row["display_name"] or "")
        display_name = str(row["display_name"] or row["name"] or "")
        state = str(row["state_code"] or "").strip()

        if state:
            aliases.add(normalize(f"{name}, {state}"))
            aliases.add(normalize(f"{display_name}, {state}"))

        for alias in aliases:
            if alias and alias not in index:
                index[alias] = dict(row)

    return index


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    raw_regions = load_raw_zillow_regions()
    existing_keys = load_existing_zillow_crosswalk_keys()
    dim_geo_index = load_dim_geo_index()

    unmatched: list[dict[str, Any]] = []
    proposals: list[dict[str, Any]] = []

    for region in raw_regions:
        already_mapped = any(
            source_geo_id == region.source_geo_id
            for source_geo_id, _canonical_geo_id in existing_keys
        )

        if already_mapped:
            continue

        normalized_name = normalize(region.source_geo_name)
        match = dim_geo_index.get(normalized_name)

        base_row = {
            "dataset": region.dataset,
            "source_geo_id": region.source_geo_id,
            "source_geo_name": region.source_geo_name,
            "source_geo_type": region.source_geo_type,
            "state_code": region.state_code or "",
            "row_count": region.row_count,
            "normalized_source_name": normalized_name,
        }

        if match:
            proposals.append(
                {
                    "source": "zillow",
                    "source_geo_id": region.source_geo_id,
                    "source_geo_name": region.source_geo_name,
                    "source_geo_type": region.source_geo_type,
                    "canonical_geo_id": match["geo_id"],
                    "match_method": "manual",
                    "confidence_score": "0.9500",
                    "notes": (
                        "Story 9.2 generated exact normalized name match; "
                        f"dataset={region.dataset}; state_code={region.state_code or ''}"
                    ),
                    "dataset": region.dataset,
                    "row_count": region.row_count,
                    "matched_geo_name": match["display_name"] or match["name"],
                    "matched_geo_type": match["geo_type"],
                }
            )
        else:
            unmatched.append(base_row)

    proposal_fields = [
        "source",
        "source_geo_id",
        "source_geo_name",
        "source_geo_type",
        "canonical_geo_id",
        "match_method",
        "confidence_score",
        "notes",
        "dataset",
        "row_count",
        "matched_geo_name",
        "matched_geo_type",
    ]

    unmatched_fields = [
        "dataset",
        "source_geo_id",
        "source_geo_name",
        "source_geo_type",
        "state_code",
        "row_count",
        "normalized_source_name",
    ]

    proposals = sorted(proposals, key=lambda row: int(row["row_count"]), reverse=True)
    unmatched = sorted(unmatched, key=lambda row: int(row["row_count"]), reverse=True)

    write_csv(OUTPUT_DIR / "zillow_crosswalk_proposals.csv", proposals, proposal_fields)
    write_csv(OUTPUT_DIR / "zillow_unmatched_regions.csv", unmatched, unmatched_fields)

    print(f"Raw Zillow regions needing review: {len(raw_regions)}")
    print(f"Generated exact-match proposals: {len(proposals)}")
    print(f"Still unmatched after exact matching: {len(unmatched)}")
    print(f"Proposal file: {OUTPUT_DIR / 'zillow_crosswalk_proposals.csv'}")
    print(f"Unmatched file: {OUTPUT_DIR / 'zillow_unmatched_regions.csv'}")

    print("\nTop 20 proposals:")
    for row in proposals[:20]:
        print(
            f"{row['source_geo_id']} | {row['source_geo_name']} -> "
            f"{row['canonical_geo_id']} | {row['matched_geo_name']} | rows={row['row_count']}"
        )

    print("\nTop 20 unmatched:")
    for row in unmatched[:20]:
        print(
            f"{row['source_geo_id']} | {row['source_geo_name']} | "
            f"type={row['source_geo_type']} | rows={row['row_count']}"
        )


if __name__ == "__main__":
    main()

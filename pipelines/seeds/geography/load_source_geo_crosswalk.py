from __future__ import annotations

import argparse
import csv
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


DEFAULT_SEED_PATH = Path("data/seeds/geography/source_geo_crosswalk_seed.csv")


@dataclass(frozen=True)
class SourceGeoCrosswalkSeed:
    source: str
    source_geo_id: str
    source_geo_name: str | None
    source_geo_type: str | None
    canonical_geo_id: str
    match_method: str
    confidence_score: Decimal
    notes: str | None


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None

    stripped = value.strip()
    return stripped or None


def _required(row: dict[str, str], key: str) -> str:
    value = _clean_optional(row.get(key))

    if value is None:
        raise ValueError(f"Missing required field {key!r}: {row}")

    return value


def read_seed_file(path: Path) -> list[SourceGeoCrosswalkSeed]:
    if not path.exists():
        raise FileNotFoundError(f"Seed file does not exist: {path}")

    seeds: list[SourceGeoCrosswalkSeed] = []

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        required_fields = {
            "source",
            "source_geo_id",
            "canonical_geo_id",
            "match_method",
            "confidence_score",
        }

        missing = required_fields - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Seed file is missing required columns: {sorted(missing)}")

        for row in reader:
            confidence_score = Decimal(_required(row, "confidence_score"))

            if confidence_score < 0 or confidence_score > 1:
                raise ValueError(f"confidence_score must be between 0 and 1: {row}")

            seeds.append(
                SourceGeoCrosswalkSeed(
                    source=_required(row, "source").lower(),
                    source_geo_id=_required(row, "source_geo_id"),
                    source_geo_name=_clean_optional(row.get("source_geo_name")),
                    source_geo_type=_clean_optional(row.get("source_geo_type")),
                    canonical_geo_id=_required(row, "canonical_geo_id"),
                    match_method=_required(row, "match_method").lower(),
                    confidence_score=confidence_score,
                    notes=_clean_optional(row.get("notes")),
                )
            )

    return seeds


def _validate_canonical_geo_ids(engine: Engine, seeds: list[SourceGeoCrosswalkSeed]) -> None:
    canonical_ids = sorted({seed.canonical_geo_id for seed in seeds})

    if not canonical_ids:
        return

    sql = text(
        """
        SELECT geo_id
        FROM geo.dim_geo
        WHERE geo_id = ANY(:geo_ids)
          AND is_active = true
        """
    )

    with engine.begin() as connection:
        found = {
            row[0]
            for row in connection.execute(sql, {"geo_ids": canonical_ids}).all()
        }

    missing = sorted(set(canonical_ids) - found)

    if missing:
        raise ValueError(f"canonical_geo_id values not found in geo.dim_geo: {missing}")


def load_source_geo_crosswalk(
    *,
    seed_path: Path = DEFAULT_SEED_PATH,
    database_url: str | None = None,
) -> int:
    seeds = read_seed_file(seed_path)

    resolved_database_url = database_url or os.getenv("DATABASE_URL")

    if not resolved_database_url:
        raise ValueError("DATABASE_URL is required.")

    engine = create_engine(resolved_database_url)

    _validate_canonical_geo_ids(engine, seeds)

    sql = text(
        """
        INSERT INTO geo.geo_crosswalk (
            source,
            source_geo_id,
            source_geo_name,
            source_geo_type,
            canonical_geo_id,
            match_method,
            confidence_score,
            notes
        )
        VALUES (
            :source,
            :source_geo_id,
            :source_geo_name,
            :source_geo_type,
            :canonical_geo_id,
            :match_method,
            :confidence_score,
            :notes
        )
        ON CONFLICT (source, source_geo_id, canonical_geo_id)
        DO UPDATE SET
            source_geo_name = EXCLUDED.source_geo_name,
            source_geo_type = EXCLUDED.source_geo_type,
            match_method = EXCLUDED.match_method,
            confidence_score = EXCLUDED.confidence_score,
            notes = EXCLUDED.notes
        """
    )

    payload = [
        {
            "source": seed.source,
            "source_geo_id": seed.source_geo_id,
            "source_geo_name": seed.source_geo_name,
            "source_geo_type": seed.source_geo_type,
            "canonical_geo_id": seed.canonical_geo_id,
            "match_method": seed.match_method,
            "confidence_score": seed.confidence_score,
            "notes": seed.notes,
        }
        for seed in seeds
    ]

    with engine.begin() as connection:
        if payload:
            connection.execute(sql, payload)

    return len(payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load source geography crosswalk seed file.")
    parser.add_argument(
        "--seed-path",
        default=str(DEFAULT_SEED_PATH),
        help="Path to source geography crosswalk CSV seed file.",
    )
    args = parser.parse_args()

    loaded = load_source_geo_crosswalk(seed_path=Path(args.seed_path))
    print(f"Loaded source geography crosswalk seed rows: {loaded}")


if __name__ == "__main__":
    main()

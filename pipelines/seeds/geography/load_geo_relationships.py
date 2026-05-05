from __future__ import annotations

import argparse
import csv
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine, text


DEFAULT_RELATIONSHIP_SEED_PATH = Path("data/seeds/geography/geo_relationships_seed.csv")


VALID_RELATIONSHIP_TYPES = {
    "contains",
    "overlaps",
    "rolls_up_to",
    "adjacent_to",
}


@dataclass(frozen=True)
class GeoRelationshipSeed:
    parent_geo_id: str
    child_geo_id: str
    relationship_type: str
    source: str
    confidence_score: Decimal
    overlap_ratio: Decimal | None
    is_active: bool
    notes: str | None


def _clean(value: str | None) -> str | None:
    if value is None:
        return None

    value = value.strip()
    return value or None


def _required(row: dict[str, str], key: str) -> str:
    value = _clean(row.get(key))

    if value is None:
        raise ValueError(f"Missing required field {key!r}: {row}")

    return value


def _decimal(value: str | None) -> Decimal | None:
    cleaned = _clean(value)

    if cleaned is None:
        return None

    return Decimal(cleaned)


def _bool(value: str | None) -> bool:
    cleaned = _clean(value)

    if cleaned is None:
        return True

    return cleaned.lower() in {"true", "t", "1", "yes", "y"}


def read_relationship_seed_file(path: Path) -> list[GeoRelationshipSeed]:
    if not path.exists():
        return []

    relationships: list[GeoRelationshipSeed] = []

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            relationship_type = _required(row, "relationship_type")

            if relationship_type not in VALID_RELATIONSHIP_TYPES:
                raise ValueError(f"Invalid relationship_type {relationship_type!r}: {row}")

            parent_geo_id = _required(row, "parent_geo_id")
            child_geo_id = _required(row, "child_geo_id")

            if parent_geo_id == child_geo_id:
                raise ValueError(f"Relationship cannot point to itself: {row}")

            confidence_score = _decimal(row.get("confidence_score")) or Decimal("1.0000")

            if confidence_score < 0 or confidence_score > 1:
                raise ValueError(f"confidence_score must be between 0 and 1: {row}")

            overlap_ratio = _decimal(row.get("overlap_ratio"))

            if overlap_ratio is not None and (overlap_ratio < 0 or overlap_ratio > 1):
                raise ValueError(f"overlap_ratio must be between 0 and 1: {row}")

            relationships.append(
                GeoRelationshipSeed(
                    parent_geo_id=parent_geo_id,
                    child_geo_id=child_geo_id,
                    relationship_type=relationship_type,
                    source=_required(row, "source"),
                    confidence_score=confidence_score,
                    overlap_ratio=overlap_ratio,
                    is_active=_bool(row.get("is_active")),
                    notes=_clean(row.get("notes")),
                )
            )

    return relationships


def validate_geo_ids(engine, relationships: list[GeoRelationshipSeed]) -> None:
    geo_ids = sorted(
        {
            geo_id
            for relationship in relationships
            for geo_id in (relationship.parent_geo_id, relationship.child_geo_id)
        }
    )

    if not geo_ids:
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
            for row in connection.execute(sql, {"geo_ids": geo_ids}).all()
        }

    missing = sorted(set(geo_ids) - found)

    if missing:
        raise ValueError(f"geo_relationship seed references missing geo_id values: {missing}")


def load_geo_relationships(
    *,
    relationship_seed_path: Path = DEFAULT_RELATIONSHIP_SEED_PATH,
    database_url: str | None = None,
) -> int:
    relationships = read_relationship_seed_file(relationship_seed_path)

    if not relationships:
        return 0

    resolved_database_url = database_url or os.getenv("DATABASE_URL")

    if not resolved_database_url:
        raise ValueError("DATABASE_URL is required.")

    engine = create_engine(resolved_database_url)

    validate_geo_ids(engine, relationships)

    sql = text(
        """
        INSERT INTO geo.geo_relationships (
            parent_geo_id,
            child_geo_id,
            relationship_type,
            source,
            confidence_score,
            overlap_ratio,
            is_active,
            notes
        )
        VALUES (
            :parent_geo_id,
            :child_geo_id,
            :relationship_type,
            :source,
            :confidence_score,
            :overlap_ratio,
            :is_active,
            :notes
        )
        ON CONFLICT (
            parent_geo_id,
            child_geo_id,
            relationship_type,
            source
        )
        DO UPDATE SET
            confidence_score = EXCLUDED.confidence_score,
            overlap_ratio = EXCLUDED.overlap_ratio,
            is_active = EXCLUDED.is_active,
            notes = EXCLUDED.notes,
            updated_at = now()
        """
    )

    payload = [
        {
            "parent_geo_id": relationship.parent_geo_id,
            "child_geo_id": relationship.child_geo_id,
            "relationship_type": relationship.relationship_type,
            "source": relationship.source,
            "confidence_score": relationship.confidence_score,
            "overlap_ratio": relationship.overlap_ratio,
            "is_active": relationship.is_active,
            "notes": relationship.notes,
        }
        for relationship in relationships
    ]

    with engine.begin() as connection:
        connection.execute(sql, payload)

    return len(payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load canonical geography relationship seed file.")
    parser.add_argument("--relationship-seed-path", default=str(DEFAULT_RELATIONSHIP_SEED_PATH))

    args = parser.parse_args()

    loaded = load_geo_relationships(
        relationship_seed_path=Path(args.relationship_seed_path),
    )

    print(f"Loaded geography relationship seed rows: {loaded}")


if __name__ == "__main__":
    main()

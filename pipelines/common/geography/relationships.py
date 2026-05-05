from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import text


@dataclass(frozen=True)
class GeoRelationship:
    parent_geo_id: str
    child_geo_id: str
    relationship_type: str
    source: str
    confidence_score: Decimal
    overlap_ratio: Decimal | None
    parent_geo_type: str | None = None
    child_geo_type: str | None = None
    parent_display_name: str | None = None
    child_display_name: str | None = None


def get_children(
    connection,
    *,
    parent_geo_id: str,
    relationship_type: str = "contains",
    child_geo_type: str | None = None,
) -> list[GeoRelationship]:
    sql = text(
        """
        SELECT
            r.parent_geo_id,
            r.child_geo_id,
            r.relationship_type,
            r.source,
            r.confidence_score,
            r.overlap_ratio,
            parent.geo_type AS parent_geo_type,
            child.geo_type AS child_geo_type,
            parent.display_name AS parent_display_name,
            child.display_name AS child_display_name
        FROM geo.geo_relationships r
        JOIN geo.dim_geo parent
          ON parent.geo_id = r.parent_geo_id
        JOIN geo.dim_geo child
          ON child.geo_id = r.child_geo_id
        WHERE r.parent_geo_id = :parent_geo_id
          AND r.relationship_type = :relationship_type
          AND r.is_active = true
          AND (:child_geo_type IS NULL OR child.geo_type = :child_geo_type)
        ORDER BY child.geo_type, child.display_name, child.geo_id
        """
    )

    rows = connection.execute(
        sql,
        {
            "parent_geo_id": parent_geo_id,
            "relationship_type": relationship_type,
            "child_geo_type": child_geo_type,
        },
    ).mappings().all()

    return [
        GeoRelationship(
            parent_geo_id=row["parent_geo_id"],
            child_geo_id=row["child_geo_id"],
            relationship_type=row["relationship_type"],
            source=row["source"],
            confidence_score=row["confidence_score"],
            overlap_ratio=row["overlap_ratio"],
            parent_geo_type=row["parent_geo_type"],
            child_geo_type=row["child_geo_type"],
            parent_display_name=row["parent_display_name"],
            child_display_name=row["child_display_name"],
        )
        for row in rows
    ]


def get_parents(
    connection,
    *,
    child_geo_id: str,
    relationship_type: str = "contains",
    parent_geo_type: str | None = None,
) -> list[GeoRelationship]:
    sql = text(
        """
        SELECT
            r.parent_geo_id,
            r.child_geo_id,
            r.relationship_type,
            r.source,
            r.confidence_score,
            r.overlap_ratio,
            parent.geo_type AS parent_geo_type,
            child.geo_type AS child_geo_type,
            parent.display_name AS parent_display_name,
            child.display_name AS child_display_name
        FROM geo.geo_relationships r
        JOIN geo.dim_geo parent
          ON parent.geo_id = r.parent_geo_id
        JOIN geo.dim_geo child
          ON child.geo_id = r.child_geo_id
        WHERE r.child_geo_id = :child_geo_id
          AND r.relationship_type = :relationship_type
          AND r.is_active = true
          AND (:parent_geo_type IS NULL OR parent.geo_type = :parent_geo_type)
        ORDER BY parent.geo_type, parent.display_name, parent.geo_id
        """
    )

    rows = connection.execute(
        sql,
        {
            "child_geo_id": child_geo_id,
            "relationship_type": relationship_type,
            "parent_geo_type": parent_geo_type,
        },
    ).mappings().all()

    return [
        GeoRelationship(
            parent_geo_id=row["parent_geo_id"],
            child_geo_id=row["child_geo_id"],
            relationship_type=row["relationship_type"],
            source=row["source"],
            confidence_score=row["confidence_score"],
            overlap_ratio=row["overlap_ratio"],
            parent_geo_type=row["parent_geo_type"],
            child_geo_type=row["child_geo_type"],
            parent_display_name=row["parent_display_name"],
            child_display_name=row["child_display_name"],
        )
        for row in rows
    ]

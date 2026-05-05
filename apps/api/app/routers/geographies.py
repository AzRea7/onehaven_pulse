from __future__ import annotations

from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.geographies import (
    GeographyChildrenResponse,
    GeographyIdentity,
    GeographyParentsResponse,
    GeographyRelatedResponse,
    GeographyRelationshipItem,
    GeographySearchItem,
    GeographySearchResponse,
)


router = APIRouter(prefix="/geographies", tags=["geographies"])


VALID_RELATIONSHIP_TYPES = {
    "contains",
    "overlaps",
    "rolls_up_to",
    "adjacent_to",
}


def _validate_relationship_type(value: str) -> str:
    if value not in VALID_RELATIONSHIP_TYPES:
        raise ValueError(
            f"Unsupported relationship_type {value!r}. "
            f"Allowed values: {sorted(VALID_RELATIONSHIP_TYPES)}"
        )

    return value


def _get_geography(db: Session, geo_id: str) -> dict[str, Any] | None:
    row = db.execute(
        text(
            """
            SELECT
                geo_id,
                geo_type,
                name,
                display_name,
                state_code,
                state_name,
                county_fips,
                cbsa_code,
                place_fips,
                zcta,
                country_code,
                latitude,
                longitude,
                hierarchy_level,
                canonical_slug
            FROM geo.dim_geo
            WHERE geo_id = :geo_id
              AND is_active = true
            """
        ),
        {"geo_id": geo_id},
    ).mappings().first()

    return dict(row) if row else None


def _require_geography(db: Session, geo_id: str) -> dict[str, Any]:
    geography = _get_geography(db, geo_id)

    if geography is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "geography_not_found",
                "message": f"Geography '{geo_id}' was not found.",
                "details": {"geo_id": geo_id},
            },
        )

    return geography


def _identity_from_prefixed_row(row: dict[str, Any], prefix: str) -> GeographyIdentity:
    return GeographyIdentity(
        geo_id=row[f"{prefix}_geo_id"],
        geo_type=row[f"{prefix}_geo_type"],
        name=row[f"{prefix}_name"],
        display_name=row[f"{prefix}_display_name"],
        state_code=row[f"{prefix}_state_code"],
        state_name=row[f"{prefix}_state_name"],
        county_fips=row[f"{prefix}_county_fips"],
        cbsa_code=row[f"{prefix}_cbsa_code"],
        place_fips=row[f"{prefix}_place_fips"],
        zcta=row[f"{prefix}_zcta"],
        country_code=row[f"{prefix}_country_code"],
        latitude=float(row[f"{prefix}_latitude"]) if row[f"{prefix}_latitude"] is not None else None,
        longitude=float(row[f"{prefix}_longitude"]) if row[f"{prefix}_longitude"] is not None else None,
        hierarchy_level=row[f"{prefix}_hierarchy_level"],
        canonical_slug=row[f"{prefix}_canonical_slug"],
    )


def _relationship_from_row(row: dict[str, Any]) -> GeographyRelationshipItem:
    return GeographyRelationshipItem(
        parent=_identity_from_prefixed_row(row, "parent"),
        child=_identity_from_prefixed_row(row, "child"),
        relationship_type=row["relationship_type"],
        source=row["source"],
        confidence_score=Decimal(row["confidence_score"]),
        overlap_ratio=Decimal(row["overlap_ratio"]) if row["overlap_ratio"] is not None else None,
        is_active=row["is_active"],
    )


def _relationship_select_sql(where_clause: str) -> str:
    return f"""
        SELECT
            r.relationship_type,
            r.source,
            r.confidence_score,
            r.overlap_ratio,
            r.is_active,

            parent.geo_id AS parent_geo_id,
            parent.geo_type AS parent_geo_type,
            parent.name AS parent_name,
            parent.display_name AS parent_display_name,
            parent.state_code AS parent_state_code,
            parent.state_name AS parent_state_name,
            parent.county_fips AS parent_county_fips,
            parent.cbsa_code AS parent_cbsa_code,
            parent.place_fips AS parent_place_fips,
            parent.zcta AS parent_zcta,
            parent.country_code AS parent_country_code,
            parent.latitude AS parent_latitude,
            parent.longitude AS parent_longitude,
            parent.hierarchy_level AS parent_hierarchy_level,
            parent.canonical_slug AS parent_canonical_slug,

            child.geo_id AS child_geo_id,
            child.geo_type AS child_geo_type,
            child.name AS child_name,
            child.display_name AS child_display_name,
            child.state_code AS child_state_code,
            child.state_name AS child_state_name,
            child.county_fips AS child_county_fips,
            child.cbsa_code AS child_cbsa_code,
            child.place_fips AS child_place_fips,
            child.zcta AS child_zcta,
            child.country_code AS child_country_code,
            child.latitude AS child_latitude,
            child.longitude AS child_longitude,
            child.hierarchy_level AS child_hierarchy_level,
            child.canonical_slug AS child_canonical_slug
        FROM geo.geo_relationships r
        JOIN geo.dim_geo parent
          ON parent.geo_id = r.parent_geo_id
        JOIN geo.dim_geo child
          ON child.geo_id = r.child_geo_id
        WHERE r.is_active = true
          AND parent.is_active = true
          AND child.is_active = true
          {where_clause}
        ORDER BY
            parent.hierarchy_level,
            parent.display_name,
            child.hierarchy_level,
            child.display_name,
            r.relationship_type,
            r.source
    """


def _get_children(
    db: Session,
    *,
    geo_id: str,
    relationship_type: str,
    child_geo_type: str | None,
) -> list[GeographyRelationshipItem]:
    sql = text(
        _relationship_select_sql(
            """
            AND r.parent_geo_id = :geo_id
            AND r.relationship_type = :relationship_type
            AND (:child_geo_type IS NULL OR child.geo_type = :child_geo_type)
            """
        )
    )

    rows = db.execute(
        sql,
        {
            "geo_id": geo_id,
            "relationship_type": relationship_type,
            "child_geo_type": child_geo_type,
        },
    ).mappings().all()

    return [_relationship_from_row(dict(row)) for row in rows]


def _get_parents(
    db: Session,
    *,
    geo_id: str,
    relationship_type: str,
    parent_geo_type: str | None,
) -> list[GeographyRelationshipItem]:
    sql = text(
        _relationship_select_sql(
            """
            AND r.child_geo_id = :geo_id
            AND r.relationship_type = :relationship_type
            AND (:parent_geo_type IS NULL OR parent.geo_type = :parent_geo_type)
            """
        )
    )

    rows = db.execute(
        sql,
        {
            "geo_id": geo_id,
            "relationship_type": relationship_type,
            "parent_geo_type": parent_geo_type,
        },
    ).mappings().all()

    return [_relationship_from_row(dict(row)) for row in rows]




def _identity_from_row(row: dict[str, Any]) -> GeographyIdentity:
    return GeographyIdentity(
        geo_id=row["geo_id"],
        geo_type=row["geo_type"],
        name=row["name"],
        display_name=row["display_name"],
        state_code=row["state_code"],
        state_name=row["state_name"],
        county_fips=row["county_fips"],
        cbsa_code=row["cbsa_code"],
        place_fips=row["place_fips"],
        zcta=row["zcta"],
        country_code=row["country_code"],
        latitude=float(row["latitude"]) if row["latitude"] is not None else None,
        longitude=float(row["longitude"]) if row["longitude"] is not None else None,
        hierarchy_level=row["hierarchy_level"],
        canonical_slug=row["canonical_slug"],
    )




@router.get("/search", response_model=GeographySearchResponse)
def search_geographies(
    q: str | None = Query(default=None),
    geo_type: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> GeographySearchResponse:
    normalized_q = q.strip() if q else None
    normalized_geo_type = geo_type.strip() if geo_type else None

    sql = text(
        """
        WITH relationship_counts AS (
            SELECT
                geo_id,
                SUM(parent_count) AS parent_count,
                SUM(child_count) AS child_count
            FROM (
                SELECT
                    child_geo_id AS geo_id,
                    COUNT(*) AS parent_count,
                    0 AS child_count
                FROM geo.geo_relationships
                WHERE is_active = true
                GROUP BY child_geo_id

                UNION ALL

                SELECT
                    parent_geo_id AS geo_id,
                    0 AS parent_count,
                    COUNT(*) AS child_count
                FROM geo.geo_relationships
                WHERE is_active = true
                GROUP BY parent_geo_id
            ) counts
            GROUP BY geo_id
        )
        SELECT
            g.geo_id,
            g.geo_type,
            g.name,
            g.display_name,
            g.state_code,
            g.state_name,
            g.county_fips,
            g.cbsa_code,
            g.place_fips,
            g.zcta,
            g.country_code,
            g.latitude,
            g.longitude,
            g.hierarchy_level,
            g.canonical_slug,
            COALESCE(rc.parent_count, 0) AS parent_count,
            COALESCE(rc.child_count, 0) AS child_count
        FROM geo.dim_geo g
        LEFT JOIN relationship_counts rc
          ON rc.geo_id = g.geo_id
        WHERE g.is_active = true
          AND (:geo_type IS NULL OR g.geo_type = :geo_type)
          AND (
              :q IS NULL
              OR g.geo_id ILIKE :q_like
              OR g.name ILIKE :q_like
              OR g.display_name ILIKE :q_like
              OR g.canonical_slug ILIKE :q_like
              OR g.state_code ILIKE :q_exact
              OR g.cbsa_code ILIKE :q_exact
              OR g.county_fips ILIKE :q_exact
              OR g.place_fips ILIKE :q_exact
              OR g.zcta ILIKE :q_exact
          )
        ORDER BY
            CASE
                WHEN :q IS NOT NULL AND g.geo_id = :q THEN 0
                WHEN :q IS NOT NULL AND g.zcta = :q THEN 1
                WHEN :q IS NOT NULL AND g.cbsa_code = :q THEN 2
                WHEN :q IS NOT NULL AND g.place_fips = :q THEN 3
                WHEN :q IS NOT NULL AND g.display_name ILIKE :q_prefix THEN 4
                WHEN :q IS NOT NULL AND g.name ILIKE :q_prefix THEN 5
                ELSE 6
            END,
            g.hierarchy_level,
            g.display_name,
            g.geo_id
        LIMIT :limit
        """
    )

    rows = db.execute(
        sql,
        {
            "q": normalized_q,
            "q_like": f"%{normalized_q}%" if normalized_q else None,
            "q_prefix": f"{normalized_q}%" if normalized_q else None,
            "q_exact": normalized_q,
            "geo_type": normalized_geo_type,
            "limit": limit,
        },
    ).mappings().all()

    return GeographySearchResponse(
        q=normalized_q,
        geo_type=normalized_geo_type,
        limit=limit,
        items=[
            GeographySearchItem(
                geography=_identity_from_row(dict(row)),
                parent_count=int(row["parent_count"]),
                child_count=int(row["child_count"]),
            )
            for row in rows
        ],
    )


@router.get("/{geo_id}/children", response_model=GeographyChildrenResponse)
def get_geography_children(
    geo_id: str,
    relationship_type: str = Query(default="contains"),
    child_geo_type: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> GeographyChildrenResponse:
    relationship_type = _validate_relationship_type(relationship_type)
    _require_geography(db, geo_id)

    items = _get_children(
        db,
        geo_id=geo_id,
        relationship_type=relationship_type,
        child_geo_type=child_geo_type,
    )

    return GeographyChildrenResponse(
        geo_id=geo_id,
        relationship_type=relationship_type,
        child_geo_type=child_geo_type,
        items=items,
    )


@router.get("/{geo_id}/parents", response_model=GeographyParentsResponse)
def get_geography_parents(
    geo_id: str,
    relationship_type: str = Query(default="contains"),
    parent_geo_type: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> GeographyParentsResponse:
    relationship_type = _validate_relationship_type(relationship_type)
    _require_geography(db, geo_id)

    items = _get_parents(
        db,
        geo_id=geo_id,
        relationship_type=relationship_type,
        parent_geo_type=parent_geo_type,
    )

    return GeographyParentsResponse(
        geo_id=geo_id,
        relationship_type=relationship_type,
        parent_geo_type=parent_geo_type,
        items=items,
    )


@router.get("/{geo_id}/related", response_model=GeographyRelatedResponse)
def get_geography_related(
    geo_id: str,
    relationship_type: str = Query(default="contains"),
    parent_geo_type: str | None = Query(default=None),
    child_geo_type: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> GeographyRelatedResponse:
    relationship_type = _validate_relationship_type(relationship_type)
    _require_geography(db, geo_id)

    parents = _get_parents(
        db,
        geo_id=geo_id,
        relationship_type=relationship_type,
        parent_geo_type=parent_geo_type,
    )
    children = _get_children(
        db,
        geo_id=geo_id,
        relationship_type=relationship_type,
        child_geo_type=child_geo_type,
    )

    return GeographyRelatedResponse(
        geo_id=geo_id,
        relationship_type=relationship_type,
        parent_geo_type=parent_geo_type,
        child_geo_type=child_geo_type,
        parents=parents,
        children=children,
    )

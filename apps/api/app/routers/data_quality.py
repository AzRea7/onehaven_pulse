from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.data_quality import (
    MarketDataQualityListResponse,
    MarketDataQualityResponse,
)

router = APIRouter(prefix="/data-quality", tags=["data-quality"])


def _row_to_quality(row) -> MarketDataQualityResponse:
    return MarketDataQualityResponse(
        geo_id=row["geo_id"],
        quality_version=row["quality_version"],
        as_of_date=row["as_of_date"].isoformat(),
        latest_period=row["latest_period"].isoformat() if row["latest_period"] else None,
        coverage_score=float(row["coverage_score"]),
        freshness_score=float(row["freshness_score"]),
        validity_score=float(row["validity_score"]),
        overall_quality_score=float(row["overall_quality_score"]),
        has_price=bool(row["has_price"]),
        has_rent=bool(row["has_rent"]),
        has_inventory=bool(row["has_inventory"]),
        has_affordability=bool(row["has_affordability"]),
        has_labor=bool(row["has_labor"]),
        has_permits=bool(row["has_permits"]),
        is_fresh=bool(row["is_fresh"]),
        has_bad_values=bool(row["has_bad_values"]),
        missing_categories=list(row["missing_categories"] or []),
        stale_categories=list(row["stale_categories"] or []),
        quality_issues=list(row["quality_issues"] or []),
    )


@router.get("/markets", response_model=MarketDataQualityListResponse)
def list_market_data_quality(
    db: Annotated[Session, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    min_quality: Annotated[float | None, Query(ge=0, le=1)] = None,
    max_quality: Annotated[float | None, Query(ge=0, le=1)] = None,
    only_fresh: bool | None = None,
) -> MarketDataQualityListResponse:
    filters = ["q.quality_version = 'v1'", "q.as_of_date = (SELECT MAX(as_of_date) FROM analytics.market_data_quality WHERE quality_version = 'v1')"]
    params = {
        "limit": limit,
        "offset": offset,
    }

    if min_quality is not None:
        filters.append("q.overall_quality_score >= :min_quality")
        params["min_quality"] = min_quality

    if max_quality is not None:
        filters.append("q.overall_quality_score <= :max_quality")
        params["max_quality"] = max_quality

    if only_fresh is not None:
        filters.append("q.is_fresh = :only_fresh")
        params["only_fresh"] = only_fresh

    where_sql = " AND ".join(filters)

    rows = db.execute(
        text(
            f"""
            SELECT
                q.*,
                COUNT(*) OVER() AS total
            FROM analytics.market_data_quality q
            WHERE {where_sql}
            ORDER BY
                q.overall_quality_score ASC,
                q.coverage_score ASC,
                q.freshness_score ASC,
                q.geo_id ASC
            LIMIT :limit
            OFFSET :offset
            """
        ),
        params,
    ).mappings().all()

    total = int(rows[0]["total"]) if rows else 0

    return MarketDataQualityListResponse(
        items=[_row_to_quality(row) for row in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/markets/{geo_id}", response_model=MarketDataQualityResponse)
def get_market_data_quality(
    geo_id: str,
    db: Annotated[Session, Depends(get_db)],
) -> MarketDataQualityResponse:
    row = db.execute(
        text(
            """
            SELECT *
            FROM analytics.market_data_quality
            WHERE geo_id = :geo_id
              AND quality_version = 'v1'
              AND as_of_date = (
                  SELECT MAX(as_of_date)
                  FROM analytics.market_data_quality
                  WHERE quality_version = 'v1'
              )
            """
        ),
        {"geo_id": geo_id},
    ).mappings().first()

    if row is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Market data quality not found.")

    return _row_to_quality(row)

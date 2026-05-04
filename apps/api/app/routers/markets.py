from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.context import MarketContextResponse
from app.schemas.coverage import MarketCoverageResponse
from app.schemas.markets import MarketDetailResponse, MarketListResponse, MarketTimeSeriesResponse
from app.services.context import get_market_context
from app.services.coverage import get_market_coverage
from app.services.markets import get_market_detail, get_market_timeseries, list_markets

router = APIRouter(prefix="/markets", tags=["markets"])

GeoTypeParam = Literal["national", "state", "metro", "county", "zcta"]


@router.get("", response_model=MarketListResponse)
def get_markets(
    db: Annotated[Session, Depends(get_db)],
    geo_type: Annotated[
        GeoTypeParam | None,
        Query(
            description="Canonical geography type. Allowed: national, state, metro, county, zcta.",
        ),
    ] = None,
    state: Annotated[
        str | None,
        Query(
            min_length=2,
            max_length=2,
            pattern="^[A-Za-z]{2}$",
            description="Two-letter US state code, for example MI, OH, TX.",
        ),
    ] = None,
    search: Annotated[
        str | None,
        Query(
            min_length=1,
            max_length=100,
            description="Case-insensitive search across market names, ids, CBSA codes, county FIPS, and ZCTAs.",
        ),
    ] = None,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=100,
            description="Maximum number of markets to return.",
        ),
    ] = 50,
    offset: Annotated[
        int,
        Query(
            ge=0,
            description="Number of markets to skip for pagination.",
        ),
    ] = 0,
) -> MarketListResponse:
    return list_markets(
        db,
        geo_type=geo_type,
        state=state,
        search=search,
        limit=limit,
        offset=offset,
    )


@router.get("/metrics/summary")
def market_metrics_summary(db: Annotated[Session, Depends(get_db)]):
    metric_count_result = db.execute(
        text(
            """
            SELECT COUNT(*) AS count
            FROM analytics.market_monthly_metrics
            """
        )
    )
    metric_count = metric_count_result.scalar()

    latest_period_result = db.execute(
        text(
            """
            SELECT MAX(period_month) AS latest_period
            FROM analytics.market_monthly_metrics
            """
        )
    )
    latest_period = latest_period_result.scalar()

    source_count_result = db.execute(
        text(
            """
            SELECT COUNT(*) AS count
            FROM analytics.market_metric_sources
            """
        )
    )
    source_count = source_count_result.scalar()

    return {
        "market_monthly_metrics_count": metric_count,
        "market_metric_sources_count": source_count,
        "latest_period": latest_period.isoformat() if latest_period else None,
    }


@router.get("/{geo_id}/context", response_model=MarketContextResponse)
def get_market_context_endpoint(
    geo_id: str,
    db: Annotated[Session, Depends(get_db)],
) -> MarketContextResponse:
    return get_market_context(db, geo_id=geo_id)


@router.get("/{geo_id}/coverage", response_model=MarketCoverageResponse)
def get_market_coverage_endpoint(
    geo_id: str,
    db: Annotated[Session, Depends(get_db)],
) -> MarketCoverageResponse:
    return get_market_coverage(db, geo_id=geo_id)


@router.get("/{geo_id}/timeseries", response_model=MarketTimeSeriesResponse)
def get_market_timeseries_endpoint(
    geo_id: str,
    db: Annotated[Session, Depends(get_db)],
    metrics: Annotated[
        str,
        Query(
            min_length=1,
            description=(
                "Comma-separated metric list. Example: "
                "home_price_yoy,rent_yoy,composite_cycle_score"
            ),
        ),
    ],
    start_date: Annotated[
        date | None,
        Query(
            description="Optional inclusive start date in YYYY-MM-DD format.",
        ),
    ] = None,
    end_date: Annotated[
        date | None,
        Query(
            description="Optional inclusive end date in YYYY-MM-DD format.",
        ),
    ] = None,
) -> MarketTimeSeriesResponse:
    return get_market_timeseries(
        db,
        geo_id=geo_id,
        metrics=metrics,
        start_date=start_date,
        end_date=end_date,
    )



@router.get("/{geo_id}", response_model=MarketDetailResponse)
def get_market(
    geo_id: str,
    db: Annotated[Session, Depends(get_db)],
) -> MarketDetailResponse:
    return get_market_detail(db, geo_id=geo_id)

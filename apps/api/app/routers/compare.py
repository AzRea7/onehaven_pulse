from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.compare import MarketCompareResponse
from app.services.compare import compare_markets

router = APIRouter(prefix="/compare", tags=["compare"])


@router.get("/markets", response_model=MarketCompareResponse)
def compare_markets_endpoint(
    db: Annotated[Session, Depends(get_db)],
    geo_ids: Annotated[
        str,
        Query(
            min_length=1,
            description="Comma-separated list of 2 to 5 geo_ids.",
        ),
    ],
    metrics: Annotated[
        str | None,
        Query(
            description=(
                "Optional comma-separated metrics. Defaults to "
                "home_price_yoy,rent_yoy,building_permits,composite_cycle_score."
            ),
        ),
    ] = None,
    start_date: Annotated[
        date | None,
        Query(description="Optional inclusive start date in YYYY-MM-DD format."),
    ] = None,
    end_date: Annotated[
        date | None,
        Query(description="Optional inclusive end date in YYYY-MM-DD format."),
    ] = None,
) -> MarketCompareResponse:
    return compare_markets(
        db,
        raw_geo_ids=geo_ids,
        raw_metrics=metrics,
        start_date=start_date,
        end_date=end_date,
    )

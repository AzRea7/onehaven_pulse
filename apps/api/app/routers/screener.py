from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.screener import MarketScreenerResponse
from app.services.screener import screen_markets

router = APIRouter(prefix="/markets", tags=["markets"])


@router.get("/screener", response_model=MarketScreenerResponse)
def get_market_screener(
    geo_type: str | None = Query(default="metro"),
    state: str | None = Query(default=None),
    cycle_phase: str | None = Query(default=None),
    investor_signal: str | None = Query(default=None),
    min_confidence: float | None = Query(default=None, ge=0, le=1),
    min_price_growth: float | None = Query(default=None),
    max_price_growth: float | None = Query(default=None),
    min_rent_growth: float | None = Query(default=None),
    max_inventory_growth: float | None = Query(default=None),
    max_payment_to_income: float | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    return screen_markets(
        db,
        geo_type=geo_type,
        state=state,
        cycle_phase=cycle_phase,
        investor_signal=investor_signal,
        min_confidence=min_confidence,
        min_price_growth=min_price_growth,
        max_price_growth=max_price_growth,
        min_rent_growth=min_rent_growth,
        max_inventory_growth=max_inventory_growth,
        max_payment_to_income=max_payment_to_income,
        limit=limit,
        offset=offset,
    )

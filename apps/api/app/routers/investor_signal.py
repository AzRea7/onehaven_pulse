from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.investor_signal import InvestorMarketSignal
from app.services.investor_signal import get_investor_signal


router = APIRouter(prefix="/markets", tags=["investor-signal"])


@router.get("/{geo_id}/investor-signal", response_model=InvestorMarketSignal)
def read_market_investor_signal(
    geo_id: str,
    db: Session = Depends(get_db),
) -> InvestorMarketSignal:
    return get_investor_signal(db, geo_id)

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.ai.compare_summary import build_compare_summary
from app.ai.schemas import CompareSummaryRequest, CompareSummaryResponse


router = APIRouter(prefix="/ai", tags=["ai"])


@router.post("/compare-summary", response_model=CompareSummaryResponse)
def create_compare_summary(request: CompareSummaryRequest) -> CompareSummaryResponse:
    try:
        return build_compare_summary(request)
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Failed to build compare summary from deterministic tool payloads.",
                "error": str(exc),
            },
        ) from exc

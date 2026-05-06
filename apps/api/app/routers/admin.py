from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.source_freshness import SourceFreshnessResponse
from app.services.source_freshness import list_source_freshness

from app.schemas.pipeline_observability import (
    PipelineRunDetail,
    PipelineRunsResponse,
    PipelineRunSummary,
)
from app.services.pipeline_observability import (
    get_pipeline_run_detail,
    get_pipeline_run_summary,
    list_pipeline_runs,
)

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/source-freshness", response_model=SourceFreshnessResponse, response_model_by_alias=False)
def get_source_freshness(
    source: str | None = Query(default=None),
    dataset: str | None = Query(default=None),
    status: str | None = Query(default=None),
    stale_only: bool | None = Query(default=None),
    search: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    refresh: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> SourceFreshnessResponse:
    return list_source_freshness(
        db,
        source=source,
        dataset=dataset,
        status=status,
        stale_only=stale_only,
        search=search,
        limit=limit,
        refresh=refresh,
    )

@router.get("/pipeline-runs", response_model=PipelineRunsResponse)
def get_pipeline_runs(
    source: str | None = Query(default=None),
    dataset: str | None = Query(default=None),
    status: str | None = Query(default=None),
    pipeline_name: str | None = Query(default=None),
    search: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
) -> PipelineRunsResponse:
    return list_pipeline_runs(
        db,
        source=source,
        dataset=dataset,
        status=status,
        pipeline_name=pipeline_name,
        search=search,
        limit=limit,
    )


@router.get("/pipeline-runs/summary", response_model=PipelineRunSummary)
def get_pipeline_runs_summary(
    source: str | None = Query(default=None),
    dataset: str | None = Query(default=None),
    status: str | None = Query(default=None),
    pipeline_name: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> PipelineRunSummary:
    return get_pipeline_run_summary(
        db,
        source=source,
        dataset=dataset,
        status=status,
        pipeline_name=pipeline_name,
    )


@router.get("/pipeline-runs/{run_id}", response_model=PipelineRunDetail)
def get_pipeline_run(
    run_id: str,
    db: Session = Depends(get_db),
) -> PipelineRunDetail:
    detail = get_pipeline_run_detail(db, run_id=run_id)

    if detail is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "pipeline_run_not_found",
                "message": "Pipeline run not found.",
                "details": {"run_id": run_id},
            },
        )

    return detail


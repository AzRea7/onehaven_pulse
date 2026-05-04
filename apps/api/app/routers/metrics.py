from fastapi import APIRouter, Response

from app.services.metric_catalog import MetricDefinition, get_metric_catalog

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/catalog", response_model=list[MetricDefinition])
def get_metrics_catalog(response: Response) -> list[MetricDefinition]:
    response.headers["Cache-Control"] = "public, max-age=3600"
    return get_metric_catalog()

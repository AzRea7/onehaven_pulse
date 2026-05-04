from pydantic import BaseModel


class MarketCoverageResponse(BaseModel):
    geo_id: str
    latest_data_period: str | None = None
    latest_scoreable_period: str | None = None
    coverage: dict[str, bool]
    available_metrics: list[str]
    missing_score_inputs: list[str]
    data_status: str

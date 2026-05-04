from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.map import GeoJsonFeatureCollection
from app.services.map import get_market_map

router = APIRouter(prefix="/map", tags=["map"])

MapGeoType = Literal["state", "metro", "county", "zcta"]


@router.get("/markets", response_model=GeoJsonFeatureCollection)
def get_market_map_endpoint(
    response: Response,
    db: Annotated[Session, Depends(get_db)],
    geo_type: Annotated[
        MapGeoType,
        Query(
            description="Map geography level. Allowed: state, metro, county, zcta.",
        ),
    ] = "metro",
    metric: Annotated[
        str,
        Query(
            min_length=1,
            description=(
                "Metric to include in GeoJSON properties. Example: "
                "home_price_yoy, rent_yoy, building_permits, composite_cycle_score."
            ),
        ),
    ] = "composite_cycle_score",
    period_month: Annotated[
        date | None,
        Query(
            description="Optional map period in YYYY-MM-DD format. Defaults to latest available period for the selected metric.",
        ),
    ] = None,
) -> GeoJsonFeatureCollection:
    response.headers["Cache-Control"] = "public, max-age=300"

    return get_market_map(
        db,
        geo_type=geo_type,
        metric=metric,
        period_month=period_month,
    )

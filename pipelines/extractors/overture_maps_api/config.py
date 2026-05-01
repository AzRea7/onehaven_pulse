from dataclasses import dataclass

from pipelines.common.settings import settings


@dataclass(frozen=True)
class OvertureMapsApiDataset:
    dataset: str
    endpoint: str
    filename: str
    description: str
    expected_frequency: str
    base_url: str
    area_slug: str
    area_name: str
    country: str
    lat: float
    lng: float
    radius: int
    categories: str
    brand_name: str
    limit: int


OVERTURE_MAPS_API_PLACES = OvertureMapsApiDataset(
    dataset="places",
    endpoint=settings.overture_maps_api_endpoint,
    filename=(
        f"overture_maps_api_{settings.overture_maps_api_area_slug}_"
        f"{settings.overture_maps_api_endpoint}.json"
    ),
    description="Overture Maps API places response",
    expected_frequency="on_demand",
    base_url=settings.overture_maps_api_base_url,
    area_slug=settings.overture_maps_api_area_slug,
    area_name=settings.overture_maps_api_area_name,
    country=settings.overture_maps_api_country,
    lat=settings.overture_maps_api_lat,
    lng=settings.overture_maps_api_lng,
    radius=settings.overture_maps_api_radius,
    categories=settings.overture_maps_api_categories,
    brand_name=settings.overture_maps_api_brand_name,
    limit=settings.overture_maps_api_limit,
)

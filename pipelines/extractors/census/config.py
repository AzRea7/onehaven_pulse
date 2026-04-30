from dataclasses import dataclass

from pipelines.common.settings import settings


@dataclass(frozen=True)
class CensusGeographyDataset:
    dataset: str
    geo_type: str
    url: str
    filename: str
    description: str
    expected_frequency: str
    geometry_source: str
    geometry_year: int


CENSUS_STATE = CensusGeographyDataset(
    dataset="state_boundaries",
    geo_type="state",
    url=settings.census_state_url,
    filename=f"cb_{settings.census_year}_us_state_500k.zip",
    description="Census cartographic boundary state shapefile",
    expected_frequency="annual",
    geometry_source="census_cartographic_boundary",
    geometry_year=settings.census_year,
)


CENSUS_CBSA = CensusGeographyDataset(
    dataset="cbsa_boundaries",
    geo_type="metro",
    url=settings.census_cbsa_url,
    filename=f"tl_{settings.census_year}_us_cbsa.zip",
    description="Census TIGER/Line CBSA shapefile",
    expected_frequency="annual",
    geometry_source="census_tigerline",
    geometry_year=settings.census_year,
)


CENSUS_GEOGRAPHY_DATASETS = [
    CENSUS_STATE,
    CENSUS_CBSA,
]

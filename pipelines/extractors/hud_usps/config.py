from dataclasses import dataclass

from pipelines.common.settings import settings


@dataclass(frozen=True)
class HudUspsDataset:
    dataset: str
    crosswalk_type: str
    api_type: int
    filename: str
    description: str
    expected_frequency: str
    year: int
    quarter: int
    query: str


HUD_USPS_ZIP_TRACT = HudUspsDataset(
    dataset="zip_crosswalk",
    crosswalk_type="zip_tract",
    api_type=1,
    filename=f"hud_usps_zip_tract_{settings.hud_usps_year}_q{settings.hud_usps_quarter}.json",
    description="HUD-USPS ZIP to Census Tract Crosswalk",
    expected_frequency="quarterly",
    year=settings.hud_usps_year,
    quarter=settings.hud_usps_quarter,
    query=settings.hud_usps_query,
)


HUD_USPS_ZIP_COUNTY = HudUspsDataset(
    dataset="zip_crosswalk",
    crosswalk_type="zip_county",
    api_type=2,
    filename=f"hud_usps_zip_county_{settings.hud_usps_year}_q{settings.hud_usps_quarter}.json",
    description="HUD-USPS ZIP to County Crosswalk",
    expected_frequency="quarterly",
    year=settings.hud_usps_year,
    quarter=settings.hud_usps_quarter,
    query=settings.hud_usps_query,
)


HUD_USPS_ZIP_CBSA = HudUspsDataset(
    dataset="zip_crosswalk",
    crosswalk_type="zip_cbsa",
    api_type=3,
    filename=f"hud_usps_zip_cbsa_{settings.hud_usps_year}_q{settings.hud_usps_quarter}.json",
    description="HUD-USPS ZIP to CBSA Crosswalk",
    expected_frequency="quarterly",
    year=settings.hud_usps_year,
    quarter=settings.hud_usps_quarter,
    query=settings.hud_usps_query,
)


HUD_USPS_DATASETS = [
    HUD_USPS_ZIP_TRACT,
    HUD_USPS_ZIP_COUNTY,
    HUD_USPS_ZIP_CBSA,
]

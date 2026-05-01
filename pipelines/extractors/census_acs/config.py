from dataclasses import dataclass

from pipelines.common.settings import settings

ACS_PROFILE_VARIABLES = [
    "NAME",
    "DP05_0001E",  # Total population
    "DP03_0062E",  # Median household income
    "DP04_0001E",  # Total housing units
    "DP04_0002E",  # Occupied housing units
    "DP04_0003E",  # Vacant housing units
    "DP04_0046E",  # Owner-occupied housing units
    "DP04_0047E",  # Renter-occupied housing units
    "DP04_0089E",  # Median gross rent
    "DP04_0142PE",  # Gross rent 35 percent or more of household income
]


@dataclass(frozen=True)
class CensusAcsDataset:
    dataset: str
    geography_level: str
    year: int
    endpoint_path: str
    filename: str
    description: str
    expected_frequency: str
    variables: list[str]
    params: dict[str, str]


def _profile_endpoint_path() -> str:
    return f"{settings.census_acs_year}/acs/acs5/profile"


CENSUS_ACS_STATE = CensusAcsDataset(
    dataset="profile",
    geography_level="state",
    year=settings.census_acs_year,
    endpoint_path=_profile_endpoint_path(),
    filename=f"acs_{settings.census_acs_year}_profile_state.json",
    description="ACS 5-Year Data Profile for all states",
    expected_frequency="annual",
    variables=ACS_PROFILE_VARIABLES,
    params={
        "for": "state:*",
    },
)


CENSUS_ACS_COUNTY = CensusAcsDataset(
    dataset="profile",
    geography_level="county",
    year=settings.census_acs_year,
    endpoint_path=_profile_endpoint_path(),
    filename=f"acs_{settings.census_acs_year}_profile_county.json",
    description="ACS 5-Year Data Profile for all counties",
    expected_frequency="annual",
    variables=ACS_PROFILE_VARIABLES,
    params={
        "for": "county:*",
    },
)


CENSUS_ACS_METRO = CensusAcsDataset(
    dataset="profile",
    geography_level="metro",
    year=settings.census_acs_year,
    endpoint_path=_profile_endpoint_path(),
    filename=f"acs_{settings.census_acs_year}_profile_metro.json",
    description="ACS 5-Year Data Profile for all metropolitan and micropolitan statistical areas",
    expected_frequency="annual",
    variables=ACS_PROFILE_VARIABLES,
    params={
        "for": "metropolitan statistical area/micropolitan statistical area:*",
    },
)


CENSUS_ACS_DATASETS = [
    CENSUS_ACS_STATE,
    CENSUS_ACS_COUNTY,
    CENSUS_ACS_METRO,
]

from dataclasses import dataclass
import os

from pipelines.common.settings import settings


@dataclass(frozen=True)
class BlsLausSeries:
    series_id: str
    label: str
    geography_level: str
    measure: str
    geo_reference: str




def _configured_series_ids() -> list[str]:
    raw = os.getenv("BLS_LAUS_SERIES_IDS", "").strip()
    if not raw:
        return []

    return [
        value.strip()
        for value in raw.split(",")
        if value.strip()
    ]

BLS_LAUS_SERIES = [
    # United States, national CPS labor force series.
    BlsLausSeries(
        series_id="LNS14000000",
        label="United States unemployment rate",
        geography_level="national",
        measure="unemployment_rate",
        geo_reference="US",
    ),
    BlsLausSeries(
        series_id="LNS11000000",
        label="United States labor force",
        geography_level="national",
        measure="labor_force",
        geo_reference="US",
    ),
    BlsLausSeries(
        series_id="LNS12000000",
        label="United States employment",
        geography_level="national",
        measure="employment",
        geo_reference="US",
    ),
    BlsLausSeries(
        series_id="LNS13000000",
        label="United States unemployment count",
        geography_level="national",
        measure="unemployment_count",
        geo_reference="US",
    ),

    # Michigan.
    BlsLausSeries(
        series_id="LASST260000000000003",
        label="Michigan unemployment rate",
        geography_level="state",
        measure="unemployment_rate",
        geo_reference="MI",
    ),
    BlsLausSeries(
        series_id="LASST260000000000006",
        label="Michigan labor force",
        geography_level="state",
        measure="labor_force",
        geo_reference="MI",
    ),
    BlsLausSeries(
        series_id="LASST260000000000005",
        label="Michigan employment",
        geography_level="state",
        measure="employment",
        geo_reference="MI",
    ),
    BlsLausSeries(
        series_id="LASST260000000000004",
        label="Michigan unemployment count",
        geography_level="state",
        measure="unemployment_count",
        geo_reference="MI",
    ),

    # Detroit-Warren-Dearborn, MI Metropolitan Statistical Area.
    BlsLausSeries(
        series_id="LAUMT261982000000003",
        label="Detroit-Warren-Dearborn, MI unemployment rate",
        geography_level="metro",
        measure="unemployment_rate",
        geo_reference="metro_19820",
    ),
    BlsLausSeries(
        series_id="LAUMT261982000000006",
        label="Detroit-Warren-Dearborn, MI labor force",
        geography_level="metro",
        measure="labor_force",
        geo_reference="metro_19820",
    ),
    BlsLausSeries(
        series_id="LAUMT261982000000005",
        label="Detroit-Warren-Dearborn, MI employment",
        geography_level="metro",
        measure="employment",
        geo_reference="metro_19820",
    ),
    BlsLausSeries(
        series_id="LAUMT261982000000004",
        label="Detroit-Warren-Dearborn, MI unemployment count",
        geography_level="metro",
        measure="unemployment_count",
        geo_reference="metro_19820",
    ),

    # Texas.
    BlsLausSeries(
        series_id="LASST480000000000003",
        label="Texas unemployment rate",
        geography_level="state",
        measure="unemployment_rate",
        geo_reference="TX",
    ),
    BlsLausSeries(
        series_id="LASST480000000000006",
        label="Texas labor force",
        geography_level="state",
        measure="labor_force",
        geo_reference="TX",
    ),
    BlsLausSeries(
        series_id="LASST480000000000005",
        label="Texas employment",
        geography_level="state",
        measure="employment",
        geo_reference="TX",
    ),
    BlsLausSeries(
        series_id="LASST480000000000004",
        label="Texas unemployment count",
        geography_level="state",
        measure="unemployment_count",
        geo_reference="TX",
    ),

    # Florida.
    BlsLausSeries(
        series_id="LASST120000000000003",
        label="Florida unemployment rate",
        geography_level="state",
        measure="unemployment_rate",
        geo_reference="FL",
    ),
    BlsLausSeries(
        series_id="LASST120000000000006",
        label="Florida labor force",
        geography_level="state",
        measure="labor_force",
        geo_reference="FL",
    ),
    BlsLausSeries(
        series_id="LASST120000000000005",
        label="Florida employment",
        geography_level="state",
        measure="employment",
        geo_reference="FL",
    ),
    BlsLausSeries(
        series_id="LASST120000000000004",
        label="Florida unemployment count",
        geography_level="state",
        measure="unemployment_count",
        geo_reference="FL",
    ),
]


@dataclass(frozen=True)
class BlsLausDataset:
    dataset: str
    filename: str
    description: str
    expected_frequency: str
    start_year: int
    end_year: int
    series: list[BlsLausSeries]


BLS_LAUS_DATASET = BlsLausDataset(
    dataset="labor_market",
    filename=f"bls_laus_labor_market_{settings.bls_laus_start_year}_{settings.bls_laus_end_year}.json",
    description="BLS LAUS labor market time series starter set",
    expected_frequency="monthly",
    start_year=settings.bls_laus_start_year,
    end_year=settings.bls_laus_end_year,
    series=BLS_LAUS_SERIES,
)

_env_series_ids = _configured_series_ids()
_existing_series_ids = {series.series_id for series in BLS_LAUS_SERIES}

for _series_id in _env_series_ids:
    if _series_id not in _existing_series_ids:
        BLS_LAUS_SERIES.append(
            BlsLausSeries(
                series_id=_series_id,
                label=f"{_series_id} unemployment rate",
                geography_level="metro",
                measure="unemployment_rate",
                geo_reference=_series_id,
            )
        )
        _existing_series_ids.add(_series_id)


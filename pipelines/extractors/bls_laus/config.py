from dataclasses import dataclass

from pipelines.common.settings import settings


@dataclass(frozen=True)
class BlsLausSeries:
    series_id: str
    label: str
    geography_level: str
    measure: str
    geo_reference: str


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

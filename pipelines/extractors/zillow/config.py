from dataclasses import dataclass

from pipelines.common.settings import settings


@dataclass(frozen=True)
class ZillowDataset:
    dataset: str
    metric_name: str
    filename: str
    description: str
    expected_frequency: str
    local_path: str | None
    url: str | None


ZILLOW_ZHVI = ZillowDataset(
    dataset="zhvi",
    metric_name="zhvi",
    filename="zhvi.csv",
    description="Zillow Home Value Index, Metro and U.S., all homes, smoothed seasonally adjusted",
    expected_frequency="monthly",
    local_path=settings.zillow_zhvi_local_path,
    url=settings.zillow_zhvi_url,
)


ZILLOW_ZORI = ZillowDataset(
    dataset="zori",
    metric_name="zori",
    filename="zori.csv",
    description="Zillow Observed Rent Index, Metro and U.S., all homes plus multifamily, smoothed",
    expected_frequency="monthly",
    local_path=settings.zillow_zori_local_path,
    url=settings.zillow_zori_url,
)


ZILLOW_DATASETS = [
    ZILLOW_ZHVI,
    ZILLOW_ZORI,
]

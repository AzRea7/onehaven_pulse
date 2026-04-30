from dataclasses import dataclass

from pipelines.common.settings import settings


@dataclass(frozen=True)
class RedfinDataset:
    dataset: str
    metric_name: str
    filename: str
    description: str
    expected_frequency: str
    release_cadence_note: str
    local_path: str | None
    url: str | None


REDFIN_MARKET_TRACKER = RedfinDataset(
    dataset="market_tracker",
    metric_name="market_tracker",
    filename="market_tracker.csv",
    description="Redfin Data Center monthly housing market data, preferably Metro geography",
    expected_frequency="monthly",
    release_cadence_note=(
        "Monthly Redfin housing market data is released during the Friday "
        "of the third full week of the month for the previous month."
    ),
    local_path=settings.redfin_market_tracker_local_path,
    url=settings.redfin_market_tracker_url,
)

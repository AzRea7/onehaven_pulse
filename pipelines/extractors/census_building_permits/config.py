from dataclasses import dataclass

from pipelines.common.settings import settings


@dataclass(frozen=True)
class CensusBpsDataset:
    dataset: str
    geography_level: str
    period_type: str
    source_period_label: str
    filename: str
    description: str
    expected_frequency: str
    release_cadence_note: str
    local_path: str | None
    url: str | None
    required: bool = False


CENSUS_BPS_STATE_MONTHLY = CensusBpsDataset(
    dataset="permits",
    geography_level="state",
    period_type="monthly",
    source_period_label="2026-01",
    filename="state_jan_2026.xls",
    description="Census Building Permits Survey state-level January 2026 permits file",
    expected_frequency="monthly",
    release_cadence_note=(
        "Current Census BPS monthly files may only be available for the latest month."
    ),
    local_path=settings.census_bps_state_monthly_local_path,
    url=settings.census_bps_state_monthly_url,
    required=True,
)


CENSUS_BPS_CBSA_MONTHLY = CensusBpsDataset(
    dataset="permits",
    geography_level="cbsa",
    period_type="monthly",
    source_period_label="2026-01",
    filename="cbsa_jan_2026.xls",
    description="Census Building Permits Survey CBSA-level January 2026 permits file",
    expected_frequency="monthly",
    release_cadence_note=(
        "Current Census BPS monthly files may only be available for the latest month."
    ),
    local_path=settings.census_bps_cbsa_monthly_local_path,
    url=settings.census_bps_cbsa_monthly_url,
    required=True,
)


CENSUS_BPS_STATE_ANNUAL = CensusBpsDataset(
    dataset="permits",
    geography_level="state",
    period_type="annual",
    source_period_label="2025",
    filename="state_annual_2025.xls",
    description="Census Building Permits Survey state-level annual 2025 permits file",
    expected_frequency="annual",
    release_cadence_note="Annual Census BPS files provide prior-year permit totals.",
    local_path=settings.census_bps_state_annual_local_path,
    url=settings.census_bps_state_annual_url,
    required=False,
)


CENSUS_BPS_CBSA_ANNUAL = CensusBpsDataset(
    dataset="permits",
    geography_level="cbsa",
    period_type="annual",
    source_period_label="2025",
    filename="cbsa_annual_2025.xls",
    description="Census Building Permits Survey CBSA-level annual 2025 permits file",
    expected_frequency="annual",
    release_cadence_note="Annual Census BPS files provide prior-year permit totals.",
    local_path=settings.census_bps_cbsa_annual_local_path,
    url=settings.census_bps_cbsa_annual_url,
    required=False,
)


CENSUS_BPS_DATASETS = [
    CENSUS_BPS_STATE_MONTHLY,
    CENSUS_BPS_CBSA_MONTHLY,
    CENSUS_BPS_STATE_ANNUAL,
    CENSUS_BPS_CBSA_ANNUAL,
]

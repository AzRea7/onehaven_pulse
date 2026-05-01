from collections.abc import Callable
from dataclasses import dataclass

from pipelines.transforms.smoke.market_metric_smoke_transform import main as run_smoke_transform
from pipelines.transforms.fred.macro_transform import main as run_fred_macro_transform
from pipelines.transforms.fhfa.hpi_transform import main as run_fhfa_hpi_transform
from pipelines.transforms.zillow.value_rent_transform import main as run_zillow_value_rent_transform
from pipelines.transforms.redfin.market_tracker_transform import main as run_redfin_market_tracker_transform
from pipelines.transforms.census_acs.profile_transform import main as run_census_acs_profile_transform
from pipelines.transforms.bls_laus.labor_market_transform import main as run_bls_laus_labor_market_transform
from pipelines.transforms.census_building_permits.permits_transform import main as run_census_bps_permits_transform
from pipelines.transforms.fema_nri.hazard_risk_transform import main as run_fema_nri_hazard_risk_transform

@dataclass(frozen=True)
class TransformDefinition:
    name: str
    description: str
    target_table: str
    runner: Callable[[], None]


TRANSFORMS: dict[str, TransformDefinition] = {
    "fema_nri_hazard_risk": TransformDefinition(
        name="fema_nri_hazard_risk",
        description="Transform FEMA NRI county risk into hazard, loss, vulnerability, and resilience metrics.",
        target_table="analytics.market_monthly_metrics",
        runner=run_fema_nri_hazard_risk_transform,
    ),
    "census_building_permits": TransformDefinition(
        name="census_building_permits",
        description="Transform Census Building Permits Survey data into monthly supply pipeline metrics.",
        target_table="analytics.market_monthly_metrics",
        runner=run_census_bps_permits_transform,
    ),
    "bls_laus_labor_market": TransformDefinition(
        name="bls_laus_labor_market",
        description="Transform BLS LAUS into unemployment rate, labor force, employment, and unemployment count metrics.",
        target_table="analytics.market_monthly_metrics",
        runner=run_bls_laus_labor_market_transform,
    ),
    "census_acs_profile": TransformDefinition(
        name="census_acs_profile",
        description="Transform Census ACS profile into population, income, household, housing, rent burden, and tenure metrics.",
        target_table="analytics.market_monthly_metrics",
        runner=run_census_acs_profile_transform,
    ),
    "redfin_market_tracker": TransformDefinition(
        name="redfin_market_tracker",
        description="Transform Redfin market tracker into inventory, price, sales, and velocity metrics.",
        target_table="analytics.market_monthly_metrics",
        runner=run_redfin_market_tracker_transform,
    ),
    "zillow_value_rent": TransformDefinition(
        name="zillow_value_rent",
        description="Transform Zillow ZHVI/ZORI into monthly value, rent, and growth metrics.",
        target_table="analytics.market_monthly_metrics",
        runner=run_zillow_value_rent_transform,
    ),
    "fhfa_hpi": TransformDefinition(
        name="fhfa_hpi",
        description="Transform FHFA HPI into home price index and appreciation metrics by geography.",
        target_table="analytics.market_monthly_metrics",
        runner=run_fhfa_hpi_transform,
    ),
    "smoke_market_metric": TransformDefinition(
        name="smoke_market_metric",
        description="Smoke test for canonical market metric transform loader.",
        target_table="analytics.market_monthly_metrics",
        runner=run_smoke_transform,
    ),
    "fred_macro_monthly": TransformDefinition(
        name="fred_macro_monthly",
        description="Transform FRED macro, Treasury, recession, and rate series to monthly national market metrics.",
        target_table="analytics.market_monthly_metrics",
        runner=run_fred_macro_transform,
    ),
}


def list_transform_names() -> list[str]:
    return sorted(TRANSFORMS)


def get_transform_definition(name: str) -> TransformDefinition:
    try:
        return TRANSFORMS[name]
    except KeyError as exc:
        allowed = ", ".join(list_transform_names())
        raise ValueError(f"Unknown transform '{name}'. Allowed transforms: {allowed}") from exc


def resolve_transform_names(names: list[str]) -> list[str]:
    if names == ["all"]:
        return list_transform_names()

    for name in names:
        get_transform_definition(name)

    return names

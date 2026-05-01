from collections.abc import Callable
from dataclasses import dataclass

from pipelines.transforms.smoke.market_metric_smoke_transform import main as run_smoke_transform
from pipelines.transforms.fred.macro_transform import main as run_fred_macro_transform
from pipelines.transforms.fhfa.hpi_transform import main as run_fhfa_hpi_transform
from pipelines.transforms.zillow.value_rent_transform import main as run_zillow_value_rent_transform
from pipelines.transforms.redfin.market_tracker_transform import main as run_redfin_market_tracker_transform

@dataclass(frozen=True)
class TransformDefinition:
    name: str
    description: str
    target_table: str
    runner: Callable[[], None]


TRANSFORMS: dict[str, TransformDefinition] = {
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

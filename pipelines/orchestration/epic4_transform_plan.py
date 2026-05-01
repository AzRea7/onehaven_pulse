from dataclasses import dataclass


@dataclass(frozen=True)
class Epic4TransformStep:
    name: str
    audit_pipeline_name: str
    required: bool
    allow_zero_loaded: bool
    reason: str


EPIC4_TRANSFORM_PLAN: tuple[Epic4TransformStep, ...] = (
    Epic4TransformStep(
        name="fred_macro_monthly",
        audit_pipeline_name="fred_macro_monthly_transform",
        required=True,
        allow_zero_loaded=False,
        reason="Macro, rates, CPI, recession, and yield curve metrics.",
    ),
    Epic4TransformStep(
        name="zillow_value_rent",
        audit_pipeline_name="zillow_value_rent_transform",
        required=True,
        allow_zero_loaded=False,
        reason="Zillow home value and rent metrics.",
    ),
    Epic4TransformStep(
        name="fhfa_hpi",
        audit_pipeline_name="fhfa_hpi_transform",
        required=True,
        allow_zero_loaded=False,
        reason="FHFA home price index metrics.",
    ),
    Epic4TransformStep(
        name="redfin_market_tracker",
        audit_pipeline_name="redfin_market_tracker_transform",
        required=True,
        allow_zero_loaded=False,
        reason="Redfin listings, sales, inventory, DOM, and sale-to-list metrics.",
    ),
    Epic4TransformStep(
        name="census_acs_profile",
        audit_pipeline_name="census_acs_profile_transform",
        required=True,
        allow_zero_loaded=False,
        reason="ACS population, income, rent, housing, and tenure metrics.",
    ),
    Epic4TransformStep(
        name="bls_laus_labor_market",
        audit_pipeline_name="bls_laus_labor_market_transform",
        required=True,
        allow_zero_loaded=False,
        reason="BLS labor force, employment, unemployment, and unemployment rate metrics.",
    ),
    Epic4TransformStep(
        name="census_building_permits",
        audit_pipeline_name="census_building_permits_transform",
        required=True,
        allow_zero_loaded=False,
        reason="Census BPS housing supply pipeline metrics.",
    ),
    Epic4TransformStep(
        name="fema_nri_hazard_risk",
        audit_pipeline_name="fema_nri_hazard_risk_transform",
        required=True,
        allow_zero_loaded=False,
        reason="FEMA NRI risk, loss, vulnerability, and resilience metrics.",
    ),
    Epic4TransformStep(
        name="hmda_mortgage_credit",
        audit_pipeline_name="hmda_mortgage_credit_transform",
        required=True,
        allow_zero_loaded=False,
        reason="HMDA mortgage applications, originations, denials, and loan amount metrics.",
    ),
    Epic4TransformStep(
        name="overture_places_amenities",
        audit_pipeline_name="overture_places_amenity_transform",
        required=False,
        allow_zero_loaded=False,
        reason="Overture Places area amenity enrichment.",
    ),
    Epic4TransformStep(
        name="derived_market_ratios",
        audit_pipeline_name="derived_market_ratios_transform",
        required=True,
        allow_zero_loaded=False,
        reason="Derived affordability, rent-to-price, real HPI, and permit-per-capita metrics.",
    ),
)

from dataclasses import dataclass


@dataclass(frozen=True)
class Epic4MetricDefinition:
    metric_name: str
    source: str
    dataset: str
    required: bool
    expected_grain: str
    description: str


EPIC4_METRIC_CATALOG: tuple[Epic4MetricDefinition, ...] = (
    # Smoke / canonical loader
    Epic4MetricDefinition(
        metric_name="median_sale_price",
        source="smoke",
        dataset="market_metric",
        required=False,
        expected_grain="monthly",
        description="Smoke metric validating canonical metric load path.",
    ),

    # FRED macro
    Epic4MetricDefinition("mortgage_rate_30y", "fred", "macro_series", True, "weekly/monthly", "30-year fixed mortgage rate."),
    Epic4MetricDefinition("cpi", "fred", "macro_series", True, "monthly", "Consumer Price Index."),
    Epic4MetricDefinition("unemployment_rate", "fred", "macro_series", True, "monthly", "National unemployment rate."),
    Epic4MetricDefinition("fed_funds_rate", "fred", "macro_series", True, "monthly", "Federal funds rate."),
    Epic4MetricDefinition("recession_indicator", "fred", "macro_series", True, "monthly", "NBER recession indicator."),
    Epic4MetricDefinition("treasury_2yr_rate", "fred", "macro_series", True, "daily/monthly", "2-year Treasury yield."),
    Epic4MetricDefinition("treasury_5yr_rate", "fred", "macro_series", True, "daily/monthly", "5-year Treasury yield."),
    Epic4MetricDefinition("treasury_10yr_rate", "fred", "macro_series", True, "daily/monthly", "10-year Treasury yield."),
    Epic4MetricDefinition("treasury_30yr_rate", "fred", "macro_series", True, "daily/monthly", "30-year Treasury yield."),
    Epic4MetricDefinition("treasury_10yr_2yr_spread", "fred", "macro_series", True, "daily/monthly", "10-year minus 2-year Treasury spread."),
    Epic4MetricDefinition("treasury_10yr_3mo_spread", "fred", "macro_series", True, "daily/monthly", "10-year minus 3-month Treasury spread."),

    # Zillow
    Epic4MetricDefinition("zhvi", "zillow", "zhvi", True, "monthly", "Zillow Home Value Index."),
    Epic4MetricDefinition("zhvi_mom", "zillow", "zhvi", True, "monthly", "ZHVI month-over-month change."),
    Epic4MetricDefinition("zhvi_yoy", "zillow", "zhvi", True, "monthly", "ZHVI year-over-year change."),
    Epic4MetricDefinition("zori", "zillow", "zori", True, "monthly", "Zillow Observed Rent Index."),
    Epic4MetricDefinition("zori_mom", "zillow", "zori", True, "monthly", "ZORI month-over-month change."),
    Epic4MetricDefinition("zori_yoy", "zillow", "zori", True, "monthly", "ZORI year-over-year change."),

    # FHFA
    Epic4MetricDefinition("home_price_index", "fhfa", "hpi", True, "monthly/quarterly", "FHFA home price index."),
    Epic4MetricDefinition("home_price_index_mom", "fhfa", "hpi", True, "monthly", "FHFA HPI month-over-month change."),
    Epic4MetricDefinition("home_price_index_yoy", "fhfa", "hpi", True, "monthly/quarterly", "FHFA HPI year-over-year change."),

    # Redfin
    Epic4MetricDefinition("median_sale_price", "redfin", "market_tracker", True, "monthly", "Median sale price."),
    Epic4MetricDefinition("median_sale_price_mom", "redfin", "market_tracker", False, "monthly", "Median sale price MoM."),
    Epic4MetricDefinition("median_sale_price_yoy", "redfin", "market_tracker", False, "monthly", "Median sale price YoY."),
    Epic4MetricDefinition("homes_sold", "redfin", "market_tracker", True, "monthly", "Homes sold."),
    Epic4MetricDefinition("homes_sold_yoy", "redfin", "market_tracker", False, "monthly", "Homes sold YoY."),
    Epic4MetricDefinition("new_listings", "redfin", "market_tracker", True, "monthly", "New listings."),
    Epic4MetricDefinition("new_listings_yoy", "redfin", "market_tracker", False, "monthly", "New listings YoY."),
    Epic4MetricDefinition("active_listings", "redfin", "market_tracker", True, "monthly", "Active listings / inventory."),
    Epic4MetricDefinition("active_listings_yoy", "redfin", "market_tracker", False, "monthly", "Active listings YoY."),
    Epic4MetricDefinition("median_days_on_market", "redfin", "market_tracker", True, "monthly", "Median days on market."),
    Epic4MetricDefinition("sale_to_list_ratio", "redfin", "market_tracker", True, "monthly", "Sale-to-list ratio."),

    # ACS
    Epic4MetricDefinition("population", "census_acs", "profile", True, "annual", "Population."),
    Epic4MetricDefinition("population_yoy", "census_acs", "profile", False, "annual", "Population YoY."),
    Epic4MetricDefinition("households", "census_acs", "profile", True, "annual", "Households."),
    Epic4MetricDefinition("median_household_income", "census_acs", "profile", True, "annual", "Median household income."),
    Epic4MetricDefinition("median_gross_rent", "census_acs", "profile", True, "annual", "Median gross rent."),
    Epic4MetricDefinition("median_rent", "census_acs", "profile", False, "annual", "Median rent."),
    Epic4MetricDefinition("housing_units", "census_acs", "profile", True, "annual", "Housing units."),
    Epic4MetricDefinition("occupied_housing_units", "census_acs", "profile", True, "annual", "Occupied housing units."),
    Epic4MetricDefinition("vacant_housing_units", "census_acs", "profile", True, "annual", "Vacant housing units."),
    Epic4MetricDefinition("owner_occupied_housing_units", "census_acs", "profile", True, "annual", "Owner occupied housing units."),
    Epic4MetricDefinition("renter_occupied_housing_units", "census_acs", "profile", True, "annual", "Renter occupied housing units."),
    Epic4MetricDefinition("owner_occupied_share", "census_acs", "profile", True, "annual", "Owner occupied share."),
    Epic4MetricDefinition("renter_occupied_share", "census_acs", "profile", True, "annual", "Renter occupied share."),
    Epic4MetricDefinition("rent_burden_pct", "census_acs", "profile", True, "annual", "Rent burden percent."),

    # BLS LAUS
    Epic4MetricDefinition("labor_force", "bls_laus", "labor_market", True, "monthly", "Labor force."),
    Epic4MetricDefinition("employment", "bls_laus", "labor_market", True, "monthly", "Employment."),
    Epic4MetricDefinition("unemployment_count", "bls_laus", "labor_market", True, "monthly", "Unemployment count."),
    Epic4MetricDefinition("unemployment_rate", "bls_laus", "labor_market", True, "monthly", "Local unemployment rate."),

    # Census BPS
    Epic4MetricDefinition("building_permits", "census", "building_permits", True, "monthly/annual", "Building permits."),
    Epic4MetricDefinition("single_family_permits", "census", "building_permits", True, "monthly/annual", "Single-family permits."),
    Epic4MetricDefinition("multi_family_permits", "census", "building_permits", True, "monthly/annual", "Multifamily permits."),
    Epic4MetricDefinition("permit_units", "census", "building_permits", True, "monthly/annual", "Permit units."),

    # FEMA NRI
    Epic4MetricDefinition("hazard_risk_score", "fema_nri", "county_risk", True, "annual", "Hazard risk score."),
    Epic4MetricDefinition("expected_annual_loss", "fema_nri", "county_risk", True, "annual", "Expected annual loss."),
    Epic4MetricDefinition("social_vulnerability_score", "fema_nri", "county_risk", True, "annual", "Social vulnerability score."),
    Epic4MetricDefinition("community_resilience_score", "fema_nri", "county_risk", True, "annual", "Community resilience score."),

    # HMDA
    Epic4MetricDefinition("hmda_applications", "hmda", "modified_lar", True, "annual", "HMDA applications."),
    Epic4MetricDefinition("hmda_originations", "hmda", "modified_lar", True, "annual", "HMDA originations."),
    Epic4MetricDefinition("hmda_denials", "hmda", "modified_lar", True, "annual", "HMDA denials."),
    Epic4MetricDefinition("hmda_denial_rate", "hmda", "modified_lar", True, "annual", "HMDA denial rate."),
    Epic4MetricDefinition("hmda_median_loan_amount", "hmda", "modified_lar", True, "annual", "HMDA median loan amount."),

    # Overture
    Epic4MetricDefinition("amenity_place_count", "overture_maps_api", "places", False, "monthly", "Overture place count."),
    Epic4MetricDefinition("amenity_school_count", "overture_maps_api", "places", False, "monthly", "School amenity count."),
    Epic4MetricDefinition("amenity_healthcare_count", "overture_maps_api", "places", False, "monthly", "Healthcare amenity count."),
    Epic4MetricDefinition("amenity_grocery_count", "overture_maps_api", "places", False, "monthly", "Grocery amenity count."),
    Epic4MetricDefinition("amenity_food_service_count", "overture_maps_api", "places", False, "monthly", "Food service amenity count."),
    Epic4MetricDefinition("amenity_bank_count", "overture_maps_api", "places", False, "monthly", "Bank amenity count."),

    # Derived
    Epic4MetricDefinition("estimated_monthly_payment", "derived", "market_ratios", True, "monthly", "Estimated monthly mortgage payment."),
    Epic4MetricDefinition("payment_to_income_ratio", "derived", "market_ratios", False, "monthly/annual overlap", "Payment-to-income ratio."),
    Epic4MetricDefinition("price_to_income_ratio", "derived", "market_ratios", False, "monthly/annual overlap", "Price-to-income ratio."),
    Epic4MetricDefinition("rent_to_price_ratio", "derived", "market_ratios", True, "monthly", "Rent-to-price ratio."),
    Epic4MetricDefinition("real_home_price_index", "derived", "market_ratios", True, "monthly", "Real home price index."),
    Epic4MetricDefinition("permits_per_1000_people", "derived", "market_ratios", False, "monthly/annual overlap", "Permits per 1,000 people."),
)

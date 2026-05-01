from dataclasses import dataclass


@dataclass(frozen=True)
class RawPipelineDefinition:
    name: str
    module: str
    source: str
    dataset: str
    description: str
    is_heavy: bool = False
    requires_api_key: bool = False


RAW_PIPELINES: tuple[RawPipelineDefinition, ...] = (
    RawPipelineDefinition(
        name="fred",
        module="pipelines.extractors.fred.extract",
        source="fred",
        dataset="macro_series",
        description="FRED macro, mortgage-rate, Treasury, inflation, labor, and recession series",
        requires_api_key=True,
    ),
    RawPipelineDefinition(
        name="fhfa",
        module="pipelines.extractors.fhfa.extract",
        source="fhfa",
        dataset="hpi",
        description="FHFA House Price Index raw extract",
    ),
    RawPipelineDefinition(
        name="zillow",
        module="pipelines.extractors.zillow.extract",
        source="zillow",
        dataset="zhvi_zori",
        description="Zillow ZHVI and ZORI raw extracts",
    ),
    RawPipelineDefinition(
        name="redfin",
        module="pipelines.extractors.redfin.extract",
        source="redfin",
        dataset="market_tracker",
        description="Redfin market tracker raw extract",
    ),
    RawPipelineDefinition(
        name="census_geo",
        module="pipelines.extractors.census_geo.extract",
        source="census",
        dataset="geography",
        description="Census TIGER/Cartographic geography raw extracts",
    ),
    RawPipelineDefinition(
        name="census_bps",
        module="pipelines.extractors.census_building_permits.extract",
        source="census",
        dataset="building_permits",
        description="Census Building Permits Survey raw extracts",
    ),
    RawPipelineDefinition(
        name="census_acs",
        module="pipelines.extractors.census_acs.extract",
        source="census_acs",
        dataset="profile",
        description="Census ACS profile data raw extracts",
        requires_api_key=False,
    ),
    RawPipelineDefinition(
        name="hud_usps",
        module="pipelines.extractors.hud_usps.extract",
        source="hud_usps",
        dataset="zip_crosswalk",
        description="HUD-USPS ZIP Code Crosswalk raw extract",
        requires_api_key=True,
    ),
    RawPipelineDefinition(
        name="bls_laus",
        module="pipelines.extractors.bls_laus.extract",
        source="bls",
        dataset="laus",
        description="BLS LAUS labor market raw extract",
        requires_api_key=False,
    ),
    RawPipelineDefinition(
        name="hmda",
        module="pipelines.extractors.hmda.extract",
        source="hmda",
        dataset="modified_lar",
        description="HMDA Modified LAR raw extract",
        is_heavy=True,
    ),
    RawPipelineDefinition(
        name="fema_nri",
        module="pipelines.extractors.fema_nri.extract",
        source="fema_nri",
        dataset="county_risk",
        description="FEMA National Risk Index county risk raw extract",
    ),
    RawPipelineDefinition(
        name="overture_maps",
        module="pipelines.extractors.overture_maps_api.extract",
        source="overture_maps",
        dataset="places",
        description="Overture Maps API places raw extract",
        requires_api_key=True,
    ),
)


RAW_PIPELINE_BY_NAME = {pipeline.name: pipeline for pipeline in RAW_PIPELINES}


DEFAULT_RAW_PIPELINE_ORDER: tuple[str, ...] = tuple(
    pipeline.name for pipeline in RAW_PIPELINES
)


def list_pipeline_names() -> list[str]:
    return list(DEFAULT_RAW_PIPELINE_ORDER)


def get_pipeline_definition(name: str) -> RawPipelineDefinition:
    try:
        return RAW_PIPELINE_BY_NAME[name]
    except KeyError as exc:
        valid_names = ", ".join(list_pipeline_names())
        raise ValueError(
            f"Unknown raw pipeline: {name}. Valid names: {valid_names}"
        ) from exc


def resolve_pipeline_names(requested_names: list[str] | None) -> list[str]:
    if not requested_names:
        return list_pipeline_names()

    if len(requested_names) == 1 and requested_names[0] == "all":
        return list_pipeline_names()

    resolved: list[str] = []

    for name in requested_names:
        clean_name = name.strip()

        if not clean_name:
            continue

        get_pipeline_definition(clean_name)
        resolved.append(clean_name)

    return resolved

from dataclasses import dataclass

from pipelines.common.settings import settings


@dataclass(frozen=True)
class FemaNriDataset:
    dataset: str
    filename: str
    description: str
    expected_frequency: str
    source_mode: str
    arcgis_item_id: str
    arcgis_service_url: str
    arcgis_layer_id: int
    arcgis_page_size: int
    arcgis_where: str
    arcgis_out_fields: str
    version: str
    release_label: str


FEMA_NRI_COUNTY_RISK = FemaNriDataset(
    dataset="county_risk",
    filename="fema_nri_counties_arcgis.json",
    description="FEMA National Risk Index Counties from ArcGIS REST Feature Layer",
    expected_frequency="periodic",
    source_mode=settings.fema_nri_source_mode,
    arcgis_item_id=settings.fema_nri_arcgis_item_id,
    arcgis_service_url=settings.fema_nri_arcgis_service_url,
    arcgis_layer_id=settings.fema_nri_arcgis_layer_id,
    arcgis_page_size=settings.fema_nri_arcgis_page_size,
    arcgis_where=settings.fema_nri_arcgis_where,
    arcgis_out_fields=settings.fema_nri_arcgis_out_fields,
    version=settings.fema_nri_version,
    release_label=settings.fema_nri_release_label,
)

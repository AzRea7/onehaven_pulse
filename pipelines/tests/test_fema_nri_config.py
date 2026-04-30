from pipelines.extractors.fema_nri.config import FEMA_NRI_COUNTY_RISK


def test_fema_nri_county_risk_configured():
    assert FEMA_NRI_COUNTY_RISK.dataset == "county_risk"
    assert FEMA_NRI_COUNTY_RISK.source_mode == "arcgis"
    assert FEMA_NRI_COUNTY_RISK.arcgis_item_id
    assert FEMA_NRI_COUNTY_RISK.arcgis_layer_id >= 0
    assert FEMA_NRI_COUNTY_RISK.arcgis_page_size >= 1
    assert FEMA_NRI_COUNTY_RISK.filename.endswith(".json")

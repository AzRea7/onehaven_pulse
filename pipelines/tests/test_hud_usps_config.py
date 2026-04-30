from pipelines.extractors.hud_usps.config import (
    HUD_USPS_DATASETS,
    HUD_USPS_ZIP_CBSA,
    HUD_USPS_ZIP_COUNTY,
    HUD_USPS_ZIP_TRACT,
)


def test_hud_usps_datasets_configured():
    crosswalk_types = {dataset.crosswalk_type for dataset in HUD_USPS_DATASETS}

    assert "zip_tract" in crosswalk_types
    assert "zip_county" in crosswalk_types
    assert "zip_cbsa" in crosswalk_types


def test_hud_usps_type_mapping():
    assert HUD_USPS_ZIP_TRACT.api_type == 1
    assert HUD_USPS_ZIP_COUNTY.api_type == 2
    assert HUD_USPS_ZIP_CBSA.api_type == 3


def test_hud_usps_dataset_metadata():
    for dataset in HUD_USPS_DATASETS:
        assert dataset.dataset == "zip_crosswalk"
        assert dataset.expected_frequency == "quarterly"
        assert dataset.filename.endswith(".json")

import pytest

from app.domain.geography import (
    CANONICAL_GEO_TYPES,
    build_canonical_slug,
    build_geo_id,
    hierarchy_level_for_geo_type,
    slugify,
)


def test_canonical_geo_types_include_required_types():
    assert "national" in CANONICAL_GEO_TYPES
    assert "state" in CANONICAL_GEO_TYPES
    assert "county" in CANONICAL_GEO_TYPES
    assert "metro" in CANONICAL_GEO_TYPES
    assert "place" in CANONICAL_GEO_TYPES
    assert "zcta" in CANONICAL_GEO_TYPES
    assert "custom" in CANONICAL_GEO_TYPES


def test_build_geo_id_for_national():
    assert build_geo_id("national") == "us"


def test_build_geo_id_for_state():
    assert build_geo_id("state", state_code="MI") == "state_mi"


def test_build_geo_id_for_county():
    assert build_geo_id("county", county_fips="26163") == "county_26163"


def test_build_geo_id_for_metro():
    assert build_geo_id("metro", cbsa_code="19820") == "metro_19820"


def test_build_geo_id_for_place():
    assert build_geo_id("place", place_fips="2622000") == "place_2622000"


def test_build_geo_id_for_zcta():
    assert build_geo_id("zcta", zcta="48226") == "zcta_48226"


def test_build_geo_id_zero_pads_numeric_codes():
    assert build_geo_id("county", county_fips="163") == "county_00163"
    assert build_geo_id("zcta", zcta="9021") == "zcta_09021"


def test_build_geo_id_rejects_bad_state_code():
    with pytest.raises(ValueError):
        build_geo_id("state", state_code="MICH")


def test_slugify():
    assert slugify("Detroit-Warren-Dearborn, MI") == "detroit-warren-dearborn-mi"


def test_build_canonical_slug():
    assert (
        build_canonical_slug(
            geo_type="metro",
            name="Detroit-Warren-Dearborn, MI",
            geo_id="metro_19820",
        )
        == "detroit-warren-dearborn-mi-metro-19820"
    )


def test_hierarchy_levels():
    assert hierarchy_level_for_geo_type("national") == 0
    assert hierarchy_level_for_geo_type("state") == 1
    assert hierarchy_level_for_geo_type("metro") == 2
    assert hierarchy_level_for_geo_type("county") == 2
    assert hierarchy_level_for_geo_type("place") == 3
    assert hierarchy_level_for_geo_type("zcta") == 4

from app.domain.geography import (
    CANONICAL_RELATIONSHIP_TYPES,
    build_relationship_key,
    validate_relationship_type,
)


def test_canonical_relationship_types_include_required_types():
    assert "contains" in CANONICAL_RELATIONSHIP_TYPES
    assert "overlaps" in CANONICAL_RELATIONSHIP_TYPES
    assert "member_of" in CANONICAL_RELATIONSHIP_TYPES
    assert "primary_parent" in CANONICAL_RELATIONSHIP_TYPES
    assert "equivalent" in CANONICAL_RELATIONSHIP_TYPES
    assert "custom_contains" in CANONICAL_RELATIONSHIP_TYPES


def test_validate_relationship_type():
    assert validate_relationship_type("contains") == "contains"
    assert validate_relationship_type("CONTAINS") == "contains"


def test_validate_relationship_type_rejects_unknown_type():
    with pytest.raises(ValueError):
        validate_relationship_type("fake_type")


def test_build_relationship_key():
    assert build_relationship_key(
        parent_geo_id="us",
        child_geo_id="state_mi",
        relationship_type="contains",
    ) == ("us", "state_mi", "contains")


def test_build_relationship_key_rejects_self_relationship():
    with pytest.raises(ValueError):
        build_relationship_key(
            parent_geo_id="us",
            child_geo_id="us",
            relationship_type="contains",
        )

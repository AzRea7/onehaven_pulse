from __future__ import annotations

import re
from enum import StrEnum


class GeoType(StrEnum):
    NATIONAL = "national"
    STATE = "state"
    COUNTY = "county"
    METRO = "metro"
    PLACE = "place"
    ZCTA = "zcta"
    CUSTOM = "custom"


CANONICAL_GEO_TYPES: tuple[str, ...] = tuple(item.value for item in GeoType)


GEO_TYPE_HIERARCHY_LEVEL: dict[str, int] = {
    GeoType.NATIONAL.value: 0,
    GeoType.STATE.value: 1,
    GeoType.METRO.value: 2,
    GeoType.COUNTY.value: 2,
    GeoType.PLACE.value: 3,
    GeoType.ZCTA.value: 4,
    GeoType.CUSTOM.value: 9,
}


def normalize_state_code(value: str) -> str:
    state = value.strip().lower()

    if not re.fullmatch(r"[a-z]{2}", state):
        raise ValueError(f"Invalid state code: {value!r}")

    return state


def normalize_numeric_code(value: str | int, *, width: int, label: str) -> str:
    code = str(value).strip()

    if not re.fullmatch(r"\d+", code):
        raise ValueError(f"{label} must be numeric: {value!r}")

    padded = code.zfill(width)

    if len(padded) != width:
        raise ValueError(f"{label} must be {width} digits: {value!r}")

    return padded


def slugify(value: str) -> str:
    slug = value.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    slug = slug.strip("-")

    if not slug:
        raise ValueError("Cannot build slug from empty value")

    return slug


def build_geo_id(
    geo_type: str,
    *,
    state_code: str | None = None,
    county_fips: str | int | None = None,
    cbsa_code: str | int | None = None,
    place_fips: str | int | None = None,
    zcta: str | int | None = None,
    custom_slug: str | None = None,
) -> str:
    if geo_type == GeoType.NATIONAL.value:
        return "us"

    if geo_type == GeoType.STATE.value:
        if not state_code:
            raise ValueError("state_code is required for state geo_id")
        return f"state_{normalize_state_code(state_code)}"

    if geo_type == GeoType.COUNTY.value:
        if county_fips is None:
            raise ValueError("county_fips is required for county geo_id")
        return f"county_{normalize_numeric_code(county_fips, width=5, label='county_fips')}"

    if geo_type == GeoType.METRO.value:
        if cbsa_code is None:
            raise ValueError("cbsa_code is required for metro geo_id")
        return f"metro_{normalize_numeric_code(cbsa_code, width=5, label='cbsa_code')}"

    if geo_type == GeoType.PLACE.value:
        if place_fips is None:
            raise ValueError("place_fips is required for place geo_id")
        return f"place_{normalize_numeric_code(place_fips, width=7, label='place_fips')}"

    if geo_type == GeoType.ZCTA.value:
        if zcta is None:
            raise ValueError("zcta is required for zcta geo_id")
        return f"zcta_{normalize_numeric_code(zcta, width=5, label='zcta')}"

    if geo_type == GeoType.CUSTOM.value:
        if not custom_slug:
            raise ValueError("custom_slug is required for custom geo_id")
        return f"custom_{slugify(custom_slug)}"

    raise ValueError(f"Unsupported geo_type: {geo_type!r}")


def hierarchy_level_for_geo_type(geo_type: str) -> int:
    try:
        return GEO_TYPE_HIERARCHY_LEVEL[geo_type]
    except KeyError as exc:
        raise ValueError(f"Unsupported geo_type: {geo_type!r}") from exc


def build_canonical_slug(
    *,
    geo_type: str,
    name: str,
    state_code: str | None = None,
    geo_id: str | None = None,
) -> str:
    base = slugify(name)

    if geo_type in {GeoType.STATE.value, GeoType.NATIONAL.value}:
        return base

    if state_code:
        return f"{base}-{normalize_state_code(state_code)}"

    if geo_id:
        return f"{base}-{slugify(geo_id)}"

    return base

class GeoRelationshipType(StrEnum):
    CONTAINS = "contains"
    OVERLAPS = "overlaps"
    MEMBER_OF = "member_of"
    PRIMARY_PARENT = "primary_parent"
    EQUIVALENT = "equivalent"
    CUSTOM_CONTAINS = "custom_contains"


CANONICAL_RELATIONSHIP_TYPES: tuple[str, ...] = tuple(
    item.value for item in GeoRelationshipType
)


def validate_relationship_type(value: str) -> str:
    normalized = value.strip().lower()

    if normalized not in CANONICAL_RELATIONSHIP_TYPES:
        raise ValueError(f"Unsupported relationship_type: {value!r}")

    return normalized


def build_relationship_key(
    *,
    parent_geo_id: str,
    child_geo_id: str,
    relationship_type: str,
) -> tuple[str, str, str]:
    if not parent_geo_id.strip():
        raise ValueError("parent_geo_id is required")

    if not child_geo_id.strip():
        raise ValueError("child_geo_id is required")

    if parent_geo_id == child_geo_id:
        raise ValueError("parent_geo_id and child_geo_id cannot be the same")

    return (
        parent_geo_id.strip(),
        child_geo_id.strip(),
        validate_relationship_type(relationship_type),
    )

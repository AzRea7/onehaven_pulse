from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class GeographyIdentity(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    geo_id: str
    geo_type: str
    name: str
    display_name: str
    state_code: str | None = None
    state_name: str | None = None
    county_fips: str | None = None
    cbsa_code: str | None = None
    place_fips: str | None = None
    zcta: str | None = None
    country_code: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    hierarchy_level: int | None = None
    canonical_slug: str | None = None


class GeographyRelationshipItem(BaseModel):
    parent: GeographyIdentity
    child: GeographyIdentity
    relationship_type: str
    source: str
    confidence_score: Decimal
    overlap_ratio: Decimal | None = None
    is_active: bool


class GeographyChildrenResponse(BaseModel):
    geo_id: str
    relationship_type: str
    child_geo_type: str | None = None
    items: list[GeographyRelationshipItem]


class GeographyParentsResponse(BaseModel):
    geo_id: str
    relationship_type: str
    parent_geo_type: str | None = None
    items: list[GeographyRelationshipItem]


class GeographyRelatedResponse(BaseModel):
    geo_id: str
    relationship_type: str
    parent_geo_type: str | None = None
    child_geo_type: str | None = None
    parents: list[GeographyRelationshipItem]
    children: list[GeographyRelationshipItem]


class GeographySearchItem(BaseModel):
    geography: GeographyIdentity
    parent_count: int = 0
    child_count: int = 0


class GeographySearchResponse(BaseModel):
    q: str | None = None
    geo_type: str | None = None
    limit: int
    items: list[GeographySearchItem]


class GeographySearchItem(BaseModel):
    geography: GeographyIdentity
    parent_count: int = 0
    child_count: int = 0


class GeographySearchResponse(BaseModel):
    q: str | None = None
    geo_type: str | None = None
    limit: int
    items: list[GeographySearchItem]

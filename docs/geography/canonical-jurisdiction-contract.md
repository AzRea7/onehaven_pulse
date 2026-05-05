# OneHaven Canonical Jurisdiction Contract

## Purpose

OneHaven uses canonical geography records so every platform tool can reference the same jurisdiction IDs, hierarchy, display names, and source mappings.

This contract applies to the Market Engine and all future OneHaven tools.

## Canonical geo types

| geo_type | Meaning | Example geo_id |
|---|---|---|
| national | Country-level market | `us` |
| state | U.S. state | `state_mi` |
| county | U.S. county | `county_26163` |
| metro | Census CBSA / metro area | `metro_19820` |
| place | Census place, often city/town/village | `place_2622000` |
| zcta | ZIP Code Tabulation Area | `zcta_48226` |
| custom | Platform-defined custom jurisdiction | `custom_detroit_downtown` |

## Canonical ID rules

Canonical IDs must be deterministic and source-neutral.

Valid examples:

- `us`
- `state_mi`
- `county_26163`
- `metro_19820`
- `place_2622000`
- `zcta_48226`
- `custom_detroit_downtown`

Invalid examples:

- `zillow_394532`
- `redfin_detroit`
- `bls_LAUMT261982000000003`

Provider IDs belong in `geo.geo_crosswalk`, not `geo.dim_geo.geo_id`.

## Required fields

| Field | Required | Description |
|---|---:|---|
| geo_id | yes | Canonical OneHaven geography ID |
| geo_type | yes | One of the canonical geo types |
| name | yes | Source-neutral name |
| display_name | yes | User-facing name |
| state_code | conditional | Two-letter state code where applicable |
| state_name | conditional | Full state name where applicable |
| county_fips | conditional | 5-digit county FIPS |
| cbsa_code | conditional | 5-digit CBSA code |
| place_fips | conditional | 7-digit state+place FIPS |
| zcta | conditional | 5-digit ZCTA |
| country_code | yes | Country code, usually `US` |
| latitude | optional | Centroid latitude |
| longitude | optional | Centroid longitude |
| parent_geo_id | optional | Primary parent geography when one exists |
| hierarchy_level | yes | Hierarchy sort depth |
| canonical_slug | yes | Stable lowercase URL/search slug |
| is_active | yes | Whether record is product-active |
| created_at | yes | Creation timestamp |
| updated_at | yes | Update timestamp |

## Hierarchy levels

- 0 national
- 1 state
- 2 metro
- 2 county
- 3 place
- 4 zcta
- 9 custom

This is not a full relationship graph. It is only a simple level for sorting and display.

Full parent/child and overlap relationships belong in Story 7.2 through `geo.geo_relationship`.

## Provider crosswalk rule

Source-specific IDs must be mapped through `geo.geo_crosswalk`.

Examples:

- Zillow `394532` -> `metro_19820`
- BLS LAUS `LAUMT261982000000003` -> `metro_19820`
- Redfin `Detroit, MI` -> `metro_19820`

## Frontend routing rule

Frontend code must use canonical `geo_id`, not provider IDs.

Routes should use:

- `/markets/metro_19820`
- `/markets/state_mi`
- `/markets/county_26163`
- `/markets/place_2622000`
- `/markets/zcta_48226`

## Future work

Story 7.2 creates geography relationships.

Story 7.3 creates provider crosswalk seed loading.

Story 7.4 makes transforms use a shared crosswalk-first resolver.

Story 7.5 expands canonical jurisdictions for counties, places, and ZCTAs.


---

# Geography Relationships

## Purpose

`geo.dim_geo.parent_geo_id` is only a simple primary-parent hint.

Real jurisdiction hierarchy and overlap must be represented in `geo.geo_relationship`.

## Relationship table

`geo.geo_relationship` stores directed parent-child or overlap relationships between canonical OneHaven geographies.

## Relationship types

| relationship_type | Meaning |
|---|---|
| contains | Parent fully or administratively contains child |
| overlaps | Parent spatially overlaps child |
| member_of | Child is a member of parent grouping |
| primary_parent | Preferred display/navigation parent |
| equivalent | Two records represent the same logical geography in different systems |
| custom_contains | Custom geography contains child geography |

## Examples

| parent_geo_id | child_geo_id | relationship_type |
|---|---|---|
| us | state_mi | contains |
| state_mi | county_26163 | contains |
| us | metro_19820 | contains |
| state_mi | metro_19820 | member_of |
| metro_19820 | county_26163 | overlaps |
| county_26163 | place_2622000 | contains |
| place_2622000 | zcta_48226 | overlaps |

## Rules

- Do not model all hierarchy in `dim_geo.parent_geo_id`.
- Use `geo.geo_relationship` for many-to-many geography relationships.
- Relationships must use canonical `geo_id` values.
- Relationships may include source and confidence metadata.
- Do not create spatial relationships without source evidence.
- Do not fake place/ZCTA or metro/county overlap relationships without data.

## Current seed scope

Story 7.2 seeds only relationships that can be derived from current canonical fields:

- `us -> state_*`
- `us -> metro_*`
- `state_* -> county_*`
- `state_* -> metro_*` where `state_code` is available

Metro-to-county, county-to-place, place-to-ZCTA, and spatial overlap relationships are later stories.


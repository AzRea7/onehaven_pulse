from pathlib import Path

import geopandas as gpd
from shapely.geometry import MultiPolygon
from sqlalchemy import create_engine, text

from pipelines.common.settings import settings
from pipelines.extractors.census.config import CensusGeographyDataset


def _make_multipolygon(geometry):
    if geometry is None:
        return None

    if geometry.geom_type == "Polygon":
        return MultiPolygon([geometry])

    if geometry.geom_type == "MultiPolygon":
        return geometry

    return geometry


def _state_geo_id(statefp: str) -> str:
    return f"state_{statefp}"


def _metro_geo_id(cbsa_code: str) -> str:
    return f"metro_{cbsa_code}"


def _load_geodataframe_from_zip(raw_file_path: str | Path):
    path = Path(raw_file_path)
    return gpd.read_file(f"zip://{path}")


def _upsert_state_rows(connection, gdf, dataset: CensusGeographyDataset) -> int:
    count = 0

    for _, row in gdf.iterrows():
        statefp = str(row.get("STATEFP")).zfill(2)
        state_code = row.get("STUSPS")
        state_name = row.get("NAME")
        geo_id = _state_geo_id(statefp)

        connection.execute(
            text(
                """
                INSERT INTO geo.dim_geo (
                    geo_id,
                    geo_type,
                    name,
                    display_name,
                    state_code,
                    state_name,
                    country_code,
                    is_active,
                    updated_at
                )
                VALUES (
                    :geo_id,
                    'state',
                    :name,
                    :display_name,
                    :state_code,
                    :state_name,
                    'US',
                    TRUE,
                    NOW()
                )
                ON CONFLICT (geo_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    display_name = EXCLUDED.display_name,
                    state_code = EXCLUDED.state_code,
                    state_name = EXCLUDED.state_name,
                    is_active = TRUE,
                    updated_at = NOW()
                """
            ),
            {
                "geo_id": geo_id,
                "name": state_name,
                "display_name": state_name,
                "state_code": state_code,
                "state_name": state_name,
            },
        )

        geometry = _make_multipolygon(row.geometry)
        simplified = geometry.simplify(0.01, preserve_topology=True) if geometry else None

        connection.execute(
            text(
                """
                INSERT INTO geo.geo_geometry (
                    geo_id,
                    geo_type,
                    geometry_source,
                    geometry_year,
                    geometry,
                    simplified_geometry,
                    updated_at
                )
                VALUES (
                    :geo_id,
                    'state',
                    :geometry_source,
                    :geometry_year,
                    ST_Multi(ST_GeomFromText(:geometry_wkt, 4326)),
                    ST_Multi(ST_GeomFromText(:simplified_geometry_wkt, 4326)),
                    NOW()
                )
                ON CONFLICT (geo_id)
                DO UPDATE SET
                    geo_type = EXCLUDED.geo_type,
                    geometry_source = EXCLUDED.geometry_source,
                    geometry_year = EXCLUDED.geometry_year,
                    geometry = EXCLUDED.geometry,
                    simplified_geometry = EXCLUDED.simplified_geometry,
                    updated_at = NOW()
                """
            ),
            {
                "geo_id": geo_id,
                "geometry_source": dataset.geometry_source,
                "geometry_year": dataset.geometry_year,
                "geometry_wkt": geometry.wkt if geometry else None,
                "simplified_geometry_wkt": simplified.wkt if simplified else None,
            },
        )

        count += 1

    return count


def _upsert_cbsa_rows(connection, gdf, dataset: CensusGeographyDataset) -> int:
    count = 0

    for _, row in gdf.iterrows():
        cbsa_code = str(row.get("GEOID")).zfill(5)
        name = row.get("NAME")
        geo_id = _metro_geo_id(cbsa_code)

        connection.execute(
            text(
                """
                INSERT INTO geo.dim_geo (
                    geo_id,
                    geo_type,
                    name,
                    display_name,
                    cbsa_code,
                    country_code,
                    is_active,
                    updated_at
                )
                VALUES (
                    :geo_id,
                    'metro',
                    :name,
                    :display_name,
                    :cbsa_code,
                    'US',
                    TRUE,
                    NOW()
                )
                ON CONFLICT (geo_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    display_name = EXCLUDED.display_name,
                    cbsa_code = EXCLUDED.cbsa_code,
                    is_active = TRUE,
                    updated_at = NOW()
                """
            ),
            {
                "geo_id": geo_id,
                "name": name,
                "display_name": name,
                "cbsa_code": cbsa_code,
            },
        )

        geometry = _make_multipolygon(row.geometry)
        simplified = geometry.simplify(0.01, preserve_topology=True) if geometry else None

        connection.execute(
            text(
                """
                INSERT INTO geo.geo_geometry (
                    geo_id,
                    geo_type,
                    geometry_source,
                    geometry_year,
                    geometry,
                    simplified_geometry,
                    updated_at
                )
                VALUES (
                    :geo_id,
                    'metro',
                    :geometry_source,
                    :geometry_year,
                    ST_Multi(ST_GeomFromText(:geometry_wkt, 4326)),
                    ST_Multi(ST_GeomFromText(:simplified_geometry_wkt, 4326)),
                    NOW()
                )
                ON CONFLICT (geo_id)
                DO UPDATE SET
                    geo_type = EXCLUDED.geo_type,
                    geometry_source = EXCLUDED.geometry_source,
                    geometry_year = EXCLUDED.geometry_year,
                    geometry = EXCLUDED.geometry,
                    simplified_geometry = EXCLUDED.simplified_geometry,
                    updated_at = NOW()
                """
            ),
            {
                "geo_id": geo_id,
                "geometry_source": dataset.geometry_source,
                "geometry_year": dataset.geometry_year,
                "geometry_wkt": geometry.wkt if geometry else None,
                "simplified_geometry_wkt": simplified.wkt if simplified else None,
            },
        )

        count += 1

    return count


def load_geography_to_postgis(
    dataset: CensusGeographyDataset,
    raw_file_path: str | Path,
) -> int:
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    gdf = _load_geodataframe_from_zip(raw_file_path)

    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4269)

    gdf = gdf.to_crs(epsg=4326)
    gdf["geometry"] = gdf["geometry"].apply(_make_multipolygon)

    with engine.begin() as connection:
        if dataset.geo_type == "state":
            return _upsert_state_rows(connection, gdf, dataset)

        if dataset.geo_type == "metro":
            return _upsert_cbsa_rows(connection, gdf, dataset)

        raise ValueError(f"Unsupported Census geo_type={dataset.geo_type}")

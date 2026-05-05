from sqlalchemy import create_engine

from pipelines.common.settings import settings
from pipelines.common.geography.relationships import get_children, get_parents
from pipelines.seeds.geography.load_geo_relationships import load_geo_relationships


def test_get_children_for_detroit_place():
    engine = create_engine(settings.database_url)

    load_geo_relationships()

    with engine.begin() as connection:
        children = get_children(
            connection,
            parent_geo_id="place_2622000",
            child_geo_type="zcta",
        )

    child_ids = {item.child_geo_id for item in children}

    assert {"zcta_48201", "zcta_48202", "zcta_48226"}.issubset(child_ids)


def test_get_parents_for_detroit_zcta():
    engine = create_engine(settings.database_url)

    load_geo_relationships()

    with engine.begin() as connection:
        parents = get_parents(
            connection,
            child_geo_id="zcta_48201",
        )

    parent_ids = {item.parent_geo_id for item in parents}

    assert "place_2622000" in parent_ids
    assert "metro_19820" in parent_ids

from pathlib import Path
from decimal import Decimal

from pipelines.seeds.geography.load_geo_relationships import read_relationship_seed_file


def test_relationship_seed_contains_detroit_state_relationship():
    rows = read_relationship_seed_file(Path("data/seeds/geography/geo_relationships_seed.csv"))

    matches = [
        row
        for row in rows
        if row.parent_geo_id == "state_26"
        and row.child_geo_id == "place_2622000"
        and row.relationship_type == "contains"
    ]

    assert len(matches) == 1
    assert matches[0].confidence_score == Decimal("1.0000")


def test_relationship_seed_contains_zcta_relationships():
    rows = read_relationship_seed_file(Path("data/seeds/geography/geo_relationships_seed.csv"))

    zcta_children = {
        row.child_geo_id
        for row in rows
        if row.parent_geo_id == "place_2622000"
        and row.relationship_type == "contains"
    }

    assert {"zcta_48201", "zcta_48202", "zcta_48226"}.issubset(zcta_children)

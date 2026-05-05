from pathlib import Path

from pipelines.seeds.geography.load_jurisdiction_seeds import read_seed_file


def test_places_seed_contains_detroit():
    rows = read_seed_file(Path("data/seeds/geography/places_seed.csv"))

    matches = [
        row
        for row in rows
        if row.geo_id == "place_2622000"
        and row.geo_type == "place"
        and row.place_fips == "2622000"
    ]

    assert len(matches) == 1
    assert matches[0].display_name == "Detroit, MI"
    assert matches[0].canonical_slug == "detroit-mi"


def test_zcta_seed_contains_detroit_area_zcta():
    rows = read_seed_file(Path("data/seeds/geography/zctas_seed.csv"))

    matches = [
        row
        for row in rows
        if row.geo_id == "zcta_48201"
        and row.geo_type == "zcta"
        and row.zcta == "48201"
    ]

    assert len(matches) == 1
    assert matches[0].canonical_slug == "zcta-48201"

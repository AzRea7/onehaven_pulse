from pathlib import Path

from pipelines.seeds.geography.load_census_jurisdictions import (
    place_to_jurisdiction,
    zcta_to_jurisdiction,
)
from pipelines.sources.census.gazetteer_geographies import (
    read_census_places,
    read_census_zctas,
)


def test_read_census_places_fixture():
    records = read_census_places(Path("data/test/census/gazetteer/places_fixture.tsv"))

    assert len(records) == 1

    record = records[0]

    assert record.geo_id == "place_2622000"
    assert record.full_place_fips == "2622000"
    assert record.state_code == "MI"
    assert record.name == "Detroit"


def test_place_to_jurisdiction_fixture():
    record = read_census_places(Path("data/test/census/gazetteer/places_fixture.tsv"))[0]
    jurisdiction = place_to_jurisdiction(record)

    assert jurisdiction.geo_id == "place_2622000"
    assert jurisdiction.geo_type == "place"
    assert jurisdiction.display_name == "Detroit, MI"
    assert jurisdiction.place_fips == "2622000"
    assert jurisdiction.hierarchy_level == 3
    assert jurisdiction.canonical_slug == "detroit-mi"


def test_read_census_zctas_fixture():
    records = read_census_zctas(Path("data/test/census/gazetteer/zctas_fixture.tsv"))

    assert len(records) == 2
    assert records[0].geo_id == "zcta_48201"
    assert records[1].geo_id == "zcta_48202"


def test_zcta_to_jurisdiction_fixture():
    record = read_census_zctas(Path("data/test/census/gazetteer/zctas_fixture.tsv"))[0]
    jurisdiction = zcta_to_jurisdiction(record)

    assert jurisdiction.geo_id == "zcta_48201"
    assert jurisdiction.geo_type == "zcta"
    assert jurisdiction.display_name == "ZCTA 48201"
    assert jurisdiction.zcta == "48201"
    assert jurisdiction.hierarchy_level == 4
    assert jurisdiction.canonical_slug == "zcta-48201"

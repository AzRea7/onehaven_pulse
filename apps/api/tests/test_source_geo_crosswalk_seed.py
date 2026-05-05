from pathlib import Path

import pytest

from pipelines.seeds.geography.load_source_geo_crosswalk import read_seed_file


def test_read_source_geo_crosswalk_seed_file():
    rows = read_seed_file(Path("data/seeds/geography/source_geo_crosswalk_seed.csv"))

    assert len(rows) >= 5

    detroit_zillow = [
        row
        for row in rows
        if row.source == "zillow"
        and row.source_geo_id == "394532"
        and row.canonical_geo_id == "metro_19820"
    ]

    assert len(detroit_zillow) == 1
    assert detroit_zillow[0].match_method == "manual"
    assert detroit_zillow[0].confidence_score == 1


def test_seed_file_has_no_duplicate_source_to_canonical_rows():
    rows = read_seed_file(Path("data/seeds/geography/source_geo_crosswalk_seed.csv"))

    keys = [
        (row.source, row.source_geo_id, row.canonical_geo_id)
        for row in rows
    ]

    assert len(keys) == len(set(keys))


def test_seed_reader_rejects_missing_file():
    with pytest.raises(FileNotFoundError):
        read_seed_file(Path("data/seeds/geography/not_a_real_file.csv"))

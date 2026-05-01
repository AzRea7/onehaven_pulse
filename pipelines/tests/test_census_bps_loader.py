from datetime import date
from decimal import Decimal

from pipelines.extractors.census_building_permits.config import CENSUS_BPS_STATE_MONTHLY
from pipelines.loaders.census_bps_loader import parse_census_bps_content


def test_parse_census_bps_csv_content():
    content = (
        "state_fips,source_name,total_permits,single_family_permits,"
        "multi_family_permits,total_units\n"
        "26,Michigan,100,70,30,120\n"
    ).encode()

    rows = parse_census_bps_content(
        content=content,
        dataset=CENSUS_BPS_STATE_MONTHLY,
        source_file_id="source_file_1",
        load_date=date(2026, 5, 1),
    )

    assert len(rows) == 1
    assert rows[0]["geography_level"] == "state"
    assert rows[0]["period_type"] == "monthly"
    assert rows[0]["source_geo_id"] == "state:26"
    assert rows[0]["period_month"] == date(2026, 1, 1)
    assert rows[0]["building_permits"] == Decimal("100")
    assert rows[0]["single_family_permits"] == Decimal("70")
    assert rows[0]["multi_family_permits"] == Decimal("30")
    assert rows[0]["permit_units"] == Decimal("120")

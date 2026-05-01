from datetime import date
from decimal import Decimal

from pipelines.loaders.zillow_loader import parse_zillow_wide_csv


def test_parse_zillow_wide_csv_to_long_rows():
    content = (
        "RegionID,RegionName,RegionType,StateName,Metro,CountyName,2025-01-31,2025-02-28\\n"
        "102001,United States,country,,,,300000,301500\\n"
    ).encode()

    rows = parse_zillow_wide_csv(
        content=content,
        source_file_id="source_file_1",
        load_date=date(2026, 5, 1),
    )

    assert len(rows) == 2
    assert rows[0]["source_region_id"] == "102001"
    assert rows[0]["region_name"] == "United States"
    assert rows[0]["region_type"] == "country"
    assert rows[0]["period_month"] == date(2025, 1, 1)
    assert rows[0]["value"] == Decimal("300000")
    assert rows[1]["period_month"] == date(2025, 2, 1)
    assert rows[1]["value"] == Decimal("301500")

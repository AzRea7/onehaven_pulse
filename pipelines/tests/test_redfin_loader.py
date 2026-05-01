from datetime import date
from decimal import Decimal

from pipelines.loaders.redfin_loader import parse_redfin_market_tracker_csv


def test_parse_redfin_market_tracker_csv():
    content = (
        "period_begin,period_end,region_type,region,state_code,property_type,"
        "median_sale_price,homes_sold,pending_sales,new_listings,inventory,"
        "months_of_supply,median_dom,avg_sale_to_list,price_drops\n"
        '2026-01-01,2026-01-31,metro,"Detroit, MI",MI,All Residential,'
        "250000,100,80,120,500,5.0,35,0.98,0.12\n"
    ).encode()

    rows = parse_redfin_market_tracker_csv(
        content=content,
        source_file_id="source_file_1",
        load_date=date(2026, 5, 1),
    )

    assert len(rows) == 1
    assert rows[0]["region_name"] == "Detroit, MI"
    assert rows[0]["region_type"] == "metro"
    assert rows[0]["period_month"] == date(2026, 1, 1)
    assert rows[0]["median_sale_price"] == Decimal("250000")
    assert rows[0]["homes_sold"] == Decimal("100")
    assert rows[0]["pending_sales"] == Decimal("80")
    assert rows[0]["active_listings"] == Decimal("500")
    assert rows[0]["sale_to_list_ratio"] == Decimal("0.98")
    assert rows[0]["price_drops_pct"] == Decimal("12.00")


def test_parse_redfin_utf16_tsv_market_tracker_csv():
    content = (
        "Region\tMonth of Period End\tMedian Sale Price\tHomes Sold\tNew Listings\t"
        "Inventory\tDays on Market\tAverage Sale To List\n"
        "Boston, MA metro area\tJanuary 2012\t$303K\t2119\t3738\t18030\t133\t95.7%\n"
    ).encode("utf-16")

    rows = parse_redfin_market_tracker_csv(
        content=content,
        source_file_id="source_file_1",
        load_date=date(2026, 5, 1),
    )

    assert len(rows) == 1
    assert rows[0]["region_name"] == "Boston, MA"
    assert rows[0]["region_type"] == "metro"
    assert rows[0]["state_code"] == "MA"
    assert rows[0]["period_month"] == date(2012, 1, 1)
    assert rows[0]["median_sale_price"] == Decimal("303000")
    assert rows[0]["homes_sold"] == Decimal("2119")
    assert rows[0]["new_listings"] == Decimal("3738")
    assert rows[0]["active_listings"] == Decimal("18030")
    assert rows[0]["median_days_on_market"] == Decimal("133")
    assert rows[0]["sale_to_list_ratio"] == Decimal("0.957")

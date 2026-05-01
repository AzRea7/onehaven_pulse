from datetime import date
from decimal import Decimal

from pipelines.transforms.redfin.market_tracker_transform import (
    RawRedfinRecord,
    _parse_decimal,
    _quantize_metric,
    build_records,
)


def test_parse_decimal():
    assert _parse_decimal("123.45") == Decimal("123.45")
    assert _parse_decimal("") is None
    assert _parse_decimal(".") is None
    assert _parse_decimal(None) is None


def test_quantize_metric():
    assert _quantize_metric(Decimal("123.456"), "median_sale_price") == Decimal("123.46")
    assert _quantize_metric(Decimal("1.2345678"), "sale_to_list_ratio") == Decimal("1.234568")


def test_build_records_for_country():
    raw_records = [
        RawRedfinRecord(
            source_region_id="1",
            region_name="United States",
            region_type="country",
            state_code=None,
            property_type="All Residential",
            period_month=date(2026, 1, 1),
            median_sale_price=Decimal("250000"),
            homes_sold=Decimal("100"),
            pending_sales=Decimal("80"),
            new_listings=Decimal("120"),
            active_listings=Decimal("500"),
            months_supply=Decimal("5"),
            median_days_on_market=Decimal("35"),
            sale_to_list_ratio=Decimal("0.98"),
            price_drops_pct=Decimal("12"),
            source_file_id="source_file_1",
        )
    ]

    metric_records, unmatched = build_records(raw_records, "transform_test")
    metric_names = {record.metric_name for record in metric_records}

    assert unmatched == []
    assert "median_sale_price" in metric_names
    assert "homes_sold" in metric_names
    assert "pending_sales" in metric_names
    assert "active_listings" in metric_names
    assert "median_days_on_market" in metric_names
    assert "sale_to_list_ratio" in metric_names


def test_provisional_redfin_geo_id_shape():
    from datetime import date
    from decimal import Decimal

    from pipelines.transforms.redfin.market_tracker_transform import (
        RawRedfinRecord,
        _provisional_redfin_geo_id,
    )

    record = RawRedfinRecord(
        source_region_id=None,
        region_name="Boston, MA",
        region_type="metro",
        state_code="MA",
        property_type="All Residential",
        period_month=date(2026, 1, 1),
        median_sale_price=Decimal("500000"),
        homes_sold=None,
        pending_sales=None,
        new_listings=None,
        active_listings=None,
        months_supply=None,
        median_days_on_market=None,
        sale_to_list_ratio=None,
        price_drops_pct=None,
        source_file_id=None,
    )

    assert _provisional_redfin_geo_id(record) == "metro_redfin_boston_ma_ma"

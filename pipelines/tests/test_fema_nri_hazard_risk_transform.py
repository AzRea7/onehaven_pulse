from decimal import Decimal

from pipelines.transforms.fema_nri.hazard_risk_transform import (
    RawFemaNriCountyRiskRecord,
    _period_month_for_release,
    _quantize_money,
    _quantize_score,
    build_records,
)


def test_period_month_for_release():
    assert _period_month_for_release("December 2025").isoformat() == "2025-12-01"


def test_quantizers():
    assert _quantize_score(Decimal("12.1234567")) == Decimal("12.123457")
    assert _quantize_money(Decimal("123.456")) == Decimal("123.46")


def test_build_records_with_provisional_county():
    raw_records = [
        RawFemaNriCountyRiskRecord(
            county_fips="01001",
            county_name="Autauga",
            state_name="Alabama",
            state_code="AL",
            release_label="December 2025",
            risk_score=Decimal("50.1"),
            risk_rating="Relatively Moderate",
            expected_annual_loss=Decimal("123456.78"),
            expected_annual_loss_score=Decimal("44.2"),
            expected_annual_loss_rating="Relatively Moderate",
            social_vulnerability_score=Decimal("60.3"),
            social_vulnerability_rating="Relatively High",
            community_resilience_score=Decimal("70.4"),
            community_resilience_rating="Relatively High",
            source_file_id="source_file_1",
        )
    ]

    metric_records, unmatched = build_records(raw_records, "transform_test")
    metric_names = {record.metric_name for record in metric_records}

    assert unmatched == []
    assert "hazard_risk_score" in metric_names
    assert "expected_annual_loss" in metric_names
    assert "social_vulnerability_score" in metric_names
    assert "community_resilience_score" in metric_names

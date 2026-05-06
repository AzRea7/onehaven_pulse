from __future__ import annotations

from app.ai.compare_summary import _confidence_bucket, _coverage_false_categories, _format_value
from app.ai.schemas import CompareSummaryRequest


def test_compare_summary_request_requires_two_unique_markets() -> None:
    request = CompareSummaryRequest(
        geo_ids=["metro_19820", "metro_19820", "metro_16980"],
        metrics=["zhvi_yoy"],
    )

    assert request.geo_ids == ["metro_19820", "metro_16980"]


def test_confidence_bucket() -> None:
    assert _confidence_bucket(0.95) == "high confidence"
    assert _confidence_bucket(0.85) == "usable confidence"
    assert _confidence_bucket(0.65) == "limited confidence"
    assert _confidence_bucket(0.2) == "low confidence"
    assert _confidence_bucket(None) == "unknown confidence"


def test_coverage_false_categories() -> None:
    payload = {
        "coverage": {
            "price": True,
            "rent": True,
            "inventory": False,
            "affordability": True,
            "labor": True,
            "permits": False,
        }
    }

    assert _coverage_false_categories(payload) == ["inventory", "permits"]


def test_format_value() -> None:
    assert _format_value(None) == "missing"
    assert _format_value(0.12345) == "0.123"
    assert _format_value(12.345) == "12.35"

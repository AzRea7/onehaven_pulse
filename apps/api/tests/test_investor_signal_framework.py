from __future__ import annotations

from app.services.investor_signal import build_investor_signal_from_payloads


def _context(
    *,
    confidence_score: float | None = 0.90,
    zhvi_yoy: float | None = 0.05,
    zori_yoy: float | None = 0.04,
    payment_to_income_ratio: float | None = 0.27,
    unemployment_rate: float | None = 4.2,
    active_listings_yoy: float | None = 0.02,
    median_days_on_market: float | None = 40,
    permits_per_1000_people: float | None = 2.5,
) -> dict:
    return {
        "confidence_score": confidence_score,
        "latest_period": "2026-03-01",
        "latest_data_period": "2026-03-01",
        "latest_scoreable_period": "2026-03-01",
        "evidence": {
            "price_growth_yoy": zhvi_yoy,
            "rent_growth_yoy": zori_yoy,
            "payment_to_income_ratio": payment_to_income_ratio,
            "unemployment_rate": unemployment_rate,
            "active_listings_yoy": active_listings_yoy,
            "median_days_on_market": median_days_on_market,
            "permits_per_1000_people": permits_per_1000_people,
        },
    }


def _coverage(
    *,
    price: bool = True,
    rent: bool = True,
    affordability: bool = True,
    labor: bool = True,
    inventory: bool = True,
    permits: bool = True,
    missing_score_inputs: list[str] | None = None,
) -> dict:
    return {
        "latest_data_period": "2026-03-01",
        "latest_scoreable_period": "2026-03-01",
        "coverage": {
            "price": price,
            "rent": rent,
            "inventory": inventory,
            "affordability": affordability,
            "labor": labor,
            "permits": permits,
        },
        "available_metrics": [
            "zhvi_yoy",
            "zori_yoy",
            "payment_to_income_ratio",
            "unemployment_rate",
        ],
        "missing_score_inputs": missing_score_inputs or [],
    }


def test_attractive_market_rule_requires_strong_confidence_and_no_core_negatives() -> None:
    signal = build_investor_signal_from_payloads(
        "metro_test",
        _context(confidence_score=0.90, zhvi_yoy=0.06, zori_yoy=0.04),
        _coverage(),
    )

    assert signal.stance == "attractive"
    assert signal.rule_version == "investor_signal_v2"
    assert signal.stance_score is not None
    assert signal.material_missing_score_inputs is False
    assert signal.deterministic is True


def test_detroit_like_promising_but_imperfect_market_is_watchlist_not_attractive() -> None:
    signal = build_investor_signal_from_payloads(
        "metro_test",
        _context(
            confidence_score=0.80,
            zhvi_yoy=3.027356,
            zori_yoy=2.326381,
            payment_to_income_ratio=0.201103,
            unemployment_rate=5.3,
            active_listings_yoy=None,
            median_days_on_market=37.0,
            permits_per_1000_people=None,
        ),
        _coverage(
            permits=False,
            missing_score_inputs=[
                "active_listings_yoy",
                "home_price_index_yoy",
                "median_rent_yoy",
                "median_sale_price_yoy",
                "months_supply",
            ],
        ),
    )

    assert signal.stance == "watchlist"
    assert signal.material_missing_score_inputs is True
    assert signal.dimension_statuses["price_momentum"] == "negative"
    assert signal.dimension_statuses["rent_momentum"] == "positive"
    assert signal.dimension_statuses["affordability"] == "positive"
    assert signal.dimension_statuses["labor_stability"] == "neutral"


def test_insufficient_data_when_required_coverage_missing() -> None:
    signal = build_investor_signal_from_payloads(
        "metro_test",
        _context(),
        _coverage(rent=False),
    )

    assert signal.stance == "insufficient_data"
    assert signal.required_coverage_present is False


def test_avoid_when_confidence_is_low() -> None:
    signal = build_investor_signal_from_payloads(
        "metro_test",
        _context(confidence_score=0.40),
        _coverage(),
    )

    assert signal.stance == "avoid"


def test_avoid_when_affordability_and_labor_are_negative() -> None:
    signal = build_investor_signal_from_payloads(
        "metro_test",
        _context(payment_to_income_ratio=0.45, unemployment_rate=8.0),
        _coverage(),
    )

    assert signal.stance == "avoid"


def test_missing_confidence_forces_insufficient_data() -> None:
    signal = build_investor_signal_from_payloads(
        "metro_test",
        _context(confidence_score=None),
        _coverage(),
    )

    assert signal.stance == "insufficient_data"


def test_response_contains_drivers_risks_and_dimensions() -> None:
    signal = build_investor_signal_from_payloads(
        "metro_test",
        _context(),
        _coverage(missing_score_inputs=["months_supply"]),
    )

    assert signal.dimension_statuses
    assert signal.drivers
    assert signal.risks
    assert signal.missing_score_inputs == ["months_supply"]
    assert signal.stance_reason

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.schemas.investor_signal import (
    DimensionStatus,
    InvestorMarketSignal,
    InvestorSignalDriver,
    InvestorSignalEvidence,
    InvestorSignalRisk,
)


STANCE_LABELS = {
    "attractive": "Attractive",
    "watchlist": "Watchlist",
    "mixed": "Mixed",
    "avoid": "Avoid",
    "insufficient_data": "Insufficient Data",
}


REQUIRED_COVERAGE = [
    "price",
    "rent",
    "affordability",
    "labor",
]


RULE_VERSION = "investor_signal_v2"


MATERIAL_MISSING_SCORE_INPUTS = {
    "active_listings_yoy",
    "months_supply",
    "median_days_on_market",
    "zhvi_yoy",
    "zori_yoy",
    "payment_to_income_ratio",
    "unemployment_rate",
}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


METRIC_ALIASES = {
    "zhvi_yoy": ["zhvi_yoy", "price_growth_yoy"],
    "zori_yoy": ["zori_yoy", "rent_growth_yoy"],
    "payment_to_income_ratio": ["payment_to_income_ratio"],
    "unemployment_rate": ["unemployment_rate"],
    "active_listings_yoy": ["active_listings_yoy"],
    "median_days_on_market": ["median_days_on_market"],
    "permits_per_1000_people": ["permits_per_1000_people"],
    "confidence_score": ["confidence_score"],
}


def _metric_keys(metric_name: str) -> list[str]:
    return METRIC_ALIASES.get(metric_name, [metric_name])


def _latest_metric(context_payload: dict[str, Any], metric_name: str) -> Any:
    keys = _metric_keys(metric_name)

    for key in keys:
        if key in context_payload:
            return context_payload.get(key)

    market = context_payload.get("market")
    if isinstance(market, dict):
        for key in keys:
            if key in market:
                return market.get(key)

    latest = context_payload.get("latest")
    if isinstance(latest, dict):
        for key in keys:
            if key in latest:
                return latest.get(key)

    metrics = context_payload.get("metrics")
    if isinstance(metrics, dict):
        for key in keys:
            if key in metrics:
                return metrics.get(key)

    evidence = context_payload.get("evidence")

    if isinstance(evidence, dict):
        for key in keys:
            if key in evidence:
                return evidence.get(key)

    if isinstance(evidence, list):
        for item in evidence:
            if not isinstance(item, dict):
                continue
            if item.get("metric_name") in keys:
                return item.get("value")

    return None


def _metric_period(context_payload: dict[str, Any], metric_name: str) -> str | None:
    keys = _metric_keys(metric_name)

    evidence = context_payload.get("evidence")
    if isinstance(evidence, list):
        for item in evidence:
            if not isinstance(item, dict):
                continue
            if item.get("metric_name") in keys:
                period = item.get("period_month") or item.get("period") or item.get("latest_period")
                return str(period) if period is not None else None

    period = (
        context_payload.get("latest_period")
        or context_payload.get("latest_scoreable_period")
        or context_payload.get("latest_data_period")
    )
    return str(period) if period is not None else None


def _evidence(
    context_payload: dict[str, Any],
    metric_name: str,
    interpretation: str,
) -> InvestorSignalEvidence:
    return InvestorSignalEvidence(
        metric_name=metric_name,
        value=_latest_metric(context_payload, metric_name),
        period=_metric_period(context_payload, metric_name),
        interpretation=interpretation,
    )


def _coverage_dict(coverage_payload: dict[str, Any]) -> dict[str, bool]:
    coverage = coverage_payload.get("coverage")
    if isinstance(coverage, dict):
        return {
            str(key): bool(value)
            for key, value in coverage.items()
        }
    return {}


def _required_coverage_present(coverage: dict[str, bool]) -> bool:
    return all(coverage.get(category) is True for category in REQUIRED_COVERAGE)


def _confidence_status(confidence_score: float | None) -> DimensionStatus:
    if confidence_score is None:
        return "missing"
    if confidence_score >= 0.85:
        return "positive"
    if confidence_score >= 0.70:
        return "neutral"
    return "negative"


def _price_status(value: float | None) -> DimensionStatus:
    if value is None:
        return "missing"
    if 0.02 <= value <= 0.12:
        return "positive"
    if -0.02 <= value < 0.02 or 0.12 < value <= 0.18:
        return "neutral"
    return "negative"


def _rent_status(value: float | None) -> DimensionStatus:
    if value is None:
        return "missing"
    if value >= 0.03:
        return "positive"
    if 0.00 <= value < 0.03:
        return "neutral"
    return "negative"


def _affordability_status(payment_to_income: float | None) -> DimensionStatus:
    if payment_to_income is None:
        return "missing"
    if payment_to_income <= 0.28:
        return "positive"
    if payment_to_income <= 0.36:
        return "neutral"
    return "negative"


def _labor_status(unemployment_rate: float | None) -> DimensionStatus:
    if unemployment_rate is None:
        return "missing"
    if unemployment_rate <= 4.5:
        return "positive"
    if unemployment_rate <= 6.5:
        return "neutral"
    return "negative"


def _inventory_status(active_listings_yoy: float | None, median_days_on_market: float | None) -> DimensionStatus:
    if active_listings_yoy is None and median_days_on_market is None:
        return "missing"

    negative = False
    positive = False

    if active_listings_yoy is not None:
        if active_listings_yoy > 0.25:
            negative = True
        elif active_listings_yoy < 0.05:
            positive = True

    if median_days_on_market is not None:
        if median_days_on_market > 75:
            negative = True
        elif median_days_on_market <= 45:
            positive = True

    if negative:
        return "negative"
    if positive:
        return "positive"
    return "neutral"


def _supply_status(permits_per_1000_people: float | None) -> DimensionStatus:
    if permits_per_1000_people is None:
        return "missing"
    if permits_per_1000_people <= 2.0:
        return "positive"
    if permits_per_1000_people <= 6.0:
        return "neutral"
    return "negative"


def _coverage_status(required_present: bool, missing_score_inputs: list[str]) -> DimensionStatus:
    if not required_present:
        return "negative"
    if missing_score_inputs:
        return "neutral"
    return "positive"


def _driver(
    name: str,
    status: DimensionStatus,
    message: str,
    evidence: list[InvestorSignalEvidence],
) -> InvestorSignalDriver:
    return InvestorSignalDriver(
        name=name,
        status=status,
        message=message,
        evidence=evidence,
    )


def _risk(
    name: str,
    severity: str,
    message: str,
    evidence: list[InvestorSignalEvidence] | None = None,
) -> InvestorSignalRisk:
    return InvestorSignalRisk(
        name=name,
        severity=severity,  # type: ignore[arg-type]
        message=message,
        evidence=evidence or [],
    )


def _build_drivers_and_risks(
    context_payload: dict[str, Any],
    coverage_payload: dict[str, Any],
    statuses: dict[str, DimensionStatus],
) -> tuple[list[InvestorSignalDriver], list[InvestorSignalRisk]]:
    drivers: list[InvestorSignalDriver] = []
    risks: list[InvestorSignalRisk] = []

    status_messages = {
        "positive": "positive signal",
        "neutral": "neutral signal",
        "negative": "negative signal",
        "missing": "missing signal",
    }

    metric_map = {
        "price_momentum": "zhvi_yoy",
        "rent_momentum": "zori_yoy",
        "affordability": "payment_to_income_ratio",
        "labor_stability": "unemployment_rate",
        "inventory_pressure": (
            "median_days_on_market"
            if _latest_metric(context_payload, "active_listings_yoy") is None
            else "active_listings_yoy"
        ),
        "supply_pressure": "permits_per_1000_people",
        "confidence": "confidence_score",
    }

    for dimension, status in statuses.items():
        if dimension == "coverage_quality":
            continue

        metric_name = metric_map.get(dimension)
        evidence = []
        if metric_name:
            evidence.append(
                _evidence(
                    context_payload,
                    metric_name,
                    f"{dimension} is a {status_messages[status]}.",
                )
            )

        if status == "positive":
            drivers.append(
                _driver(
                    dimension,
                    status,
                    f"{dimension.replace('_', ' ').title()} is supportive.",
                    evidence,
                )
            )
        elif status == "negative":
            risks.append(
                _risk(
                    dimension,
                    "high" if dimension in {"affordability", "labor_stability", "confidence"} else "medium",
                    f"{dimension.replace('_', ' ').title()} is unfavorable.",
                    evidence,
                )
            )
        elif status == "missing":
            risks.append(
                _risk(
                    f"missing_{dimension}",
                    "medium",
                    f"{dimension.replace('_', ' ').title()} is missing or unavailable.",
                    evidence,
                )
            )

    missing_score_inputs = coverage_payload.get("missing_score_inputs") or []
    if missing_score_inputs:
        risks.append(
            _risk(
                "missing_score_inputs",
                "medium",
                "Some score inputs are missing: " + ", ".join(str(item) for item in missing_score_inputs),
            )
        )

    return drivers, risks


def _material_missing_score_inputs(missing_score_inputs: list[str]) -> bool:
    return any(str(item) in MATERIAL_MISSING_SCORE_INPUTS for item in missing_score_inputs)


def _dimension_points(status: DimensionStatus) -> float:
    if status == "positive":
        return 1.0
    if status == "neutral":
        return 0.5
    if status == "negative":
        return -1.0
    return 0.0


def _stance_score(statuses: dict[str, DimensionStatus]) -> float:
    weights = {
        "price_momentum": 1.0,
        "rent_momentum": 1.25,
        "affordability": 1.5,
        "labor_stability": 1.0,
        "inventory_pressure": 0.75,
        "supply_pressure": 0.5,
        "confidence": 1.0,
        "coverage_quality": 1.0,
    }

    max_score = sum(weights.values())
    raw_score = sum(
        weights[name] * _dimension_points(status)
        for name, status in statuses.items()
        if name in weights
    )

    normalized = (raw_score + max_score) / (2 * max_score)
    return round(max(0.0, min(1.0, normalized)), 4)


def _stance_reason(stance: str, rule_trace: list[str]) -> str:
    if rule_trace:
        return rule_trace[-1]

    return f"Market assigned {stance} by deterministic investor signal rules."



def build_investor_signal_from_payloads(
    geo_id: str,
    context_payload: dict[str, Any],
    coverage_payload: dict[str, Any],
) -> InvestorMarketSignal:
    coverage = _coverage_dict(coverage_payload)
    missing_score_inputs = coverage_payload.get("missing_score_inputs") or []
    available_metrics = coverage_payload.get("available_metrics") or []

    required_present = _required_coverage_present(coverage)

    confidence_score = _safe_float(context_payload.get("confidence_score"))

    zhvi_yoy = _safe_float(_latest_metric(context_payload, "zhvi_yoy"))
    zori_yoy = _safe_float(_latest_metric(context_payload, "zori_yoy"))
    payment_to_income_ratio = _safe_float(_latest_metric(context_payload, "payment_to_income_ratio"))
    unemployment_rate = _safe_float(_latest_metric(context_payload, "unemployment_rate"))
    active_listings_yoy = _safe_float(_latest_metric(context_payload, "active_listings_yoy"))
    median_days_on_market = _safe_float(_latest_metric(context_payload, "median_days_on_market"))
    permits_per_1000_people = _safe_float(_latest_metric(context_payload, "permits_per_1000_people"))

    statuses: dict[str, DimensionStatus] = {
        "price_momentum": _price_status(zhvi_yoy),
        "rent_momentum": _rent_status(zori_yoy),
        "affordability": _affordability_status(payment_to_income_ratio),
        "labor_stability": _labor_status(unemployment_rate),
        "inventory_pressure": _inventory_status(active_listings_yoy, median_days_on_market),
        "supply_pressure": _supply_status(permits_per_1000_people),
        "confidence": _confidence_status(confidence_score),
        "coverage_quality": _coverage_status(required_present, list(missing_score_inputs)),
    }

    core_dimensions = [
        "price_momentum",
        "rent_momentum",
        "affordability",
        "labor_stability",
        "confidence",
        "coverage_quality",
    ]

    core_negative_count = sum(1 for name in core_dimensions if statuses[name] == "negative")
    core_positive_count = sum(1 for name in core_dimensions if statuses[name] == "positive")
    core_missing_count = sum(1 for name in core_dimensions if statuses[name] == "missing")

    rule_trace: list[str] = []

    latest_data_period = coverage_payload.get("latest_data_period") or context_payload.get("latest_data_period")
    latest_scoreable_period = coverage_payload.get("latest_scoreable_period") or context_payload.get("latest_scoreable_period")

    material_missing = _material_missing_score_inputs(list(missing_score_inputs))
    secondary_missing_count = sum(
        1
        for name in ["inventory_pressure", "supply_pressure"]
        if statuses[name] == "missing"
    )

    if not required_present:
        stance = "insufficient_data"
        rule_trace.append("Required coverage missing: price, rent, affordability, or labor.")
    elif confidence_score is None:
        stance = "insufficient_data"
        rule_trace.append("Confidence score is missing.")
    elif latest_scoreable_period is None:
        stance = "insufficient_data"
        rule_trace.append("Latest scoreable period is missing.")
    elif core_missing_count >= 2:
        stance = "insufficient_data"
        rule_trace.append("Two or more core dimensions are missing.")
    elif confidence_score < 0.60:
        stance = "avoid"
        rule_trace.append("Confidence score below 0.60.")
    elif core_negative_count >= 3:
        stance = "avoid"
        rule_trace.append("Three or more core dimensions are negative.")
    elif statuses["affordability"] == "negative" and statuses["labor_stability"] == "negative":
        stance = "avoid"
        rule_trace.append("Affordability and labor stability are both negative.")
    elif (
        statuses["rent_momentum"] == "positive"
        and statuses["affordability"] in {"positive", "neutral"}
        and statuses["labor_stability"] in {"positive", "neutral"}
        and statuses["price_momentum"] in {"positive", "neutral"}
        and statuses["confidence"] == "positive"
        and statuses["coverage_quality"] in {"positive", "neutral"}
        and core_negative_count == 0
        and core_missing_count == 0
        and not material_missing
        and secondary_missing_count <= 1
    ):
        stance = "attractive"
        rule_trace.append("High-conviction market: positive rent momentum, acceptable affordability/labor/price, strong confidence, and no material missing inputs.")
    elif (
        core_positive_count >= 2
        and core_negative_count <= 2
        and statuses["confidence"] in {"positive", "neutral"}
    ):
        stance = "watchlist"
        rule_trace.append("Promising market, but negative/neutral dimensions or missing inputs prevent Attractive.")
    else:
        stance = "mixed"
        rule_trace.append("Signals are available but mixed or strategy-dependent.")

    drivers, risks = _build_drivers_and_risks(context_payload, coverage_payload, statuses)

    return InvestorMarketSignal(
        geo_id=geo_id,
        stance=stance,  # type: ignore[arg-type]
        stance_label=STANCE_LABELS[stance],
        stance_score=_stance_score(statuses),
        stance_reason=_stance_reason(stance, rule_trace),
        rule_version=RULE_VERSION,
        confidence_score=confidence_score,
        latest_data_period=str(latest_data_period) if latest_data_period is not None else None,
        latest_scoreable_period=str(latest_scoreable_period) if latest_scoreable_period is not None else None,
        required_coverage_present=required_present,
        material_missing_score_inputs=material_missing,
        coverage=coverage,
        available_metrics=[str(item) for item in available_metrics],
        missing_score_inputs=[str(item) for item in missing_score_inputs],
        dimension_statuses=statuses,
        drivers=drivers,
        risks=risks,
        rule_trace=rule_trace,
        deterministic=True,
    )


def _call_service_with_supported_signature(function: Any, db: Session, geo_id: str) -> Any:
    """Call existing service functions without assuming old/new signature shape.

    Existing services in this codebase are not fully uniform yet. Some accept
    only geo_id, while others may accept db plus geo_id. This helper keeps the
    investor signal framework compatible with the current service layer.
    """

    attempts = (
        lambda: function(db, geo_id),
        lambda: function(geo_id),
        lambda: function(geo_id=geo_id),
        lambda: function(db=db, geo_id=geo_id),
    )

    last_error: TypeError | None = None

    for attempt in attempts:
        try:
            return attempt()
        except TypeError as error:
            last_error = error

    if last_error is not None:
        raise last_error

    raise RuntimeError(f"Could not call service function for geo_id={geo_id}")


def _payload_to_dict(payload: Any) -> dict[str, Any]:
    if hasattr(payload, "model_dump"):
        return payload.model_dump()

    if isinstance(payload, dict):
        return payload

    if hasattr(payload, "dict"):
        return payload.dict()

    return dict(payload)


def get_investor_signal(db: Session, geo_id: str) -> InvestorMarketSignal:
    # Imported inside the function to avoid import-cycle risk.
    from app.services.context import get_market_context
    from app.services.coverage import get_market_coverage

    context_payload = _call_service_with_supported_signature(get_market_context, db, geo_id)
    coverage_payload = _call_service_with_supported_signature(get_market_coverage, db, geo_id)

    context_dict = _payload_to_dict(context_payload)
    coverage_dict = _payload_to_dict(coverage_payload)

    return build_investor_signal_from_payloads(
        geo_id=geo_id,
        context_payload=context_dict,
        coverage_payload=coverage_dict,
    )

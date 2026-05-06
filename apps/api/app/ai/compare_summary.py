from __future__ import annotations

from typing import Any

from app.ai.schemas import CompareSummaryRequest, CompareSummaryResponse, EvidenceCitation
from app.mcp.tools import call_tool


DEFAULT_COMPARE_METRICS = [
    "zhvi_yoy",
    "zori_yoy",
    "payment_to_income_ratio",
    "unemployment_rate",
]


def _market_name(context_payload: dict[str, Any], geo_id: str) -> str:
    market = context_payload.get("market")

    if isinstance(market, dict):
        return str(market.get("display_name") or market.get("name") or market.get("geo_id") or geo_id)

    if isinstance(market, str) and market.strip():
        return market

    return geo_id


def _safe_number(value: Any) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_value(value: Any) -> str:
    number = _safe_number(value)

    if number is None:
        return "missing"

    if abs(number) < 1:
        return f"{number:.3f}"

    return f"{number:.2f}"


def _latest_by_geo(compare_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    latest_rows = compare_payload.get("latest") or []
    by_geo: dict[str, dict[str, Any]] = {}

    for row in latest_rows:
        if not isinstance(row, dict):
            continue

        geo_id = row.get("geo_id")
        if isinstance(geo_id, str):
            by_geo[geo_id] = row

    return by_geo


def _extract_metric(row: dict[str, Any], metric: str) -> Any:
    if metric in row:
        return row.get(metric)

    values = row.get("values")
    if isinstance(values, dict):
        return values.get(metric)

    metrics = row.get("metrics")
    if isinstance(metrics, dict):
        return metrics.get(metric)

    return None


def _rank_metric(
    latest_by_geo: dict[str, dict[str, Any]],
    geo_ids: list[str],
    metric: str,
    *,
    higher_is_better: bool,
) -> tuple[str | None, Any]:
    scored: list[tuple[str, float, Any]] = []

    for geo_id in geo_ids:
        row = latest_by_geo.get(geo_id, {})
        raw_value = _extract_metric(row, metric)
        number = _safe_number(raw_value)
        if number is None:
            continue
        scored.append((geo_id, number, raw_value))

    if not scored:
        return None, None

    scored.sort(key=lambda item: item[1], reverse=higher_is_better)
    return scored[0][0], scored[0][2]


def _coverage_false_categories(coverage_payload: dict[str, Any]) -> list[str]:
    coverage = coverage_payload.get("coverage") or {}

    if not isinstance(coverage, dict):
        return []

    return [
        category
        for category, available in coverage.items()
        if available is False
    ]


def _confidence_bucket(value: Any) -> str:
    confidence = _safe_number(value)

    if confidence is None:
        return "unknown confidence"

    if confidence >= 0.9:
        return "high confidence"

    if confidence >= 0.8:
        return "usable confidence"

    if confidence >= 0.6:
        return "limited confidence"

    return "low confidence"


def _build_citations(
    contexts: dict[str, dict[str, Any]],
    coverages: dict[str, dict[str, Any]],
    compare_payload: dict[str, Any],
) -> list[EvidenceCitation]:
    citations: list[EvidenceCitation] = []

    for geo_id, context in contexts.items():
        citations.append(
            EvidenceCitation(
                label=f"{geo_id} confidence_score",
                tool_name="get_market_context",
                field_path=f"contexts.{geo_id}.confidence_score",
                value=context.get("confidence_score"),
            )
        )

        citations.append(
            EvidenceCitation(
                label=f"{geo_id} evidence",
                tool_name="get_market_context",
                field_path=f"contexts.{geo_id}.evidence",
                value=context.get("evidence"),
            )
        )

    for geo_id, coverage in coverages.items():
        citations.append(
            EvidenceCitation(
                label=f"{geo_id} coverage",
                tool_name="get_geo_coverage",
                field_path=f"coverages.{geo_id}.coverage",
                value=coverage.get("coverage"),
            )
        )

        citations.append(
            EvidenceCitation(
                label=f"{geo_id} missing_score_inputs",
                tool_name="get_geo_coverage",
                field_path=f"coverages.{geo_id}.missing_score_inputs",
                value=coverage.get("missing_score_inputs"),
            )
        )

    citations.append(
        EvidenceCitation(
            label="compare latest rows",
            tool_name="compare_markets",
            field_path="compare.latest",
            value=compare_payload.get("latest"),
        )
    )

    return citations


def build_compare_summary(request: CompareSummaryRequest) -> CompareSummaryResponse:
    metrics = request.metrics or DEFAULT_COMPARE_METRICS

    compare_result = call_tool(
        "compare_markets",
        {
            "geo_ids": request.geo_ids,
            "metrics": metrics,
            "start_date": request.start_date,
            "end_date": request.end_date,
        },
    )

    if not compare_result["ok"]:
        raise RuntimeError(f"compare_markets failed: {compare_result['error']}")

    compare_payload = compare_result["result"]

    contexts: dict[str, dict[str, Any]] = {}
    coverages: dict[str, dict[str, Any]] = {}

    for geo_id in request.geo_ids:
        context_result = call_tool("get_market_context", {"geo_id": geo_id})
        if not context_result["ok"]:
            raise RuntimeError(f"get_market_context failed for {geo_id}: {context_result['error']}")
        contexts[geo_id] = context_result["result"]

        coverage_result = call_tool("get_geo_coverage", {"geo_id": geo_id})
        if not coverage_result["ok"]:
            raise RuntimeError(f"get_geo_coverage failed for {geo_id}: {coverage_result['error']}")
        coverages[geo_id] = coverage_result["result"]

    latest_by_geo = _latest_by_geo(compare_payload)

    market_names = {
        geo_id: _market_name(contexts[geo_id], geo_id)
        for geo_id in request.geo_ids
    }

    best_price_geo, best_price_value = _rank_metric(
        latest_by_geo,
        request.geo_ids,
        "zhvi_yoy",
        higher_is_better=True,
    )
    best_rent_geo, best_rent_value = _rank_metric(
        latest_by_geo,
        request.geo_ids,
        "zori_yoy",
        higher_is_better=True,
    )
    best_affordability_geo, best_affordability_value = _rank_metric(
        latest_by_geo,
        request.geo_ids,
        "payment_to_income_ratio",
        higher_is_better=False,
    )
    best_labor_geo, best_labor_value = _rank_metric(
        latest_by_geo,
        request.geo_ids,
        "unemployment_rate",
        higher_is_better=False,
    )

    confidence_lines: list[str] = []
    missing_lines: list[str] = []

    for geo_id in request.geo_ids:
        context = contexts[geo_id]
        coverage = coverages[geo_id]

        confidence = context.get("confidence_score")
        confidence_lines.append(
            f"{market_names[geo_id]} has {_confidence_bucket(confidence)} "
            f"with confidence_score={_format_value(confidence)}."
        )

        false_categories = _coverage_false_categories(coverage)
        missing_inputs = coverage.get("missing_score_inputs") or []

        if false_categories:
            missing_lines.append(
                f"{market_names[geo_id]} has incomplete coverage for "
                f"{', '.join(false_categories)}. Missing score inputs include: "
                f"{', '.join(str(item) for item in missing_inputs) if missing_inputs else 'not specified'}."
            )
        else:
            missing_lines.append(f"{market_names[geo_id]} has complete score-category coverage.")

    takeaways: list[str] = []

    if best_price_geo:
        takeaways.append(
            f"Strongest latest price-growth signal: {market_names[best_price_geo]} "
            f"with zhvi_yoy={_format_value(best_price_value)}."
        )

    if best_rent_geo:
        takeaways.append(
            f"Strongest latest rent-growth signal: {market_names[best_rent_geo]} "
            f"with zori_yoy={_format_value(best_rent_value)}."
        )

    if best_affordability_geo:
        takeaways.append(
            f"Most favorable latest payment-to-income signal among the compared markets: "
            f"{market_names[best_affordability_geo]} with payment_to_income_ratio="
            f"{_format_value(best_affordability_value)}."
        )

    if best_labor_geo:
        takeaways.append(
            f"Lowest latest unemployment-rate signal among the compared markets: "
            f"{market_names[best_labor_geo]} with unemployment_rate={_format_value(best_labor_value)}."
        )

    if not takeaways:
        takeaways.append("The comparison payload did not include enough non-null latest metrics to rank the selected markets.")

    market_list = ", ".join(market_names[geo_id] for geo_id in request.geo_ids)

    summary_parts = [
        f"This comparison reviews {market_list} using deterministic OneHaven market context, coverage diagnostics, and compare payloads.",
        "The endpoint does not change cycle phase, confidence score, investor signal, or any deterministic metric.",
        "The main evidence-backed observations are: " + " ".join(takeaways),
    ]

    if request.detail_level in {"standard", "detailed"}:
        summary_parts.append("Confidence: " + " ".join(confidence_lines))
        summary_parts.append("Missing data: " + " ".join(missing_lines))

    if request.detail_level == "detailed":
        summary_parts.append(
            "Use the structured_payloads field for the raw compare/context/coverage data that supports this explanation."
        )

    response = CompareSummaryResponse(
        geo_ids=request.geo_ids,
        audience=request.audience,
        detail_level=request.detail_level,
        summary=" ".join(summary_parts),
        key_takeaways=takeaways,
        confidence_explanation=" ".join(confidence_lines),
        missing_data_explanation=" ".join(missing_lines),
        deterministic_scores_note=(
            "This endpoint summarizes existing deterministic scores and metrics only. "
            "It does not override cycle phase, investor signal, confidence_score, or source coverage."
        ),
        not_investment_advice=(
            "This is market intelligence for research workflow support, not investment, legal, tax, or financing advice."
        ),
        citations=_build_citations(contexts, coverages, compare_payload),
        structured_payloads={
            "compare": compare_payload,
            "contexts": contexts,
            "coverages": coverages,
        },
    )

    return response

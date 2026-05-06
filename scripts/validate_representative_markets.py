from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000").rstrip("/")
OUTPUT_DIR = Path("data/diagnostics/representative_markets")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPRESENTATIVE_MARKETS = [
    ("us", "United States"),
    ("metro_19820", "Detroit-Warren-Dearborn, MI"),
    ("metro_16980", "Chicago-Naperville-Elgin, IL-IN-WI"),
    ("metro_19100", "Dallas-Fort Worth-Arlington, TX"),
    ("metro_12420", "Austin-Round Rock-Georgetown, TX"),
    ("metro_45300", "Tampa-St. Petersburg-Clearwater, FL"),
    ("metro_38060", "Phoenix-Mesa-Chandler, AZ"),
    ("metro_12060", "Atlanta-Sandy Springs-Alpharetta, GA"),
    ("metro_42660", "Seattle-Tacoma-Bellevue, WA"),
    ("metro_14460", "Boston-Cambridge-Newton, MA-NH"),
    ("metro_31080", "Los Angeles-Long Beach-Anaheim, CA"),
    ("metro_37980", "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD"),
]


def fetch_json(path: str) -> tuple[int, dict[str, Any], float]:
    url = f"{API_BASE_URL}{path}"
    start = time.perf_counter()

    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "X-Request-ID": f"story-9-5-{path.strip('/').replace('/', '-')}",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
            body = response.read().decode("utf-8")
            return response.status, json.loads(body), elapsed_ms
    except urllib.error.HTTPError as error:
        elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
        body = error.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"error": body}
        return error.code, payload, elapsed_ms


def missing_explanation(coverage_payload: dict[str, Any], context_payload: dict[str, Any]) -> list[str]:
    explanations: list[str] = []

    coverage = coverage_payload.get("coverage", {}) or {}
    available_metrics = set(coverage_payload.get("available_metrics", []) or {})
    missing_score_inputs = coverage_payload.get("missing_score_inputs", []) or []

    for category, is_available in coverage.items():
        if is_available:
            continue

        relevant_missing = [
            metric
            for metric in missing_score_inputs
            if (
                category in metric
                or (category == "inventory" and metric in {"active_listings_yoy", "median_days_on_market", "months_supply"})
                or (category == "permits" and "permit" in metric)
                or (category == "affordability" and ("payment" in metric or "income" in metric))
                or (category == "price" and "price" in metric)
                or (category == "rent" and "rent" in metric)
                or (category == "labor" and ("unemployment" in metric or "labor" in metric))
            )
        ]

        explanations.append(
            f"{category}=false; available_metrics={sorted(available_metrics)}; "
            f"missing_score_inputs={relevant_missing or missing_score_inputs}"
        )

    if not explanations and missing_score_inputs:
        explanations.append(f"coverage=true but score still missing inputs={missing_score_inputs}")

    risks = context_payload.get("risks", []) or []
    for risk in risks:
        if isinstance(risk, str) and "missing" in risk.lower():
            explanations.append(f"context_risk={risk}")
        elif isinstance(risk, dict) and "missing" in json.dumps(risk).lower():
            explanations.append(f"context_risk={risk}")

    return explanations


def main() -> int:
    results: list[dict[str, Any]] = []
    failures: list[str] = []

    for geo_id, name in REPRESENTATIVE_MARKETS:
        context_status, context_payload, context_ms = fetch_json(f"/markets/{geo_id}/context")
        coverage_status, coverage_payload, coverage_ms = fetch_json(f"/markets/{geo_id}/coverage")

        context_ok = context_status == 200 and context_payload.get("geo_id") == geo_id
        coverage_ok = coverage_status == 200 and coverage_payload.get("geo_id") == geo_id

        confidence = context_payload.get("confidence_score")
        confidence_ge_0_8 = isinstance(confidence, (int, float)) and confidence >= 0.8

        explanations = missing_explanation(coverage_payload, context_payload)

        coverage = coverage_payload.get("coverage", {}) if isinstance(coverage_payload, dict) else {}

        result = {
            "geo_id": geo_id,
            "name": name,
            "context_status": context_status,
            "coverage_status": coverage_status,
            "context_ok": context_ok,
            "coverage_ok": coverage_ok,
            "context_latency_ms": context_ms,
            "coverage_latency_ms": coverage_ms,
            "confidence_score": confidence,
            "confidence_ge_0_8": confidence_ge_0_8,
            "latest_data_period": coverage_payload.get("latest_data_period"),
            "latest_scoreable_period": coverage_payload.get("latest_scoreable_period"),
            "coverage": coverage,
            "available_metrics": coverage_payload.get("available_metrics", []),
            "missing_score_inputs": coverage_payload.get("missing_score_inputs", []),
            "missing_data_explanations": explanations,
        }

        if not context_ok:
            failures.append(f"{geo_id}: context endpoint failed status={context_status} payload={context_payload}")

        if not coverage_ok:
            failures.append(f"{geo_id}: coverage endpoint failed status={coverage_status} payload={coverage_payload}")

        if not explanations and any(value is False for value in coverage.values()):
            failures.append(f"{geo_id}: has false coverage categories but no missing-data explanation")

        results.append(result)

    confidence_count = sum(1 for result in results if result["confidence_ge_0_8"])
    context_count = sum(1 for result in results if result["context_ok"])
    coverage_count = sum(1 for result in results if result["coverage_ok"])

    summary = {
        "api_base_url": API_BASE_URL,
        "market_count": len(REPRESENTATIVE_MARKETS),
        "context_endpoint_ok_count": context_count,
        "coverage_endpoint_ok_count": coverage_count,
        "confidence_ge_0_8_count": confidence_count,
        "confidence_threshold_required": 8,
        "passes_story_9_5": (
            context_count == len(REPRESENTATIVE_MARKETS)
            and coverage_count == len(REPRESENTATIVE_MARKETS)
            and confidence_count >= 8
            and not failures
        ),
        "failures": failures,
        "results": results,
    }

    report_path = OUTPUT_DIR / "story_9_5_representative_market_validation.json"
    report_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    print(json.dumps(summary, indent=2, sort_keys=True))

    print(f"\nReport written to: {report_path}")

    if context_count != len(REPRESENTATIVE_MARKETS):
        print(f"FAILED: context endpoints ok={context_count}/{len(REPRESENTATIVE_MARKETS)}", file=sys.stderr)
        return 1

    if coverage_count != len(REPRESENTATIVE_MARKETS):
        print(f"FAILED: coverage endpoints ok={coverage_count}/{len(REPRESENTATIVE_MARKETS)}", file=sys.stderr)
        return 1

    if confidence_count < 8:
        print(f"FAILED: confidence >= 0.8 count={confidence_count}/12", file=sys.stderr)
        return 1

    if failures:
        print("FAILED: validation failures exist", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    print("Story 9.5 representative market validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

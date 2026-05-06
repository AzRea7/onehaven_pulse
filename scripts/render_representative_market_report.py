from __future__ import annotations

import json
from pathlib import Path


REPORT_JSON = Path("data/diagnostics/representative_markets/story_9_5_representative_market_validation.json")
REPORT_MD = Path("data/diagnostics/representative_markets/story_9_5_representative_market_validation.md")


def bool_icon(value: object) -> str:
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return ""


def main() -> int:
    if not REPORT_JSON.exists():
        raise SystemExit(f"Missing validation JSON report: {REPORT_JSON}")

    payload = json.loads(REPORT_JSON.read_text(encoding="utf-8"))

    lines: list[str] = []
    lines.append("# Story 9.5 Representative Market Validation")
    lines.append("")
    lines.append(f"- API base URL: `{payload['api_base_url']}`")
    lines.append(f"- Market count: `{payload['market_count']}`")
    lines.append(f"- Context endpoints OK: `{payload['context_endpoint_ok_count']}/{payload['market_count']}`")
    lines.append(f"- Coverage endpoints OK: `{payload['coverage_endpoint_ok_count']}/{payload['market_count']}`")
    lines.append(f"- Confidence >= 0.8: `{payload['confidence_ge_0_8_count']}/{payload['market_count']}`")
    lines.append(f"- Story passed: `{payload['passes_story_9_5']}`")
    lines.append("")
    lines.append(
        "| Geo ID | Market | Confidence | Latest data | Price | Rent | Inventory | Affordability | Labor | Permits | Missing-data explanation |"
    )
    lines.append("|---|---|---:|---|---|---|---|---|---|---|---|")

    for result in payload["results"]:
        coverage = result.get("coverage") or {}
        explanations = result.get("missing_data_explanations") or []

        explanation_text = "<br>".join(str(item).replace("|", "\\|") for item in explanations)

        lines.append(
            "| "
            + " | ".join(
                [
                    str(result.get("geo_id", "")),
                    str(result.get("name", "")).replace("|", "\\|"),
                    str(result.get("confidence_score", "")),
                    str(result.get("latest_data_period", "")),
                    bool_icon(coverage.get("price")),
                    bool_icon(coverage.get("rent")),
                    bool_icon(coverage.get("inventory")),
                    bool_icon(coverage.get("affordability")),
                    bool_icon(coverage.get("labor")),
                    bool_icon(coverage.get("permits")),
                    explanation_text,
                ]
            )
            + " |"
        )

    if payload.get("failures"):
        lines.append("")
        lines.append("## Failures")
        for failure in payload["failures"]:
            lines.append(f"- {failure}")

    REPORT_MD.parent.mkdir(parents=True, exist_ok=True)
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote {REPORT_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

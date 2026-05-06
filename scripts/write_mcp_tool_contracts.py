from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CONTRACT_JSON = Path("docs/mcp/tool_contracts.v1.json")
CONTRACT_MD = Path("docs/mcp/tool_contracts.md")


def tool_contracts() -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "epic": "Epic 10 — AI/MCP Tool Layer",
        "story": "Story 10.1 — Define MCP tool contracts",
        "principles": [
            "Tools are deterministic wrappers over existing API endpoints.",
            "Tools return structured JSON, not prose.",
            "Tools do not require direct database access.",
            "Tools do not depend on hidden prompt logic.",
            "AI explanation layers must cite fields returned by these tools.",
            "Missing data must be represented explicitly, not inferred silently.",
        ],
        "tools": [
            {
                "name": "get_market_context",
                "description": "Return structured market context, evidence, scoring, coverage, risks, data quality, and freshness for one canonical geography.",
                "deterministic_api_endpoint": {
                    "method": "GET",
                    "path_template": "/markets/{geo_id}/context",
                    "example": "/markets/metro_19820/context",
                },
                "permissions": {
                    "scope": "read:market_context",
                    "auth_required_for_mvp": False,
                    "database_access": "none",
                },
                "latency_target_ms": 500,
                "data_freshness": {
                    "source": "API response source_freshness and latest_data_period",
                    "stale_behavior": "Return stale flags from source freshness. Do not hide stale data.",
                },
                "failure_behavior": {
                    "invalid_geo_id": "Return structured not_found error from API.",
                    "partial_data": "Return context with coverage false categories and missing_score_inputs.",
                    "upstream_failure": "Return API error with request id when available.",
                },
                "input_schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["geo_id"],
                    "properties": {
                        "geo_id": {
                            "type": "string",
                            "description": "Canonical geography id, for example us or metro_19820.",
                            "pattern": "^(us|state_[A-Z]{2}|metro_[0-9]{5}|county_[0-9]{5}|zcta_[0-9]{5})$",
                        }
                    },
                },
                "output_schema": {
                    "type": "object",
                    "required": [
                        "geo_id",
                        "market",
                        "geo_type",
                        "latest_period",
                        "data_status",
                        "cycle_phase",
                        "investor_signal",
                        "confidence_score",
                        "evidence",
                        "score_breakdown",
                        "coverage",
                        "risks",
                        "data_quality",
                    ],
                    "properties": {
                        "geo_id": {"type": "string"},
                        "market": {"type": "string"},
                        "geo_type": {"type": "string"},
                        "latest_period": {"type": ["string", "null"], "format": "date"},
                        "latest_data_period": {"type": ["string", "null"], "format": "date"},
                        "data_status": {"type": "string"},
                        "cycle_phase": {"type": ["string", "null"]},
                        "investor_signal": {"type": ["string", "null"]},
                        "confidence_score": {"type": ["number", "null"], "minimum": 0, "maximum": 1},
                        "evidence": {"type": "object"},
                        "score_breakdown": {"type": "object"},
                        "coverage": {
                            "type": "object",
                            "required": ["price", "rent", "inventory", "affordability", "labor", "permits"],
                            "properties": {
                                "price": {"type": "boolean"},
                                "rent": {"type": "boolean"},
                                "inventory": {"type": "boolean"},
                                "affordability": {"type": "boolean"},
                                "labor": {"type": "boolean"},
                                "permits": {"type": "boolean"},
                            },
                        },
                        "risks": {"type": "array"},
                        "data_quality": {"type": "object"},
                        "source_freshness": {"type": "array"},
                        "mcp": {"type": "object"},
                    },
                },
            },
            {
                "name": "get_market_timeseries",
                "description": "Return ordered monthly time-series values for selected metrics for one canonical geography.",
                "deterministic_api_endpoint": {
                    "method": "GET",
                    "path_template": "/markets/{geo_id}/timeseries",
                    "example": "/markets/metro_19820/timeseries?metrics=zhvi_yoy,zori_yoy,payment_to_income_ratio,unemployment_rate&start_date=2024-01-01",
                },
                "permissions": {
                    "scope": "read:market_timeseries",
                    "auth_required_for_mvp": False,
                    "database_access": "none",
                },
                "latency_target_ms": 500,
                "data_freshness": {
                    "source": "Returned period_month values and metric nulls.",
                    "stale_behavior": "Return available historical periods; do not fabricate missing months.",
                },
                "failure_behavior": {
                    "invalid_metric": "API rejects unsupported metrics.",
                    "empty_range": "Return empty items with valid market identity.",
                    "too_large_range": "API should enforce bounded windows from performance guardrails.",
                },
                "input_schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["geo_id", "metrics"],
                    "properties": {
                        "geo_id": {"type": "string"},
                        "metrics": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 12,
                            "items": {
                                "type": "string",
                                "enum": [
                                    "zhvi",
                                    "zhvi_yoy",
                                    "zori",
                                    "zori_yoy",
                                    "home_price_yoy",
                                    "rent_yoy",
                                    "median_sale_price",
                                    "active_listings",
                                    "median_days_on_market",
                                    "months_supply",
                                    "mortgage_rate_30y",
                                    "unemployment_rate",
                                    "price_to_income_ratio",
                                    "payment_to_income_ratio",
                                    "estimated_monthly_payment",
                                    "composite_cycle_score",
                                    "confidence_score",
                                ],
                            },
                        },
                        "start_date": {"type": ["string", "null"], "format": "date"},
                        "end_date": {"type": ["string", "null"], "format": "date"},
                    },
                },
                "output_schema": {
                    "type": "object",
                    "required": ["market", "metrics", "items"],
                    "properties": {
                        "market": {"type": "object"},
                        "metrics": {"type": "array", "items": {"type": "string"}},
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["period_month", "values"],
                                "properties": {
                                    "period_month": {"type": "string", "format": "date"},
                                    "values": {"type": "object"},
                                    "missing_metrics": {"type": "array", "items": {"type": "string"}},
                                },
                            },
                        },
                    },
                },
            },
            {
                "name": "compare_markets",
                "description": "Compare 2–5 canonical markets using deterministic latest metrics and time-series values.",
                "deterministic_api_endpoint": {
                    "method": "GET",
                    "path_template": "/compare/markets",
                    "example": "/compare/markets?geo_ids=metro_19820,metro_16980&metrics=zhvi_yoy,zori_yoy,payment_to_income_ratio,unemployment_rate&start_date=2024-01-01",
                },
                "permissions": {
                    "scope": "read:market_compare",
                    "auth_required_for_mvp": False,
                    "database_access": "none",
                },
                "latency_target_ms": 1000,
                "data_freshness": {
                    "source": "Returned latest rows and time-series periods.",
                    "stale_behavior": "Expose nulls/missing metrics per market.",
                },
                "failure_behavior": {
                    "too_few_markets": "Reject fewer than 2 geo_ids.",
                    "too_many_markets": "Reject more than 5 geo_ids.",
                    "invalid_geo_id": "Return structured invalid market information.",
                },
                "input_schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["geo_ids", "metrics"],
                    "properties": {
                        "geo_ids": {
                            "type": "array",
                            "minItems": 2,
                            "maxItems": 5,
                            "items": {"type": "string"},
                        },
                        "metrics": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 12,
                            "items": {"type": "string"},
                        },
                        "start_date": {"type": ["string", "null"], "format": "date"},
                        "end_date": {"type": ["string", "null"], "format": "date"},
                    },
                },
                "output_schema": {
                    "type": "object",
                    "required": ["markets", "latest", "timeseries"],
                    "properties": {
                        "markets": {"type": "array"},
                        "latest": {"type": "array"},
                        "timeseries": {"type": "array"},
                    },
                },
            },
            {
                "name": "search_markets",
                "description": "Search/list canonical markets by geography type, state, text query, pagination, and confidence filter when supported.",
                "deterministic_api_endpoint": {
                    "method": "GET",
                    "path_template": "/markets",
                    "example": "/markets?query=Detroit&limit=10",
                },
                "permissions": {
                    "scope": "read:market_search",
                    "auth_required_for_mvp": False,
                    "database_access": "none",
                },
                "latency_target_ms": 300,
                "data_freshness": {
                    "source": "geo.dim_geo and latest app-facing metrics.",
                    "stale_behavior": "Search identity remains valid even if latest metrics are missing.",
                },
                "failure_behavior": {
                    "empty_result": "Return total 0 and empty items.",
                    "invalid_filter": "Return validation error.",
                },
                "input_schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "query": {"type": ["string", "null"]},
                        "geo_type": {"type": ["string", "null"], "enum": ["national", "state", "metro", "county", "zcta", None]},
                        "state": {"type": ["string", "null"]},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
                        "offset": {"type": "integer", "minimum": 0, "default": 0},
                    },
                },
                "output_schema": {
                    "type": "object",
                    "required": ["items"],
                    "properties": {
                        "total": {"type": ["integer", "null"]},
                        "items": {"type": "array"},
                    },
                },
            },
            {
                "name": "get_source_freshness",
                "description": "Return source/dataset freshness, latest source period, load status, stale flags, and error messages.",
                "deterministic_api_endpoint": {
                    "method": "GET",
                    "path_template": "/admin/source-freshness",
                    "fallback_path_template": "/audit/source-freshness",
                    "example": "/admin/source-freshness",
                },
                "permissions": {
                    "scope": "read:source_freshness",
                    "auth_required_for_mvp": False,
                    "database_access": "none",
                },
                "latency_target_ms": 500,
                "data_freshness": {
                    "source": "analytics/source freshness audit table.",
                    "stale_behavior": "Return is_stale and stale_reason fields.",
                },
                "failure_behavior": {
                    "no_freshness_rows": "Return empty items, not fabricated statuses.",
                    "failed_source": "Return last_status and error_message.",
                },
                "input_schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "source": {"type": ["string", "null"]},
                        "dataset": {"type": ["string", "null"]},
                    },
                },
                "output_schema": {
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["source", "dataset"],
                                "properties": {
                                    "source": {"type": "string"},
                                    "dataset": {"type": "string"},
                                    "latest_source_period": {"type": ["string", "null"]},
                                    "last_loaded_at": {"type": ["string", "null"]},
                                    "last_status": {"type": ["string", "null"]},
                                    "is_stale": {"type": ["boolean", "null"]},
                                    "stale_reason": {"type": ["string", "null"]},
                                    "record_count": {"type": ["integer", "null"]},
                                    "error_message": {"type": ["string", "null"]},
                                },
                            },
                        }
                    },
                },
            },
            {
                "name": "get_geo_coverage",
                "description": "Return coverage diagnostics, available metrics, missing score inputs, and latest scoreable period for one geography.",
                "deterministic_api_endpoint": {
                    "method": "GET",
                    "path_template": "/markets/{geo_id}/coverage",
                    "example": "/markets/metro_19820/coverage",
                },
                "permissions": {
                    "scope": "read:geo_coverage",
                    "auth_required_for_mvp": False,
                    "database_access": "none",
                },
                "latency_target_ms": 500,
                "data_freshness": {
                    "source": "Latest app-facing market metrics and coverage logic.",
                    "stale_behavior": "Return latest_data_period and latest_scoreable_period.",
                },
                "failure_behavior": {
                    "invalid_geo_id": "Return structured not_found error from API.",
                    "missing_metrics": "Return coverage false categories and missing_score_inputs.",
                },
                "input_schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["geo_id"],
                    "properties": {
                        "geo_id": {"type": "string"},
                    },
                },
                "output_schema": {
                    "type": "object",
                    "required": [
                        "geo_id",
                        "latest_data_period",
                        "latest_scoreable_period",
                        "coverage",
                        "available_metrics",
                        "missing_score_inputs",
                        "data_status",
                    ],
                    "properties": {
                        "geo_id": {"type": "string"},
                        "latest_data_period": {"type": ["string", "null"], "format": "date"},
                        "latest_scoreable_period": {"type": ["string", "null"], "format": "date"},
                        "coverage": {"type": "object"},
                        "available_metrics": {"type": "array", "items": {"type": "string"}},
                        "missing_score_inputs": {"type": "array", "items": {"type": "string"}},
                        "data_status": {"type": "string"},
                    },
                },
            },
        ],
    }


def render_markdown(contract: dict[str, Any]) -> str:
    lines: list[str] = []

    lines.append("# MCP Tool Contracts v1")
    lines.append("")
    lines.append(f"**Epic:** {contract['epic']}")
    lines.append("")
    lines.append(f"**Story:** {contract['story']}")
    lines.append("")
    lines.append("## Principles")
    lines.append("")
    for principle in contract["principles"]:
        lines.append(f"- {principle}")

    lines.append("")
    lines.append("## Tools")
    lines.append("")

    for tool in contract["tools"]:
        endpoint = tool["deterministic_api_endpoint"]

        lines.append(f"### `{tool['name']}`")
        lines.append("")
        lines.append(tool["description"])
        lines.append("")
        lines.append(f"- Method: `{endpoint['method']}`")
        lines.append(f"- Endpoint: `{endpoint['path_template']}`")
        if endpoint.get("fallback_path_template"):
            lines.append(f"- Fallback endpoint: `{endpoint['fallback_path_template']}`")
        lines.append(f"- Example: `{endpoint['example']}`")
        lines.append(f"- Permission scope: `{tool['permissions']['scope']}`")
        lines.append(f"- Requires direct DB access: `{tool['permissions']['database_access']}`")
        lines.append(f"- Latency target: `{tool['latency_target_ms']} ms`")
        lines.append("")
        lines.append("#### Data freshness")
        lines.append("")
        lines.append(f"- Source: {tool['data_freshness']['source']}")
        lines.append(f"- Stale behavior: {tool['data_freshness']['stale_behavior']}")
        lines.append("")
        lines.append("#### Failure behavior")
        lines.append("")
        for key, value in tool["failure_behavior"].items():
            lines.append(f"- `{key}`: {value}")
        lines.append("")
        lines.append("#### Input schema")
        lines.append("")
        lines.append("```json")
        lines.append(json.dumps(tool["input_schema"], indent=2, sort_keys=True))
        lines.append("```")
        lines.append("")
        lines.append("#### Output schema")
        lines.append("")
        lines.append("```json")
        lines.append(json.dumps(tool["output_schema"], indent=2, sort_keys=True))
        lines.append("```")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    contract = tool_contracts()

    CONTRACT_JSON.parent.mkdir(parents=True, exist_ok=True)
    CONTRACT_JSON.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    CONTRACT_MD.write_text(render_markdown(contract), encoding="utf-8")

    print(f"Wrote {CONTRACT_JSON}")
    print(f"Wrote {CONTRACT_MD}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

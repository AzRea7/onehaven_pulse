from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


CONTRACT_PATH = Path("docs/mcp/tool_contracts.v1.json")
API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000").rstrip("/")

REQUIRED_TOOLS = {
    "get_market_context",
    "get_market_timeseries",
    "compare_markets",
    "search_markets",
    "get_source_freshness",
    "get_geo_coverage",
}


def fetch_json(path: str) -> tuple[int, Any]:
    url = f"{API_BASE_URL}{path}"

    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "X-Request-ID": f"mcp-contract-validation-{path.strip('/').replace('/', '-')}",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"error": body}
        return error.code, payload


def assert_schema(schema: dict[str, Any], label: str) -> None:
    assert isinstance(schema, dict), f"{label} schema must be object"
    assert schema.get("type") == "object", f"{label} schema must have type=object"
    assert "properties" in schema, f"{label} schema must define properties"


def validate_contract_file() -> dict[str, Any]:
    assert CONTRACT_PATH.exists(), f"Missing contract file: {CONTRACT_PATH}"

    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

    assert contract["schema_version"] == "1.0"
    assert isinstance(contract["tools"], list)

    tool_names = {tool["name"] for tool in contract["tools"]}
    missing = REQUIRED_TOOLS - tool_names
    extra = tool_names - REQUIRED_TOOLS

    assert not missing, f"Missing required tools: {sorted(missing)}"
    assert not extra, f"Unexpected tools for Story 10.1: {sorted(extra)}"

    for tool in contract["tools"]:
        name = tool["name"]

        endpoint = tool.get("deterministic_api_endpoint") or {}
        assert endpoint.get("method") == "GET", f"{name}: Story 10.1 tools should map to deterministic GET endpoints"
        assert endpoint.get("path_template"), f"{name}: missing endpoint path_template"

        permissions = tool.get("permissions") or {}
        assert permissions.get("database_access") == "none", f"{name}: MCP tool must not require direct DB access"
        assert permissions.get("scope", "").startswith("read:"), f"{name}: scope should be read-only"

        assert isinstance(tool.get("latency_target_ms"), int), f"{name}: latency target must be integer ms"
        assert tool["latency_target_ms"] <= 1000, f"{name}: latency target too loose for MVP"

        assert_schema(tool["input_schema"], f"{name} input")
        assert_schema(tool["output_schema"], f"{name} output")

        assert "failure_behavior" in tool, f"{name}: missing failure behavior"
        assert "data_freshness" in tool, f"{name}: missing data freshness"

    return contract


def validate_live_endpoint(tool_name: str) -> None:
    if tool_name == "get_market_context":
        status, payload = fetch_json("/markets/metro_19820/context")
        assert status == 200, payload
        assert payload["geo_id"] == "metro_19820"
        assert "evidence" in payload
        assert "coverage" in payload
        return

    if tool_name == "get_market_timeseries":
        params = urllib.parse.urlencode(
            {
                "metrics": "zhvi_yoy,zori_yoy,payment_to_income_ratio,unemployment_rate",
                "start_date": "2024-01-01",
            }
        )
        status, payload = fetch_json(f"/markets/metro_19820/timeseries?{params}")
        assert status == 200, payload
        assert payload["market"]["geo_id"] == "metro_19820"
        assert isinstance(payload["items"], list)
        return

    if tool_name == "compare_markets":
        params = urllib.parse.urlencode(
            {
                "geo_ids": "metro_19820,metro_16980",
                "metrics": "zhvi_yoy,zori_yoy,payment_to_income_ratio,unemployment_rate",
                "start_date": "2024-01-01",
            }
        )
        status, payload = fetch_json(f"/compare/markets?{params}")
        assert status == 200, payload
        assert len(payload["markets"]) == 2
        assert "latest" in payload
        assert "timeseries" in payload
        return

    if tool_name == "search_markets":
        # Current /markets behavior is deterministic list/search. Some deployments may not
        # apply text query filtering yet, so Story 10.1 validates the endpoint contract
        # shape rather than requiring a specific search hit.
        status, payload = fetch_json("/markets?limit=10")
        assert status == 200, payload
        assert "items" in payload and isinstance(payload["items"], list), payload
        assert "limit" in payload, payload
        assert "offset" in payload, payload
        assert "total" in payload, payload
        assert len(payload["items"]) <= 10, payload

        if payload["items"]:
            first = payload["items"][0]
            assert "geo_id" in first, payload
            assert "geo_type" in first, payload
            assert "display_name" in first or "name" in first, payload

        return

    if tool_name == "get_source_freshness":
        status, payload = fetch_json("/admin/source-freshness")
        if status == 404:
            status, payload = fetch_json("/audit/source-freshness")
        assert status == 200, payload
        assert "items" in payload or isinstance(payload, list), payload
        return

    if tool_name == "get_geo_coverage":
        status, payload = fetch_json("/markets/metro_19820/coverage")
        assert status == 200, payload
        assert payload["geo_id"] == "metro_19820"
        assert "coverage" in payload
        assert "available_metrics" in payload
        assert "missing_score_inputs" in payload
        return

    raise AssertionError(f"Unhandled live endpoint validation for {tool_name}")


def main() -> int:
    contract = validate_contract_file()

    print("Contract file validation passed.")

    for tool in contract["tools"]:
        validate_live_endpoint(tool["name"])
        print(f"Live endpoint validation passed: {tool['name']}")

    print("MCP tool contract validation passed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        raise

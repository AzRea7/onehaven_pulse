#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Story 10.1 MCP tool contracts smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo

mkdir -p .smoke

echo "-- Required contract artifacts exist"
test -s docs/mcp/tool_contracts.v1.json
test -s docs/mcp/tool_contracts.md
test -s scripts/validate_mcp_tool_contracts.py

echo "Contract artifacts exist."
echo

echo "-- Contract JSON is valid"
python -m json.tool docs/mcp/tool_contracts.v1.json > .smoke/mcp_tool_contracts.pretty.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path("docs/mcp/tool_contracts.v1.json").read_text())

required = {
    "get_market_context",
    "get_market_timeseries",
    "compare_markets",
    "search_markets",
    "get_source_freshness",
    "get_geo_coverage",
}

tools = {tool["name"]: tool for tool in payload["tools"]}

missing = required - set(tools)
assert not missing, f"Missing tools: {sorted(missing)}"

for name, tool in tools.items():
    assert tool["input_schema"]["type"] == "object", name
    assert tool["output_schema"]["type"] == "object", name
    assert tool["deterministic_api_endpoint"]["method"] == "GET", name
    assert tool["deterministic_api_endpoint"]["path_template"], name
    assert tool["permissions"]["database_access"] == "none", name
    assert tool["permissions"]["scope"].startswith("read:"), name
    assert "failure_behavior" in tool, name
    assert "data_freshness" in tool, name

print(f"Validated {len(tools)} MCP tool contracts.")
PY

echo
echo "-- Live endpoint alignment"
API_BASE_URL="${API_BASE_URL}" python scripts/validate_mcp_tool_contracts.py

echo
echo "-- No hidden prompt logic dependency in contracts"
python - <<'PY'
import json
from pathlib import Path

contract = json.loads(Path("docs/mcp/tool_contracts.v1.json").read_text(encoding="utf-8"))
md_text = Path("docs/mcp/tool_contracts.md").read_text(encoding="utf-8").lower()
json_text = json.dumps(contract, sort_keys=True).lower()

assert '"database_access": "none"' in json_text, "Contracts must explicitly disallow direct database access"

banned_positive_patterns = [
    "ask the model to decide",
    "use chain of thought",
    "infer secretly",
    "let the ai choose without schema",
    "prompt-only tool",
]

bad = [phrase for phrase in banned_positive_patterns if phrase in md_text or phrase in json_text]
assert not bad, f"Contract contains hidden-logic dependency language: {bad}"

for principle in contract.get("principles", []):
    lower = principle.lower()
    if "hidden prompt" in lower:
        assert "do not depend" in lower or "does not depend" in lower, principle

for tool in contract["tools"]:
    assert tool["permissions"]["database_access"] == "none", tool["name"]
    assert tool["deterministic_api_endpoint"]["path_template"], tool["name"]
    assert tool["input_schema"]["type"] == "object", tool["name"]
    assert tool["output_schema"]["type"] == "object", tool["name"]

print("No hidden prompt logic dependency found.")
PY

echo
echo "Story 10.1 MCP tool contracts smoke passed."

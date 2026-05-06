#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Story 10.2 MCP tool adapter smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo

mkdir -p .smoke/mcp

echo "-- Unit tests"
PYTHONPATH=. pytest apps/api/tests/test_mcp_tools.py -q

echo
echo "-- List tools"
python scripts/run_mcp_tool.py --list > .smoke/mcp/list_tools.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/mcp/list_tools.json").read_text())
tools = {tool["name"] for tool in payload["tools"]}

required = {
    "get_market_context",
    "get_market_timeseries",
    "compare_markets",
    "search_markets",
    "get_source_freshness",
    "get_geo_coverage",
}

missing = required - tools
assert not missing, f"Missing tools: {sorted(missing)}"

print(f"Listed tools={len(tools)}")
PY

echo
echo "-- get_market_context"
python scripts/run_mcp_tool.py \
  --tool get_market_context \
  --args '{"geo_id":"metro_19820"}' \
  --api-base-url "${API_BASE_URL}" \
  > .smoke/mcp/get_market_context.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/mcp/get_market_context.json").read_text())

assert payload["ok"] is True, payload
assert payload["tool_name"] == "get_market_context"
assert payload["result"]["geo_id"] == "metro_19820"
assert "coverage" in payload["result"]
assert "evidence" in payload["result"]

print("get_market_context passed.")
PY

echo
echo "-- get_market_timeseries"
python scripts/run_mcp_tool.py \
  --tool get_market_timeseries \
  --args '{"geo_id":"metro_19820","metrics":["zhvi_yoy","zori_yoy","payment_to_income_ratio","unemployment_rate"],"start_date":"2024-01-01"}' \
  --api-base-url "${API_BASE_URL}" \
  > .smoke/mcp/get_market_timeseries.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/mcp/get_market_timeseries.json").read_text())

assert payload["ok"] is True, payload
assert payload["tool_name"] == "get_market_timeseries"
assert payload["result"]["market"]["geo_id"] == "metro_19820"
assert isinstance(payload["result"]["items"], list)

print("get_market_timeseries passed.")
PY

echo
echo "-- compare_markets"
python scripts/run_mcp_tool.py \
  --tool compare_markets \
  --args '{"geo_ids":["metro_19820","metro_16980"],"metrics":["zhvi_yoy","zori_yoy","payment_to_income_ratio","unemployment_rate"],"start_date":"2024-01-01"}' \
  --api-base-url "${API_BASE_URL}" \
  > .smoke/mcp/compare_markets.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/mcp/compare_markets.json").read_text())

assert payload["ok"] is True, payload
assert payload["tool_name"] == "compare_markets"
assert len(payload["result"]["markets"]) == 2
assert "latest" in payload["result"]
assert "timeseries" in payload["result"]

print("compare_markets passed.")
PY

echo
echo "-- search_markets"
python scripts/run_mcp_tool.py \
  --tool search_markets \
  --args '{"limit":10}' \
  --api-base-url "${API_BASE_URL}" \
  > .smoke/mcp/search_markets.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/mcp/search_markets.json").read_text())

assert payload["ok"] is True, payload
assert payload["tool_name"] == "search_markets"
assert "items" in payload["result"]
assert "total" in payload["result"]

print("search_markets passed.")
PY

echo
echo "-- get_source_freshness"
python scripts/run_mcp_tool.py \
  --tool get_source_freshness \
  --args '{}' \
  --api-base-url "${API_BASE_URL}" \
  > .smoke/mcp/get_source_freshness.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/mcp/get_source_freshness.json").read_text())

assert payload["ok"] is True, payload
assert payload["tool_name"] == "get_source_freshness"
assert isinstance(payload["result"], (dict, list))

print("get_source_freshness passed.")
PY

echo
echo "-- get_geo_coverage"
python scripts/run_mcp_tool.py \
  --tool get_geo_coverage \
  --args '{"geo_id":"metro_19820"}' \
  --api-base-url "${API_BASE_URL}" \
  > .smoke/mcp/get_geo_coverage.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/mcp/get_geo_coverage.json").read_text())

assert payload["ok"] is True, payload
assert payload["tool_name"] == "get_geo_coverage"
assert payload["result"]["geo_id"] == "metro_19820"
assert "coverage" in payload["result"]
assert "available_metrics" in payload["result"]

print("get_geo_coverage passed.")
PY

echo
echo "-- invalid tool arguments return nonzero"
if python scripts/run_mcp_tool.py \
  --tool compare_markets \
  --args '{"geo_ids":["metro_19820"],"metrics":["zhvi_yoy"]}' \
  --api-base-url "${API_BASE_URL}" \
  > .smoke/mcp/invalid_compare_stdout.json \
  2> .smoke/mcp/invalid_compare_stderr.json; then
  echo "Expected invalid compare_markets call to fail, but it succeeded."
  exit 1
fi

grep -q "at least 2" .smoke/mcp/invalid_compare_stderr.json

echo "Invalid argument handling passed."

echo
echo
echo "-- Optional /mcp/tools route"
curl -fsS "${API_BASE_URL}/mcp/tools" > .smoke/mcp/mcp_tools_route.json

python - <<'PY2'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/mcp/mcp_tools_route.json").read_text())
assert "tools" in payload, payload
assert len(payload["tools"]) == 6, payload
print("/mcp/tools route passed.")
PY2

echo "Story 10.2 MCP tool adapter smoke passed."

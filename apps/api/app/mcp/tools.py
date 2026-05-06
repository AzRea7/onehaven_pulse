from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CONTRACT_PATH = Path(os.getenv("ONEHAVEN_MCP_CONTRACT_PATH", "docs/mcp/tool_contracts.v1.json"))
DEFAULT_API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")


class McpToolError(Exception):
    """Raised when an MCP tool call cannot be executed deterministically."""


@dataclass(frozen=True)
class McpToolResult:
    tool_name: str
    ok: bool
    status_code: int
    endpoint: str
    result: Any | None
    error: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "tool_name": self.tool_name,
            "ok": self.ok,
            "status_code": self.status_code,
            "endpoint": self.endpoint,
            "result": self.result,
            "error": self.error,
        }


def load_tool_contracts(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    if not path.exists():
        raise McpToolError(f"MCP tool contract file does not exist: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))

    if payload.get("schema_version") != "1.0":
        raise McpToolError(f"Unsupported MCP contract schema_version: {payload.get('schema_version')}")

    tools = payload.get("tools")
    if not isinstance(tools, list):
        raise McpToolError("MCP contract file must contain a tools list.")

    return payload


def list_tool_contracts(path: Path = CONTRACT_PATH) -> list[dict[str, Any]]:
    payload = load_tool_contracts(path)
    return list(payload["tools"])


def get_tool_contract(tool_name: str, path: Path = CONTRACT_PATH) -> dict[str, Any]:
    for tool in list_tool_contracts(path):
        if tool.get("name") == tool_name:
            return tool

    raise McpToolError(f"Unknown MCP tool: {tool_name}")


def _fetch_json(api_base_url: str, path: str) -> tuple[int, Any]:
    url = f"{api_base_url.rstrip('/')}{path}"

    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "X-Request-ID": f"mcp-tool-{path.strip('/').replace('/', '-').replace('?', '-')}",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body)
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"detail": body}
        return error.code, payload
    except urllib.error.URLError as error:
        raise McpToolError(f"Could not reach API server at {api_base_url}: {error}") from error


def _require_string(arguments: dict[str, Any], key: str) -> str:
    value = arguments.get(key)
    if not isinstance(value, str) or not value.strip():
        raise McpToolError(f"Argument '{key}' is required and must be a non-empty string.")
    return value.strip()


def _optional_string(arguments: dict[str, Any], key: str) -> str | None:
    value = arguments.get(key)
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise McpToolError(f"Argument '{key}' must be a string when provided.")
    return value.strip()


def _require_string_list(arguments: dict[str, Any], key: str, *, min_items: int = 1, max_items: int = 12) -> list[str]:
    value = arguments.get(key)

    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, list):
        items = []
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise McpToolError(f"Argument '{key}' must contain only non-empty strings.")
            items.append(item.strip())
    else:
        raise McpToolError(f"Argument '{key}' is required and must be a string list or comma-separated string.")

    if len(items) < min_items:
        raise McpToolError(f"Argument '{key}' must contain at least {min_items} item(s).")

    if len(items) > max_items:
        raise McpToolError(f"Argument '{key}' must contain at most {max_items} item(s).")

    return items


def _query_string(params: dict[str, Any]) -> str:
    clean: dict[str, str] = {}

    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, list):
            clean[key] = ",".join(str(item) for item in value)
        else:
            clean[key] = str(value)

    return urllib.parse.urlencode(clean)


def build_endpoint_path(tool_name: str, arguments: dict[str, Any]) -> str:
    if tool_name == "get_market_context":
        geo_id = _require_string(arguments, "geo_id")
        return f"/markets/{urllib.parse.quote(geo_id)}/context"

    if tool_name == "get_market_timeseries":
        geo_id = _require_string(arguments, "geo_id")
        metrics = _require_string_list(arguments, "metrics", min_items=1, max_items=12)
        start_date = _optional_string(arguments, "start_date")
        end_date = _optional_string(arguments, "end_date")

        query = _query_string(
            {
                "metrics": metrics,
                "start_date": start_date,
                "end_date": end_date,
            }
        )
        return f"/markets/{urllib.parse.quote(geo_id)}/timeseries?{query}"

    if tool_name == "compare_markets":
        geo_ids = _require_string_list(arguments, "geo_ids", min_items=2, max_items=5)
        metrics = _require_string_list(arguments, "metrics", min_items=1, max_items=12)
        start_date = _optional_string(arguments, "start_date")
        end_date = _optional_string(arguments, "end_date")

        query = _query_string(
            {
                "geo_ids": geo_ids,
                "metrics": metrics,
                "start_date": start_date,
                "end_date": end_date,
            }
        )
        return f"/compare/markets?{query}"

    if tool_name == "search_markets":
        query = _optional_string(arguments, "query")
        geo_type = _optional_string(arguments, "geo_type")
        state = _optional_string(arguments, "state")
        limit = int(arguments.get("limit", 20))
        offset = int(arguments.get("offset", 0))

        if limit < 1 or limit > 100:
            raise McpToolError("Argument 'limit' must be between 1 and 100.")
        if offset < 0:
            raise McpToolError("Argument 'offset' must be >= 0.")

        query_string = _query_string(
            {
                "query": query,
                "geo_type": geo_type,
                "state": state,
                "limit": limit,
                "offset": offset,
            }
        )
        return f"/markets?{query_string}"

    if tool_name == "get_source_freshness":
        source = _optional_string(arguments, "source")
        dataset = _optional_string(arguments, "dataset")

        query = _query_string(
            {
                "source": source,
                "dataset": dataset,
            }
        )

        if query:
            return f"/admin/source-freshness?{query}"
        return "/admin/source-freshness"

    if tool_name == "get_geo_coverage":
        geo_id = _require_string(arguments, "geo_id")
        return f"/markets/{urllib.parse.quote(geo_id)}/coverage"

    raise McpToolError(f"Unsupported MCP tool: {tool_name}")


def call_tool(
    tool_name: str,
    arguments: dict[str, Any] | None = None,
    *,
    api_base_url: str = DEFAULT_API_BASE_URL,
) -> dict[str, Any]:
    arguments = arguments or {}

    # Contract existence check is intentional. It prevents executable tools from drifting
    # beyond docs/mcp/tool_contracts.v1.json.
    get_tool_contract(tool_name)

    endpoint = build_endpoint_path(tool_name, arguments)
    status_code, payload = _fetch_json(api_base_url, endpoint)

    ok = 200 <= status_code < 300

    result = McpToolResult(
        tool_name=tool_name,
        ok=ok,
        status_code=status_code,
        endpoint=endpoint,
        result=payload if ok else None,
        error=None if ok else {"status_code": status_code, "payload": payload},
    )

    return result.to_dict()


def list_tools() -> list[dict[str, Any]]:
    return [
        {
            "name": tool["name"],
            "description": tool["description"],
            "input_schema": tool["input_schema"],
            "output_schema": tool["output_schema"],
            "deterministic_api_endpoint": tool["deterministic_api_endpoint"],
            "permissions": tool["permissions"],
        }
        for tool in list_tool_contracts()
    ]

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from apps.api.app.mcp.tools import McpToolError, call_tool, list_tools


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a OneHaven MCP-style tool against the local API.")
    parser.add_argument("--list", action="store_true", help="List available MCP tools.")
    parser.add_argument("--tool", help="Tool name to execute.")
    parser.add_argument("--args", default="{}", help="Tool arguments as JSON.")
    parser.add_argument("--api-base-url", default=os.getenv("API_BASE_URL", "http://localhost:8000"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.list:
        print(json.dumps({"tools": list_tools()}, indent=2, sort_keys=True))
        return 0

    if not args.tool:
        raise SystemExit("--tool is required unless --list is used")

    try:
        tool_args: dict[str, Any] = json.loads(args.args)
    except json.JSONDecodeError as error:
        raise SystemExit(f"--args must be valid JSON: {error}") from error

    if not isinstance(tool_args, dict):
        raise SystemExit("--args must decode to a JSON object")

    try:
        result = call_tool(args.tool, tool_args, api_base_url=args.api_base_url)
    except McpToolError as error:
        print(json.dumps({"ok": False, "error": str(error)}, indent=2, sort_keys=True), file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

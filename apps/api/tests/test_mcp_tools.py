from __future__ import annotations

import pytest

from apps.api.app.mcp.tools import (
    McpToolError,
    build_endpoint_path,
    list_tools,
)


def test_list_tools_contains_contract_tools() -> None:
    names = {tool["name"] for tool in list_tools()}

    assert {
        "get_market_context",
        "get_market_timeseries",
        "compare_markets",
        "search_markets",
        "get_source_freshness",
        "get_geo_coverage",
    }.issubset(names)


def test_build_market_context_path() -> None:
    assert (
        build_endpoint_path("get_market_context", {"geo_id": "metro_19820"})
        == "/markets/metro_19820/context"
    )


def test_build_market_timeseries_path() -> None:
    path = build_endpoint_path(
        "get_market_timeseries",
        {
            "geo_id": "metro_19820",
            "metrics": ["zhvi_yoy", "zori_yoy"],
            "start_date": "2024-01-01",
        },
    )

    assert path.startswith("/markets/metro_19820/timeseries?")
    assert "metrics=zhvi_yoy%2Czori_yoy" in path
    assert "start_date=2024-01-01" in path


def test_build_compare_markets_path() -> None:
    path = build_endpoint_path(
        "compare_markets",
        {
            "geo_ids": ["metro_19820", "metro_16980"],
            "metrics": ["zhvi_yoy", "unemployment_rate"],
            "start_date": "2024-01-01",
        },
    )

    assert path.startswith("/compare/markets?")
    assert "geo_ids=metro_19820%2Cmetro_16980" in path
    assert "metrics=zhvi_yoy%2Cunemployment_rate" in path


def test_search_markets_defaults() -> None:
    path = build_endpoint_path("search_markets", {})

    assert path == "/markets?limit=20&offset=0"


def test_get_source_freshness_path() -> None:
    assert build_endpoint_path("get_source_freshness", {}) == "/admin/source-freshness"


def test_get_geo_coverage_path() -> None:
    assert (
        build_endpoint_path("get_geo_coverage", {"geo_id": "metro_19820"})
        == "/markets/metro_19820/coverage"
    )


def test_unknown_tool_rejected() -> None:
    with pytest.raises(McpToolError):
        build_endpoint_path("not_a_tool", {})


def test_compare_requires_two_markets() -> None:
    with pytest.raises(McpToolError):
        build_endpoint_path(
            "compare_markets",
            {
                "geo_ids": ["metro_19820"],
                "metrics": ["zhvi_yoy"],
            },
        )


def test_timeseries_requires_metrics() -> None:
    with pytest.raises(McpToolError):
        build_endpoint_path(
            "get_market_timeseries",
            {
                "geo_id": "metro_19820",
                "metrics": [],
            },
        )

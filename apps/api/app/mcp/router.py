from __future__ import annotations

from fastapi import APIRouter

from app.mcp.tools import list_tools


router = APIRouter(prefix="/mcp", tags=["mcp"])


@router.get("/tools")
def get_mcp_tools() -> dict[str, object]:
    return {"tools": list_tools()}

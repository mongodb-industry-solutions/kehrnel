from __future__ import annotations

from typing import Any, Dict, List

from .catalog import load_catalog_definitions
from .con2l import build_executable_from_resolution, compile_con2l_to_query_plan, normalize_con2l_executable
from .object_maps import summarize_context_map
from .resolver import resolve_context_contract


def infer_default_collection(config: Dict[str, Any] | None) -> str:
    collections = (config or {}).get("collections", {}) or {}
    for key in ("contextobjects", "claims", "canonical", "compositions", "resources", "search"):
        item = collections.get(key) or {}
        name = item.get("name")
        if name:
            return name
    return "contextobjects"


async def _load_definitions(ctx: Any, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    catalog = payload.get("catalog") or {}
    definitions = catalog.get("definitions")
    if definitions:
        return await load_catalog_definitions(None, {"definitions": definitions})
    storage = (ctx.adapters or {}).get("storage")
    return await load_catalog_definitions(storage, catalog)


async def resolve_context_contract_runtime(ctx: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    definitions = await _load_definitions(ctx, payload)
    draft = payload.get("draft") or payload.get("request") or payload
    result = resolve_context_contract(draft, definitions, payload.get("options"))
    return {"ok": True, "result": result}


async def compile_con2l_runtime(ctx: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    executable = payload.get("con2lExecutable") or payload.get("executable") or payload
    normalized = normalize_con2l_executable(executable)
    compiled = compile_con2l_to_query_plan(normalized, default_collection=infer_default_collection(ctx.config))
    return {"ok": True, "result": {"executable": normalized, "compiled": compiled}}


async def summarize_context_map_runtime(ctx: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    definitions = await _load_definitions(ctx, payload)
    context_map = payload.get("objectMap") or payload.get("contextMap") or payload
    summary = summarize_context_map(context_map, definitions)
    return {"ok": True, "result": summary}


async def negotiate_con2l_runtime(ctx: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    definitions = await _load_definitions(ctx, payload)
    draft = payload.get("draft") or payload
    resolved = resolve_context_contract(draft, definitions, payload.get("options"))
    executable = None
    compiled = None
    if resolved.get("ready"):
        executable = build_executable_from_resolution(draft, resolved)
        compiled = compile_con2l_to_query_plan(executable, default_collection=infer_default_collection(ctx.config))
    return {
        "ok": True,
        "result": {
            "ready": resolved.get("ready", False),
            "resolved": resolved,
            "executable": executable,
            "compiled": compiled,
        },
    }

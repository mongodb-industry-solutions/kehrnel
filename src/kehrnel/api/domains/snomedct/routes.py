"""SNOMED CT domain routes backed by the active snomedct.mongodb strategy."""

from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Request

from kehrnel.api.bridge.app.core.database import (
    _default_env_id,
    _extract_env_id,
    _get_activation,
    _is_env_access_allowed,
)
from kehrnel.api.core.admin.routes import _error_response, _json_safe
from kehrnel.api.domains.snomedct.models import (
    SnomedConceptExpansionRequest,
    SnomedEclRequest,
    SnomedGroundRequest,
    SnomedHybridSearchRequest,
    SnomedRelationshipSearchRequest,
    SnomedSearchRequest,
    SnomedSemanticFacetsRequest,
    SnomedValueSetExpandRequest,
)
from kehrnel.engine.core.errors import KehrnelError

router = APIRouter(prefix="/api/domains/snomedct", tags=["SNOMED CT"])

SNOMEDCT_DOMAIN = "snomedct"
DEFAULT_STRATEGY_ID = os.getenv("KEHRNEL_SNOMEDCT_STRATEGY_ID", "snomedct.mongodb")


def _auth_enabled() -> bool:
    return os.getenv("KEHRNEL_AUTH_ENABLED", "false").lower() in ("1", "true", "yes")


def resolve_active_env_id(request: Request) -> str:
    env_id = _extract_env_id(request) or _default_env_id()
    if not env_id:
        raise HTTPException(status_code=400, detail="Missing active environment. Provide x-active-env (or env_id query param).")
    if _auth_enabled() and not _is_env_access_allowed(request, env_id):
        raise HTTPException(status_code=403, detail=f"Access to env_id={env_id} is not permitted for this API key.")
    return env_id


def _require_snomed_activation(request: Request, env_id: str):
    runtime, activation = _get_activation(request, env_id, SNOMEDCT_DOMAIN)
    strategy_id = (getattr(activation, "strategy_id", None) or "").strip()
    if strategy_id != DEFAULT_STRATEGY_ID:
        raise HTTPException(
            status_code=409,
            detail=(
                f"SNOMED CT domain requires strategy {DEFAULT_STRATEGY_ID!r}; "
                f"active activation is {strategy_id!r}."
            ),
        )
    return runtime, activation


def _query_payload(mode: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"domain": SNOMEDCT_DOMAIN, "query": {"mode": mode, **payload}}


@router.post("/search")
async def search_snomed(request: Request, payload: SnomedSearchRequest = Body(...)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        result = await runtime.dispatch(env_id, "query", _query_payload("search", payload.model_dump(exclude_none=True)))
        return _json_safe({"ok": True, "matches": result.get("rows", []), "explain": result.get("explain")})
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/hybrid-search")
async def hybrid_search_snomed(request: Request, payload: SnomedHybridSearchRequest = Body(...)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        result = await runtime.dispatch(env_id, "op", {"op": "snomed_hybrid_search", "payload": payload.model_dump(exclude_none=True)})
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.get("/concepts/{concept_id}")
async def lookup_snomed_concept(request: Request, concept_id: str, release_id: str | None = None):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        query = {"concept_id": concept_id}
        if release_id:
            query["release_id"] = release_id
        result = await runtime.dispatch(env_id, "query", _query_payload("lookup", query))
        rows = result.get("rows", []) if isinstance(result, dict) else []
        explain = result.get("explain") if isinstance(result, dict) else None
        return _json_safe({"ok": True, "concept": rows[0] if rows else None, "explain": explain})
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/concepts/{concept_id}/children")
async def children_snomed_concept(request: Request, concept_id: str, payload: SnomedConceptExpansionRequest | None = Body(default=None)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        body = payload.model_dump(exclude_none=True) if payload else {}
        body["concept_id"] = concept_id
        result = await runtime.dispatch(env_id, "op", {"op": "snomed_concept_children", "payload": body})
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/concepts/{concept_id}/descendants")
async def descendants_snomed_concept(request: Request, concept_id: str, payload: SnomedConceptExpansionRequest | None = Body(default=None)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        body = payload.model_dump(exclude_none=True) if payload else {}
        body["concept_id"] = concept_id
        result = await runtime.dispatch(env_id, "op", {"op": "snomed_concept_descendants", "payload": body})
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/concepts/{concept_id}/ancestors")
async def ancestors_snomed_concept(request: Request, concept_id: str, payload: SnomedConceptExpansionRequest | None = Body(default=None)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        body = payload.model_dump(exclude_none=True) if payload else {}
        body["concept_id"] = concept_id
        result = await runtime.dispatch(env_id, "op", {"op": "snomed_concept_ancestors", "payload": body})
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/ecl")
async def run_snomed_ecl(request: Request, payload: SnomedEclRequest = Body(...)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        result = await runtime.dispatch(env_id, "query", _query_payload("ecl", payload.model_dump(exclude_none=True)))
        return _json_safe({"ok": True, "matches": result.get("rows", []), "explain": result.get("explain")})
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/ecl/parse")
async def parse_snomed_ecl(request: Request, payload: SnomedEclRequest = Body(...)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        result = await runtime.dispatch(env_id, "op", {"op": "snomed_parse_ecl", "payload": payload.model_dump(exclude_none=True)})
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/ecl/compile")
async def compile_snomed_ecl(request: Request, payload: SnomedEclRequest = Body(...)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        result = await runtime.dispatch(env_id, "op", {"op": "snomed_compile_ecl", "payload": payload.model_dump(exclude_none=True)})
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/expand")
async def expand_snomed_value_set(request: Request, payload: SnomedValueSetExpandRequest = Body(...)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        result = await runtime.dispatch(env_id, "op", {"op": "snomed_expand_value_set", "payload": payload.model_dump(exclude_none=True)})
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/relationships/search")
async def relationship_search_snomed(request: Request, payload: SnomedRelationshipSearchRequest = Body(...)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        result = await runtime.dispatch(env_id, "op", {"op": "snomed_relationship_search", "payload": payload.model_dump(exclude_none=True)})
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/semantic-facets")
async def semantic_facets_snomed(request: Request, payload: SnomedSemanticFacetsRequest | None = Body(default=None)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        result = await runtime.dispatch(env_id, "op", {"op": "snomed_semantic_facets", "payload": payload.model_dump(exclude_none=True) if payload else {}})
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/ground")
async def ground_snomed_mentions(request: Request, payload: SnomedGroundRequest = Body(...)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        result = await runtime.dispatch(env_id, "op", {"op": "snomed_ground_note", "payload": payload.model_dump(exclude_none=True)})
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.get("/readiness")
async def snomed_readiness(request: Request, release_id: str | None = None):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        payload = {"release_id": release_id} if release_id else {}
        result = await runtime.dispatch(env_id, "op", {"op": "snomed_readiness", "payload": payload})
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)

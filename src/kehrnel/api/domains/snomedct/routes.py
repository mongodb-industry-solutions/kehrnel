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
from kehrnel.api.core.admin.routes import _error_response, _json_safe, _require_admin_access
from kehrnel.api.domains.snomedct.models import (
    SnomedConceptExpansionRequest,
    SnomedEclRequest,
    SnomedGroundRequest,
    SnomedGroundedCorpusRequest,
    SnomedGroundingReviewRequest,
    SnomedHybridSearchRequest,
    SnomedEnsureIndexesRequest,
    SnomedReleaseDiffRequest,
    SnomedReleaseIngestRequest,
    SnomedReleaseSourceRequest,
    SnomedRelationshipSearchRequest,
    SnomedRetrievalBenchmarkRequest,
    SnomedSearchRequest,
    SnomedSemanticFacetsRequest,
    SnomedSidecarRebuildRequest,
    SnomedSubsumesRequest,
    SnomedValidateCodeRequest,
    SnomedValueSetExpandRequest,
    SnomedValueSetRegistrationRequest,
    SnomedValueSetValidateRequest,
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


def _model_payload(payload: Any | None) -> dict[str, Any]:
    return payload.model_dump(exclude_none=True) if payload is not None else {}


async def _dispatch_snomed_op(request: Request, op: str, payload: dict[str, Any]) -> dict[str, Any]:
    env_id = resolve_active_env_id(request)
    runtime, _ = _require_snomed_activation(request, env_id)
    return await runtime.dispatch(
        env_id,
        "op",
        {"domain": SNOMEDCT_DOMAIN, "op": op, "payload": payload},
    )


@router.get("/releases")
async def list_snomed_releases(request: Request, file_pattern: str | None = None):
    try:
        return _json_safe(
            await _dispatch_snomed_op(
                request,
                "snomed_list_releases",
                {"file_pattern": file_pattern} if file_pattern else {},
            )
        )
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/releases/inspect")
async def inspect_snomed_release(
    request: Request,
    payload: SnomedReleaseSourceRequest | None = Body(default=None),
):
    try:
        return _json_safe(
            await _dispatch_snomed_op(request, "snomed_inspect_release", _model_payload(payload))
        )
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/releases/diff")
async def diff_snomed_release(request: Request, payload: SnomedReleaseDiffRequest = Body(...)):
    try:
        return _json_safe(
            await _dispatch_snomed_op(request, "snomed_diff_release", _model_payload(payload))
        )
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/releases/ingest")
async def ingest_snomed_release(request: Request, payload: SnomedReleaseIngestRequest = Body(...)):
    try:
        _require_admin_access(request)
        return _json_safe(
            await _dispatch_snomed_op(request, "snomed_ingest_release", _model_payload(payload))
        )
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/sidecar/rebuild")
async def rebuild_snomed_sidecar(request: Request, payload: SnomedSidecarRebuildRequest = Body(...)):
    try:
        _require_admin_access(request)
        return _json_safe(
            await _dispatch_snomed_op(request, "snomed_rebuild_sidecar", _model_payload(payload))
        )
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/indexes/ensure")
async def ensure_snomed_indexes(request: Request, payload: SnomedEnsureIndexesRequest = Body(...)):
    try:
        _require_admin_access(request)
        return _json_safe(
            await _dispatch_snomed_op(request, "snomed_ensure_indexes", _model_payload(payload))
        )
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.get("/capabilities")
async def snomed_capabilities(request: Request, release_id: str | None = None, include_readiness: bool = True):
    try:
        payload: dict[str, Any] = {"include_readiness": include_readiness}
        if release_id:
            payload["release_id"] = release_id
        return _json_safe(await _dispatch_snomed_op(request, "snomed_capabilities", payload))
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/validate-code")
async def validate_snomed_code(request: Request, payload: SnomedValidateCodeRequest = Body(...)):
    try:
        result = await _dispatch_snomed_op(request, "snomed_validate_code", payload.model_dump(exclude_none=True))
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/subsumes")
async def subsumes_snomed_codes(request: Request, payload: SnomedSubsumesRequest = Body(...)):
    try:
        result = await _dispatch_snomed_op(request, "snomed_subsumes", payload.model_dump(exclude_none=True))
        return _json_safe(result)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)


@router.get("/value-sets")
async def list_snomed_value_sets(request: Request, status: str | None = None, limit: int = 100):
    try:
        return _json_safe(
            await _dispatch_snomed_op(
                request,
                "snomed_list_value_sets",
                {"status": status, "limit": max(1, min(limit, 500))},
            )
        )
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.post("/value-sets")
async def register_snomed_value_set(
    request: Request, payload: SnomedValueSetRegistrationRequest = Body(...)
):
    try:
        _require_admin_access(request)
        return _json_safe(
            await _dispatch_snomed_op(
                request, "snomed_put_value_set", payload.model_dump(exclude_none=True)
            )
        )
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.post("/value-sets/$validate-code")
async def validate_snomed_value_set_code(
    request: Request, payload: SnomedValueSetValidateRequest = Body(...)
):
    try:
        return _json_safe(
            await _dispatch_snomed_op(
                request,
                "snomed_validate_value_set",
                payload.model_dump(exclude_none=True),
            )
        )
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/search")
async def search_snomed(request: Request, payload: SnomedSearchRequest = Body(...)):
    try:
        env_id = resolve_active_env_id(request)
        runtime, _ = _require_snomed_activation(request, env_id)
        result = await runtime.dispatch(env_id, "query", _query_payload("search", payload.model_dump(exclude_none=True)))
        explain = result.get("explain") or {}
        return _json_safe({"ok": True, "matches": result.get("rows", []), "page": explain.get("page"), "explain": explain})
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/suggest")
async def suggest_snomed(request: Request, payload: SnomedSearchRequest = Body(...)):
    try:
        return _json_safe(
            await _dispatch_snomed_op(request, "snomed_suggest", payload.model_dump(exclude_none=True))
        )
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


@router.get("/concepts/{concept_id}/history")
async def history_snomed_concept(
    request: Request,
    concept_id: str,
    language: str = "es",
    offset: int = 0,
    limit: int = 20,
):
    try:
        return _json_safe(
            await _dispatch_snomed_op(
                request,
                "snomed_concept_history",
                {
                    "concept_id": concept_id,
                    "language": language,
                    "offset": max(0, offset),
                    "limit": max(1, min(limit, 100)),
                },
            )
        )
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
        explain = result.get("explain") or {}
        return _json_safe({"ok": True, "matches": result.get("rows", []), "page": explain.get("page"), "explain": explain})
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


@router.post("/ground/reviews")
async def save_snomed_grounding_review(
    request: Request, payload: SnomedGroundingReviewRequest = Body(...)
):
    try:
        _require_admin_access(request)
        return _json_safe(
            await _dispatch_snomed_op(
                request,
                "snomed_save_grounding_review",
                payload.model_dump(exclude_none=True),
            )
        )
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/ground/corpus/search")
async def query_snomed_grounded_corpus(
    request: Request, payload: SnomedGroundedCorpusRequest = Body(...)
):
    try:
        return _json_safe(
            await _dispatch_snomed_op(
                request,
                "snomed_query_grounded_corpus",
                payload.model_dump(exclude_none=True),
            )
        )
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)


@router.post("/benchmarks/retrieval")
async def benchmark_snomed_retrieval(
    request: Request, payload: SnomedRetrievalBenchmarkRequest = Body(...)
):
    try:
        return _json_safe(
            await _dispatch_snomed_op(
                request,
                "snomed_benchmark_retrieval",
                payload.model_dump(exclude_none=True),
            )
        )
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

"""FHIR domain search routes (strategy-backed)."""

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
from kehrnel.api.domains.fhir.models import FhirSearchRequest
from kehrnel.engine.core.errors import KehrnelError

router = APIRouter(prefix="/api/domains/fhir", tags=["FHIR"])

FHIR_DOMAIN = "fhir"
DEFAULT_STRATEGY_ID = os.getenv("KEHRNEL_FHIR_STRATEGY_ID", "fhir.clinical_cdr")


def _auth_enabled() -> bool:
    return os.getenv("KEHRNEL_AUTH_ENABLED", "false").lower() in ("1", "true", "yes")


def resolve_active_env_id(request: Request) -> str:
    """Resolve environment from x-active-env (or configured default)."""
    env_id = _extract_env_id(request) or _default_env_id()
    if not env_id:
        raise HTTPException(
            status_code=400,
            detail="Missing active environment. Provide x-active-env (or env_id query param).",
        )
    if _auth_enabled() and not _is_env_access_allowed(request, env_id):
        raise HTTPException(
            status_code=403,
            detail=f"Access to env_id={env_id} is not permitted for this API key.",
        )
    return env_id


def _require_fhir_activation(request: Request, env_id: str):
    runtime, activation = _get_activation(request, env_id, FHIR_DOMAIN)
    strategy_id = (getattr(activation, "strategy_id", None) or "").strip()
    if strategy_id != DEFAULT_STRATEGY_ID:
        raise HTTPException(
            status_code=409,
            detail=(
                f"FHIR domain requires strategy {DEFAULT_STRATEGY_ID!r}; "
                f"active activation is {strategy_id!r}."
            ),
        )
    return runtime, activation


def to_strategy_query(payload: FhirSearchRequest) -> dict[str, Any]:
    """Map API request to fhir.clinical_cdr compile_query input."""
    query: dict[str, Any] = {
        "resource_type": payload.resource_type,
        "criteria": payload.criteria,
        "_count": payload.limit,
    }
    if payload.offset:
        query["_offset"] = payload.offset
    if payload.fhir_search:
        query["fhir_search"] = payload.fhir_search
    return query


def build_search_bundle(
    *,
    rows: list[dict[str, Any]],
    total: int | None,
    env_id: str,
    explain: dict[str, Any] | None,
) -> dict[str, Any]:
    """FHIR R5 searchset Bundle with Kehrnel execution metadata."""
    explain = explain or {}
    bundle_total = total if total is not None else len(rows)
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": bundle_total,
        "entry": [
            {
                "resource": row,
                "search": {"mode": "match"},
            }
            for row in rows
        ],
        "meta": {
            "kehrnel": {
                "engine": explain.get("engine") or "fhir_mql",
                "env": env_id,
                "strategy_id": explain.get("strategy_id") or DEFAULT_STRATEGY_ID,
                "returned": explain.get("returned", len(rows)),
            }
        },
    }


@router.post("/search")
async def search_fhir(request: Request, payload: FhirSearchRequest = Body(...)):
    """
    Execute FHIR search via the active environment's fhir.clinical_cdr strategy.

    Compiles FHIR search parameters to MQL and runs against MongoDB; returns a
    searchset Bundle.
    """
    try:
        env_id = resolve_active_env_id(request)
        runtime, activation = _require_fhir_activation(request, env_id)

        result = await runtime.dispatch(
            env_id,
            "query",
            {
                "domain": FHIR_DOMAIN,
                "query": to_strategy_query(payload),
            },
        )

        if not isinstance(result, dict):
            raise KehrnelError(
                code="FHIR_SEARCH_FAILED",
                status=500,
                message="Unexpected query result shape from strategy runtime",
            )

        rows = result.get("rows") or []
        if not isinstance(rows, list):
            rows = []

        explain = result.get("explain") if isinstance(result.get("explain"), dict) else {}
        explain.setdefault("strategy_id", getattr(activation, "strategy_id", None))
        explain.setdefault("activation_id", getattr(activation, "activation_id", None))

        total_raw = explain.get("total")
        total = int(total_raw) if total_raw is not None else None

        bundle = build_search_bundle(rows=rows, total=total, env_id=env_id, explain=explain)
        return _json_safe(bundle)
    except HTTPException:
        raise
    except KehrnelError as exc:
        return _error_response(exc)
    except Exception as exc:
        return _error_response(exc)

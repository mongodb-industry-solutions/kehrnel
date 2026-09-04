"""
Canonical FHIR serialization, versioned search envelope, and error mapping.

Phase 0:
- T5 canonical serialization: return valid FHIR resources with operational storage
  fields removed, while PRESERVING legitimate primitive-extension fields (``_birthDate``,
  ``_gender``, …). This is a denylist of known operational fields — never "strip every
  underscore key," which would corrupt primitive extensions.
- T4 versioned search contract: a single ``contract_version`` and a bounded execution
  summary (no large resolved-id arrays in the default response).
- FHIR HTTP error boundary: map internal ``KehrnelError`` → ``OperationOutcome``. Kept
  here so any FHIR-facing route can reuse it; strategy ops keep raising ``KehrnelError``.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

# Versioned search execution contract (T4). Bump on any breaking envelope change.
SEARCH_CONTRACT_VERSION = "1.0"

# Operational storage fields written by the FHIR store / denormalizer. These are
# NOT canonical FHIR content and must be stripped from FHIR-facing output.
# NOTE: this is an explicit denylist — primitive-extension fields such as
# ``_birthDate`` are canonical and are intentionally preserved.
OPERATIONAL_FIELDS = frozenset(
    {
        "_id",
        "_search",
        "_stored_at",
        "_compartments",
        "_fhir_resource_type",
        "_kehrnel",
        "_custom",
        "_enrichments",
    }
)


def mongo_exclusion_projection() -> Dict[str, int]:
    """Projection that prevents operational namespaces leaving MongoDB reads."""
    return {field: 0 for field in OPERATIONAL_FIELDS}


# Keys that mark the ADR's target envelope shape ({resource, search, compartments,
# control}) as distinct from a legacy root-level resource document.
_ENVELOPE_MARKERS = frozenset({"search", "compartments", "control"})


def canonical_resource(doc: Dict[str, Any] | None) -> Dict[str, Any]:
    """Return a canonical FHIR resource with operational fields removed.

    Handles BOTH storage shapes:
    - **Legacy root doc**: FHIR resource with operational keys mixed in at the root
      (`_id`, `_search`, …) → strip the operational denylist.
    - **ADR envelope** `{resource, search, compartments, control}` → return the nested
      canonical `resource` (still denylist-stripped for safety).

    Preserves FHIR primitive extensions (`_birthDate`) in both cases.
    """
    if not isinstance(doc, dict):
        return doc  # type: ignore[return-value]
    # Envelope detection: a dict `resource` plus at least one envelope marker.
    inner = doc.get("resource")
    if isinstance(inner, dict) and (_ENVELOPE_MARKERS & set(doc.keys())):
        return {k: v for k, v in inner.items() if k not in OPERATIONAL_FIELDS}
    return {k: v for k, v in doc.items() if k not in OPERATIONAL_FIELDS}


def canonical_resources(docs: Iterable[Dict[str, Any]] | None) -> List[Dict[str, Any]]:
    if not docs:
        return []
    return [canonical_resource(d) for d in docs]


# ── FHIR searchset Bundle (canonical) ─────────────────────────────────────────


def searchset_bundle(
    resources: Iterable[Dict[str, Any]] | None,
    *,
    total: int | None = None,
) -> Dict[str, Any]:
    """Wrap canonical resources in a FHIR ``searchset`` Bundle."""
    entries = [{"resource": r} for r in canonical_resources(resources)]
    bundle: Dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": entries,
    }
    if total is not None:
        bundle["total"] = total
    return bundle


# ── OperationOutcome (FHIR HTTP error boundary) ───────────────────────────────

# Map internal Kehrnel error codes → FHIR issue types. Default: processing.
_ISSUE_TYPE_BY_CODE = {
    "INVALID_INPUT": "invalid",
    "FHIR_SEARCH_UNSUPPORTED_PARAM": "not-supported",
    "FHIR_SEARCH_NOT_CONFIGURED": "not-supported",
    "FHIR_SEARCH_COMPILE_FAILED": "invalid",
    "FHIR_LIBS_NOT_INSTALLED": "exception",
}

# Valid FHIR R5 issue-type codes (subset used here) — accepted directly so callers
# can pass a real FHIR code without it being remapped to "processing".
_VALID_FHIR_ISSUE_TYPES = frozenset(
    {
        "invalid",
        "structure",
        "required",
        "value",
        "invariant",
        "security",
        "login",
        "unknown",
        "expired",
        "forbidden",
        "suppressed",
        "processing",
        "not-supported",
        "duplicate",
        "multiple-matches",
        "not-found",
        "deleted",
        "too-long",
        "code-invalid",
        "extension",
        "too-costly",
        "business-rule",
        "conflict",
        "transient",
        "lock-error",
        "no-store",
        "exception",
        "timeout",
        "incomplete",
        "throttled",
        "informational",
    }
)


def operation_outcome(
    *,
    code: str,
    message: str,
    severity: str = "error",
    details: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Build a FHIR ``OperationOutcome`` from a Kehrnel error code/message.

    Use ONLY at the FHIR HTTP boundary. Internal ops keep raising ``KehrnelError``.
    """
    # Accept a valid FHIR issue-type directly; otherwise map an internal code.
    issue_type = (
        code
        if code in _VALID_FHIR_ISSUE_TYPES
        else _ISSUE_TYPE_BY_CODE.get(code, "processing")
    )
    issue: Dict[str, Any] = {
        "severity": severity,
        "code": issue_type,
        "diagnostics": message,
    }
    if code and code not in _VALID_FHIR_ISSUE_TYPES:
        issue["details"] = {"text": code}
    outcome: Dict[str, Any] = {"resourceType": "OperationOutcome", "issue": [issue]}
    return outcome


def operation_outcome_from_error(err: Any) -> Dict[str, Any]:
    """Map a ``KehrnelError``-like object to an ``OperationOutcome``."""
    code = getattr(err, "code", None) or "PROCESSING_ERROR"
    message = getattr(err, "message", None) or str(err)
    return operation_outcome(code=str(code), message=str(message))


# ── Versioned search envelope (T4) ────────────────────────────────────────────


def _bounded_execution_summary(
    explain: Dict[str, Any] | None, plan_body: Dict[str, Any] | None
) -> Dict[str, Any]:
    """A small, safe execution summary — never large resolved-id arrays.

    Full multi-step / resolved-id detail is only exposed under privileged debug.
    """
    explain = explain or {}
    plan_body = plan_body or {}
    execution = (
        explain.get("execution") if isinstance(explain.get("execution"), dict) else {}
    )

    multi_step = None
    filt = plan_body.get("filter")
    if isinstance(filt, dict) and "_multi_step" in filt:
        steps = filt.get("_multi_step") or []
        multi_step = {"stage_count": len(steps) if isinstance(steps, list) else 0}

    summary: Dict[str, Any] = {
        "collection": execution.get("collection") or plan_body.get("collection"),
        "total": explain.get("total"),
        "returned": explain.get("returned"),
        "limit": execution.get("limit"),
        "skip": execution.get("skip"),
    }
    if multi_step is not None:
        summary["multi_step"] = multi_step
    return summary


def build_search_response(
    *,
    plan_body: Dict[str, Any],
    engine_used: str,
    rows: List[Dict[str, Any]],
    explain: Dict[str, Any],
    include_privileged: bool = False,
) -> Dict[str, Any]:
    """Assemble the versioned FHIR search response envelope (executed).

    - ``bundle``: canonical searchset Bundle (operational fields stripped).
    - ``compiled_plan``: the pre-execution plan (filter/collection/handling/ignored).
    - ``execution_summary``: bounded telemetry.
    - ``mongo_execution_stats``: only when ``include_privileged`` (opt-in debug).
    """
    total = explain.get("total")
    response: Dict[str, Any] = {
        "ok": True,
        "contract_version": SEARCH_CONTRACT_VERSION,
        "engine_used": engine_used,
        "resource_type": plan_body.get("resource_type"),
        "bundle": searchset_bundle(rows, total=total),
        "total": total,
        "returned": explain.get("returned", len(rows)),
        "compiled_plan": {
            "filter": plan_body.get("filter"),
            "collection": plan_body.get("collection"),
            "resource_type": plan_body.get("resource_type"),
            "database": plan_body.get("database"),
            "handling": plan_body.get("handling"),
            "ignored_parameters": plan_body.get("ignored_parameters", []),
        },
        "execution_summary": _bounded_execution_summary(explain, plan_body),
        # Bounded executed summary (how it actually ran): snapshot mode + whether
        # multi-step was resolved. Never the resolved-id arrays (those are privileged).
        "executed": explain.get("executed"),
    }
    if include_privileged:
        # Omit stats/pipeline keys unless real values exist (don't emit null).
        stats = explain.get("mongo_execution_stats")
        if stats is not None:
            response["mongo_execution_stats"] = stats
        executed_pipeline = explain.get("_executed_pipeline")
        if executed_pipeline is not None:
            response["executed_pipeline"] = executed_pipeline
        response["_privileged"] = {"explain": explain}
    return response


def build_compile_response(*, plan_body: Dict[str, Any], engine: str) -> Dict[str, Any]:
    """Assemble the versioned compile-only (explain_only) envelope."""
    return {
        "ok": True,
        "contract_version": SEARCH_CONTRACT_VERSION,
        "explain_only": True,
        "engine": engine,
        "compiled_plan": {
            "filter": plan_body.get("filter"),
            "collection": plan_body.get("collection"),
            "resource_type": plan_body.get("resource_type"),
            "database": plan_body.get("database"),
            "handling": plan_body.get("handling"),
            "ignored_parameters": plan_body.get("ignored_parameters", []),
        },
    }

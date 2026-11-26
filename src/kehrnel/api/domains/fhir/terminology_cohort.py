"""Governed terminology-driven cohort query over canonical FHIR resources.

WHAT THIS IS
------------
One atomic, fail-closed request that answers a *qualified* clinical question:
"which patients have a condition in this SNOMED subtree, within this window?"

It composes three capabilities that already exist in this runtime rather than
re-implementing any of them:

  1. SNOMED CT expansion     - ``snomed_expand_value_set`` resolves the caller's
     ECL expression against the ACTIVATED release, so the code set is authoritative
     and pinned, never hand-written by the caller.
  2. Projection readiness    - ``fhir_stats`` must report a
     ``projection_readiness_receipt`` whose state is ``ready``. Execution refuses
     otherwise: a cohort computed over a stale projection is silently wrong, which
     is worse than no answer.
  3. FHIR search             - the compiled, governed query path executes per
     resolved code against MongoDB.

WHY PER-CODE EXECUTION
----------------------
The resource store's ``code`` search parameter does NOT implement comma-separated
OR: ``code=a,b`` returns zero rows rather than the union, and it does so WITHOUT
an error. Issuing one search per resolved code and unioning the results is
therefore the only correct way to realise a value-set cohort on this runtime. The
alternative - trusting comma-OR - produces a silently empty cohort.

The per-code fan-out is bounded by ``maxCodes`` on the expansion request and by
``MAX_CODE_QUERIES`` here, so an over-broad ECL expression cannot turn one HTTP
request into unbounded database work.

PRIVACY
-------
Subject references are returned as pseudonymous ``Patient/<id>`` references that
already exist in the store; no demographic fields are assembled or returned by
this service. The caller renders a count and bounded citations.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Any

from kehrnel.engine.core.errors import KehrnelError

REQUEST_CONTRACT = "fhir-terminology-cohort-query-request/v1"
RESULT_CONTRACT = "fhir-terminology-cohort-query-result/v1"

#: Hard ceiling on the per-code fan-out, independent of what the caller asks for.
MAX_CODE_QUERIES = 256
#: Hard ceiling on returned resources, independent of the caller's ``limit``.
MAX_ROW_LIMIT = 200
#: How many per-code searches may be in flight at once. Bounded so one cohort
#: request cannot saturate the database connection pool.
CODE_QUERY_CONCURRENCY = 12

SNOMED_SYSTEM = "http://snomed.info/sct"


def _digest(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _require(condition: bool, code: str, status: int, message: str) -> None:
    if not condition:
        raise KehrnelError(code=code, status=status, message=message)


def _as_dict(value: Any, code: str, message: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise KehrnelError(code=code, status=502, message=message)
    return value


def validate_request(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Validate the cohort request, failing closed on anything unexpected."""
    body = payload or {}
    _require(
        isinstance(body, dict),
        "INVALID_INPUT",
        400,
        "Cohort query payload must be a JSON object.",
    )
    _require(
        str(body.get("contract") or "") == REQUEST_CONTRACT,
        "INVALID_INPUT",
        400,
        f"Cohort query requires contract {REQUEST_CONTRACT!r}.",
    )

    resource_type = str(body.get("resourceType") or "").strip()
    _require(
        bool(resource_type),
        "INVALID_INPUT",
        400,
        "Cohort query requires a resourceType.",
    )

    expansion = body.get("expansionRequest")
    _require(
        isinstance(expansion, dict),
        "INVALID_INPUT",
        400,
        "Cohort query requires an expansionRequest object.",
    )
    expression = str((expansion or {}).get("expression") or "").strip()
    _require(
        bool(expression),
        "INVALID_INPUT",
        400,
        "Cohort query requires expansionRequest.expression (an ECL expression).",
    )

    criteria = body.get("criteria") or {}
    _require(
        isinstance(criteria, dict),
        "INVALID_INPUT",
        400,
        "Cohort query criteria must be an object.",
    )

    try:
        limit = int(body.get("limit") or 50)
    except (TypeError, ValueError):
        raise KehrnelError(
            code="INVALID_INPUT", status=400, message="limit must be an integer."
        ) from None
    try:
        offset = int(body.get("offset") or 0)
    except (TypeError, ValueError):
        raise KehrnelError(
            code="INVALID_INPUT", status=400, message="offset must be an integer."
        ) from None
    _require(limit > 0, "INVALID_INPUT", 400, "limit must be positive.")
    _require(offset >= 0, "INVALID_INPUT", 400, "offset must not be negative.")

    try:
        max_codes = int((expansion or {}).get("maxCodes") or 128)
    except (TypeError, ValueError):
        max_codes = 128

    return {
        "resourceType": resource_type,
        "codeSearchParameter": str(body.get("codeSearchParameter") or "code").strip(),
        "criteria": {str(k): v for k, v in criteria.items()},
        "sort": body.get("sort"),
        "limit": min(limit, MAX_ROW_LIMIT),
        "offset": offset,
        "expansion": {
            "expansionId": (expansion or {}).get("expansionId"),
            "providerId": (expansion or {}).get("providerId"),
            "systemUri": (expansion or {}).get("systemUri") or SNOMED_SYSTEM,
            "version": (expansion or {}).get("version"),
            "expression": expression,
            "maxCodes": max(1, min(max_codes, MAX_CODE_QUERIES)),
        },
    }


async def _expand_terminology(
    runtime: Any, env_id: str, expansion: dict[str, Any]
) -> dict[str, Any]:
    """Resolve the ECL expression against the activated SNOMED release."""
    # The expansion op PAGINATES. Its page size is controlled by ``limit`` (not
    # ``count``, which it silently ignores while still reporting hasMore), so a
    # single unpaged call truncates the value set and under-counts the cohort
    # without any error. Follow nextOffset until the release is exhausted or the
    # caller's ceiling is reached.
    raw_concepts: list[Any] = []
    result: dict[str, Any] = {}
    offset = 0
    budget = expansion["maxCodes"]
    while len(raw_concepts) < budget:
        page_size = min(100, budget - len(raw_concepts))
        try:
            page_result = await runtime.dispatch(
                env_id,
                "op",
                {
                    "domain": "snomedct",
                    "op": "snomed_expand_value_set",
                    "payload": {
                        "expression": expansion["expression"],
                        "limit": page_size,
                        "offset": offset,
                    },
                },
            )
        except KehrnelError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise KehrnelError(
                code="TERMINOLOGY_UNAVAILABLE",
                status=503,
                message=(
                    "The activated SNOMED CT terminology could not expand the requested "
                    f"expression: {exc}"
                ),
            ) from exc

        page_result = _as_dict(
            page_result,
            "TERMINOLOGY_CONTRACT_MISMATCH",
            "The terminology strategy returned an unexpected expansion shape.",
        )
        result = result or page_result
        batch = (
            page_result.get("concepts")
            or page_result.get("matches")
            or page_result.get("expansion")
            or []
        )
        if not batch:
            break
        raw_concepts.extend(batch)

        page = page_result.get("page") or {}
        next_offset = page.get("nextOffset")
        if not page.get("hasMore") or next_offset is None:
            break
        offset = int(next_offset)

    codes: list[str] = []
    displays: dict[str, str] = {}
    for concept in raw_concepts:
        if not isinstance(concept, dict):
            continue
        code = str(concept.get("conceptId") or concept.get("code") or "").strip()
        if not code or code in displays:
            continue
        codes.append(code)
        display = concept.get("display") or concept.get("preferredTerm")
        if not display:
            for description in concept.get("descriptions") or []:
                if isinstance(description, dict) and description.get("term"):
                    display = description["term"]
                    break
        displays[code] = str(display or code)

    _require(
        bool(codes),
        "TERMINOLOGY_EMPTY_EXPANSION",
        422,
        (
            "The expression expanded to zero concepts in the activated release, so no "
            "authoritative code set could be bound to the query."
        ),
    )

    release_id = result.get("releaseId")
    if not release_id:
        for concept in raw_concepts:
            if isinstance(concept, dict) and concept.get("releaseId"):
                release_id = concept["releaseId"]
                break

    return {
        "codes": codes[: expansion["maxCodes"]],
        "displays": displays,
        "releaseId": release_id,
        "raw": result,
    }


async def _assert_projection_ready(
    runtime: Any, env_id: str, resource_type: str
) -> dict[str, Any]:
    """Refuse execution unless the governed readiness receipt says ``ready``."""
    try:
        stats = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": "fhir",
                "op": "fhir_stats",
                "payload": {"resource_types": [resource_type]},
            },
        )
    except KehrnelError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise KehrnelError(
            code="READINESS_UNAVAILABLE",
            status=503,
            message=f"Projection readiness could not be established: {exc}",
        ) from exc

    stats = _as_dict(
        stats,
        "READINESS_CONTRACT_MISMATCH",
        "The FHIR strategy returned an unexpected stats shape.",
    )
    collections = (stats.get("result") or stats).get("collections") or []
    row = next(
        (
            item
            for item in collections
            if isinstance(item, dict) and item.get("resource_type") == resource_type
        ),
        None,
    )
    _require(
        row is not None,
        "READINESS_UNKNOWN",
        409,
        f"No projection readiness is reported for {resource_type}.",
    )

    receipt = (row or {}).get("projection_readiness_receipt")
    _require(
        isinstance(receipt, dict),
        "READINESS_RECEIPT_MISSING",
        409,
        (
            f"The activated FHIR strategy did not emit a projection readiness receipt "
            f"for {resource_type}; this governed path refuses to execute without one."
        ),
    )
    if receipt.get("state") != "ready":
        unmet = ", ".join(receipt.get("unmet") or []) or "unspecified"
        raise KehrnelError(
            code="PROJECTION_NOT_READY",
            status=409,
            message=(
                f"{resource_type} is not ready for qualified execution. "
                f"Unmet readiness conditions: {unmet}."
            ),
        )

    return {
        "receipt": receipt,
        "database": (stats.get("result") or stats).get("database"),
        "collection": row.get("collection"),
        "documentCount": row.get("document_count"),
    }


async def _search_one_code(
    runtime: Any,
    env_id: str,
    *,
    resource_type: str,
    parameter: str,
    system: str,
    code: str,
    criteria: dict[str, Any],
    page_size: int,
) -> dict[str, Any]:
    """Run the governed compiled search for exactly one resolved code."""
    query_criteria = dict(criteria)
    query_criteria[parameter] = f"{system}|{code}" if system else code
    query_criteria["_count"] = str(page_size)
    result = await runtime.dispatch(
        env_id,
        "query",
        {
            "domain": "fhir",
            "query": {
                "resource_type": resource_type,
                "criteria": query_criteria,
                "_count": page_size,
            },
        },
    )
    return _as_dict(
        result,
        "FHIR_SEARCH_CONTRACT_MISMATCH",
        "The FHIR strategy returned an unexpected search shape.",
    )


async def execute_terminology_cohort_query(
    runtime: Any,
    env_id: str,
    payload: dict[str, Any] | None,
    *,
    strategy_id: str,
) -> dict[str, Any]:
    """Execute the governed cohort query and return the result contract."""
    started = time.perf_counter()
    request = validate_request(payload)
    resource_type = request["resourceType"]

    # Fail closed BEFORE any data query: readiness first, then terminology.
    readiness = await _assert_projection_ready(runtime, env_id, resource_type)
    terminology = await _expand_terminology(runtime, env_id, request["expansion"])

    codes = terminology["codes"]
    system = request["expansion"]["systemUri"]
    page_size = min(request["limit"] + request["offset"], MAX_ROW_LIMIT)

    resources: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    subject_refs: list[str] = []
    seen_subjects: set[str] = set()
    total_matching = 0
    matched_codes: dict[str, int] = {}

    # A value set of ~100 concepts means ~100 compiled searches. Run them with
    # bounded concurrency: serially this costs seconds of wall clock for a demo
    # question, while unbounded concurrency would let one request saturate the
    # database connection pool.
    semaphore = asyncio.Semaphore(CODE_QUERY_CONCURRENCY)

    async def run_one(code: str) -> tuple[str, dict[str, Any]]:
        async with semaphore:
            return code, await _search_one_code(
                runtime,
                env_id,
                resource_type=resource_type,
                parameter=request["codeSearchParameter"],
                system=system,
                code=code,
                criteria=request["criteria"],
                page_size=page_size,
            )

    bundles = await asyncio.gather(*(run_one(code) for code in codes))

    # Merge in the resolved-code order so the result is deterministic regardless
    # of completion order.
    for code, bundle in bundles:
        rows = bundle.get("rows")
        if rows is None:
            rows = [
                entry.get("resource")
                for entry in (bundle.get("entry") or [])
                if isinstance(entry, dict)
            ]
        rows = [row for row in rows if isinstance(row, dict)]
        if not rows:
            continue

        matched_codes[code] = int(bundle.get("total") or len(rows))
        total_matching += matched_codes[code]
        for row in rows:
            row_id = str(row.get("id") or "")
            if row_id and row_id in seen_ids:
                continue
            if row_id:
                seen_ids.add(row_id)
            resources.append(row)
            reference = (row.get("subject") or {}).get("reference")
            if reference and reference not in seen_subjects:
                seen_subjects.add(reference)
                subject_refs.append(str(reference))

    # Deterministic order, then the caller's window.
    resources.sort(key=lambda item: str(item.get("id") or ""))
    window = resources[request["offset"] : request["offset"] + request["limit"]]

    compiled_query = {
        "resourceType": resource_type,
        "codeSearchParameter": request["codeSearchParameter"],
        "system": system,
        "codes": codes,
        "criteria": request["criteria"],
        "executionMode": "per-code-union",
    }
    elapsed_ms = int((time.perf_counter() - started) * 1000)

    return {
        "contract": RESULT_CONTRACT,
        "environmentId": env_id,
        "strategyId": strategy_id,
        "resourceType": resource_type,
        "terminologyResolution": {
            "expansionId": request["expansion"]["expansionId"],
            "providerId": request["expansion"]["providerId"],
            "providerType": "kehrnel-native-snomedct",
            "releaseId": terminology["releaseId"],
            "version": request["expansion"]["version"],
            "selector": request["expansion"]["expression"],
            "selectorLanguage": "ECL",
            "system": system,
            "codes": codes,
            "conceptCount": len(codes),
            "displays": terminology["displays"],
            "expansionDigest": _digest(
                {
                    "expression": request["expansion"]["expression"],
                    "release": terminology["releaseId"],
                    "codes": codes,
                }
            ),
        },
        "executionEvidence": {
            "queryDigest": _digest(compiled_query),
            "compiledQueryDigest": _digest(
                {"compiled": compiled_query, "release": terminology["releaseId"]}
            ),
            "compiledFilterDigest": _digest(compiled_query["criteria"]),
            "rowLimit": request["limit"],
            "codeQueryCount": len(codes),
            "matchedCodeCount": len(matched_codes),
            "matchedCodes": matched_codes,
            "projectionReadiness": {
                "state": readiness["receipt"].get("state"),
                "receiptDigest": readiness["receipt"].get("receiptDigest"),
                "conditions": readiness["receipt"].get("conditions"),
                "collectionExists": True,
                "searchIndexPresent": bool(
                    (readiness["receipt"].get("conditions") or {}).get(
                        "search_index_present"
                    )
                ),
                "exactCodeIndexPresent": bool(
                    (readiness["receipt"].get("conditions") or {}).get(
                        "search_configured"
                    )
                ),
                "canonicalProjectionMismatchCount": 0,
                "database": readiness["database"],
                "collection": readiness["collection"],
            },
            "exactTokenProjection": "_search.code_systemCode",
        },
        "result": {
            "engineUsed": "fhir_mql",
            "resources": window,
            "totalMatchingResources": total_matching,
            "returnedSubjectReferences": subject_refs,
            "explain": {
                "executed": True,
                "filter": {
                    request["codeSearchParameter"]: {
                        "$in": [f"{system}|{code}" for code in codes]
                    },
                    **request["criteria"],
                },
                "execution": {
                    "collection": readiness["collection"],
                    "database": readiness["database"],
                    "durationMs": elapsed_ms,
                    "strategy": "per-code union over the resolved value set",
                },
            },
        },
    }

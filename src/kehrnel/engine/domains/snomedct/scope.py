"""Semantic search scope: resolve an ECL expression and/or an area concept.

A *scope* is the concrete set of concept ids a caller wants retrieval confined
to. Resolving it is the expensive part (a descendant expansion can return tens of
thousands of ids), so resolved clauses are cached per environment with a TTL and
a bounded size, and the response reports cache/timing metadata so callers can see
why a scope was cheap or expensive.

Scopes never widen a query: the ECL clause and the area clause are intersected,
and the result is capped at ``search.scope_max_ids``.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable

from kehrnel.engine.core.errors import KehrnelError

DEFAULT_SCOPE_TTL_SECONDS = 86_400
DEFAULT_SCOPE_MAX_ENTRIES = 5_000


class ScopeCache:
    """Bounded TTL cache for resolved scope clauses (process-wide, per environment)."""

    def __init__(self) -> None:
        self._entries: dict[str, tuple[float, dict[str, Any]]] = {}
        self.hits = 0
        self.misses = 0

    def _prune(self, now: float) -> None:
        expired = [key for key, (expires_at, _) in self._entries.items() if expires_at <= now]
        for key in expired:
            self._entries.pop(key, None)

    async def get_or_compute(
        self,
        key: str,
        *,
        ttl_seconds: int,
        max_entries: int,
        compute: Callable[[], Awaitable[dict[str, Any]]],
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        now = time.monotonic()
        self._prune(now)
        if not force_refresh:
            cached = self._entries.get(key)
            if cached and cached[0] > now:
                # Re-insert to keep the most recently used entries when trimming.
                self._entries.pop(key, None)
                self._entries[key] = cached
                self.hits += 1
                return {**cached[1], "cacheHit": True, "latencyMs": 0.0}
        self.misses += 1
        started = time.perf_counter()
        payload = await compute()
        latency_ms = round((time.perf_counter() - started) * 1000, 3)
        self._entries[key] = (now + max(ttl_seconds, 1), payload)
        while len(self._entries) > max(max_entries, 1):
            oldest = next(iter(self._entries), None)
            if oldest is None:
                break
            self._entries.pop(oldest, None)
        return {**payload, "cacheHit": False, "latencyMs": latency_ms}

    def stats(self) -> dict[str, int]:
        self._prune(time.monotonic())
        return {"size": len(self._entries), "hits": self.hits, "misses": self.misses}


SCOPE_CACHE = ScopeCache()

# Concurrent requests for the same cold clause share one expansion.
_INFLIGHT: dict[str, asyncio.Future] = {}


async def _shared_compute(key: str, compute: Callable[[], Awaitable[dict[str, Any]]]) -> dict[str, Any]:
    existing = _INFLIGHT.get(key)
    if existing is not None:
        return await asyncio.shield(existing)
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()
    _INFLIGHT[key] = future
    try:
        result = await compute()
    except BaseException as exc:  # noqa: BLE001 - propagate to every waiter
        if not future.done():
            future.set_exception(exc)
        _INFLIGHT.pop(key, None)
        raise
    if not future.done():
        future.set_result(result)
    _INFLIGHT.pop(key, None)
    return result


def _scope_settings(cfg: dict[str, Any]) -> dict[str, int]:
    search = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
    return {
        "max_ids": int(search.get("scope_max_ids") or 20_000),
        "ttl_seconds": int(search.get("scope_cache_ttl_seconds") or DEFAULT_SCOPE_TTL_SECONDS),
        "max_entries": int(search.get("scope_cache_max_entries") or DEFAULT_SCOPE_MAX_ENTRIES),
    }


async def expand_ecl_concept_ids(
    *,
    cfg: dict[str, Any],
    release_id: str,
    expression: str,
    compile_ecl: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
    aggregate: Callable[[str, list[dict[str, Any]]], Awaitable[list[dict[str, Any]]]],
    max_ids: int,
) -> dict[str, Any]:
    """Compile an ECL expression and return the matching concept ids."""
    compiled = compile_ecl(cfg, {"expression": expression, "release_id": release_id, "limit": max_ids, "offset": 0})
    pipeline = [stage for stage in compiled["pipeline"] if "$skip" not in stage and "$limit" not in stage]
    pipeline.append({"$limit": max_ids + 1})
    pipeline.append({"$project": {"_id": 0, "conceptId": 1}})
    rows = await aggregate(compiled["collection"], pipeline)
    concept_ids = sorted({str(row.get("conceptId")) for row in rows if row.get("conceptId")})
    truncated = len(concept_ids) > max_ids
    if truncated:
        concept_ids = concept_ids[:max_ids]
    return {
        "type": "ecl",
        "expression": expression,
        "ast": compiled.get("ast"),
        "planner": compiled.get("planner"),
        "conceptIds": concept_ids,
        "count": len(concept_ids),
        "truncated": truncated,
    }


async def expand_area_concept_ids(
    *,
    concepts_collection: str,
    release_id: str,
    area_concept_id: str,
    aggregate: Callable[[str, list[dict[str, Any]]], Awaitable[list[dict[str, Any]]]],
    max_ids: int,
) -> dict[str, Any]:
    """Expand an area concept to itself plus its active descendants."""
    pipeline = [
        {
            "$match": {
                "releaseId": release_id,
                "active": True,
                "$or": [{"conceptId": area_concept_id}, {"inferredAncestorIds": area_concept_id}],
            }
        },
        {"$limit": max_ids + 1},
        {"$project": {"_id": 0, "conceptId": 1}},
    ]
    rows = await aggregate(concepts_collection, pipeline)
    concept_ids = sorted({str(row.get("conceptId")) for row in rows if row.get("conceptId")})
    truncated = len(concept_ids) > max_ids
    if truncated:
        concept_ids = concept_ids[:max_ids]
    return {
        "type": "area",
        "areaConceptId": area_concept_id,
        "conceptIds": concept_ids,
        "count": len(concept_ids),
        "truncated": truncated,
    }


async def resolve_scope(
    *,
    cfg: dict[str, Any],
    env_id: str,
    release_id: str,
    expression: str | None,
    area_concept_id: str | None,
    concepts_collection: str,
    compile_ecl: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
    aggregate: Callable[[str, list[dict[str, Any]]], Awaitable[list[dict[str, Any]]]],
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Resolve the caller's ECL and/or area constraints into a concept-id scope."""
    expression = str(expression or "").strip()
    area_concept_id = str(area_concept_id or "").strip()
    if not expression and not area_concept_id:
        return {"conceptIdScope": [], "scope": None}

    settings = _scope_settings(cfg)
    max_ids = settings["max_ids"]
    ecl_clause: dict[str, Any] | None = None
    area_clause: dict[str, Any] | None = None

    if expression:
        ecl_key = f"ecl|{env_id}|{release_id}|{expression}|{max_ids}"
        ecl_clause = await SCOPE_CACHE.get_or_compute(
            ecl_key,
            ttl_seconds=settings["ttl_seconds"],
            max_entries=settings["max_entries"],
            force_refresh=force_refresh,
            compute=lambda: _shared_compute(
                ecl_key,
                lambda: expand_ecl_concept_ids(
                    cfg=cfg,
                    release_id=release_id,
                    expression=expression,
                    compile_ecl=compile_ecl,
                    aggregate=aggregate,
                    max_ids=max_ids,
                ),
            ),
        )

    if area_concept_id:
        area_key = f"area|{env_id}|{release_id}|{area_concept_id}|{max_ids}"
        area_clause = await SCOPE_CACHE.get_or_compute(
            area_key,
            ttl_seconds=settings["ttl_seconds"],
            max_entries=settings["max_entries"],
            force_refresh=force_refresh,
            compute=lambda: _shared_compute(
                area_key,
                lambda: expand_area_concept_ids(
                    concepts_collection=concepts_collection,
                    release_id=release_id,
                    area_concept_id=area_concept_id,
                    aggregate=aggregate,
                    max_ids=max_ids,
                ),
            ),
        )

    if ecl_clause and area_clause:
        mode = "ecl+area"
        area_ids = set(area_clause["conceptIds"])
        combined = [concept_id for concept_id in ecl_clause["conceptIds"] if concept_id in area_ids]
        expanded_count = len(combined)
    elif ecl_clause:
        mode = "ecl"
        combined = list(ecl_clause["conceptIds"])
        expanded_count = ecl_clause["count"]
    else:
        mode = "area"
        combined = list(area_clause["conceptIds"])
        expanded_count = area_clause["count"]

    clause_truncated = bool((ecl_clause or {}).get("truncated")) or bool((area_clause or {}).get("truncated"))
    truncated = len(combined) > max_ids
    applied = combined[:max_ids]
    return {
        "conceptIdScope": applied,
        "scope": {
            "mode": mode,
            "releaseId": release_id,
            "ecl": expression or None,
            "areaConceptId": area_concept_id or None,
            "expandedCount": expanded_count,
            "appliedScopeCount": len(applied),
            "truncated": truncated or clause_truncated,
            "ast": (ecl_clause or {}).get("ast"),
            "planner": (ecl_clause or {}).get("planner"),
            "cache": {
                "eclClauseHit": bool((ecl_clause or {}).get("cacheHit")),
                "areaClauseHit": bool((area_clause or {}).get("cacheHit")),
                "warm": (ecl_clause is None or bool(ecl_clause.get("cacheHit")))
                and (area_clause is None or bool(area_clause.get("cacheHit"))),
            },
            "timings": {
                "eclClauseMs": float((ecl_clause or {}).get("latencyMs") or 0),
                "areaClauseMs": float((area_clause or {}).get("latencyMs") or 0),
                "expansionMs": float((ecl_clause or {}).get("latencyMs") or 0)
                + float((area_clause or {}).get("latencyMs") or 0),
            },
        },
    }


def scope_payload(payload: dict[str, Any]) -> tuple[str, str]:
    """Read the scope inputs, tolerating both snake_case and camelCase callers."""
    expression = str(payload.get("ecl") or payload.get("scope_expression") or "").strip()
    area_concept_id = str(payload.get("area_concept_id") or payload.get("areaConceptId") or "").strip()
    if expression == "*":
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="A scope ECL expression must be narrower than '*'.",
        )
    return expression, area_concept_id

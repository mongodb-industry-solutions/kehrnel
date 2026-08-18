"""
strategy.py — ContextObjects RPS strategy for Kehrnel
=======================================================

Reversed-Path Search dual-collection strategy for multi-industry ContextObject
instances.  No openEHR archetypes, no dictionaries — semantic meaning lives in
the model schema.

Collections
-----------
  context_objects        — full object + flattened content nodes (cn[])
  context_objects_search — denormalised path→value pairs (sn[]) for AQL queries

Query language
--------------
  AQL dialect compiled by _aql.py → MongoDB aggregation pipeline on search coll.

Registered as: contextobjects.rps
Domain:        context
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.plugin import StrategyPlugin
from kehrnel.engine.core.types import (
    ApplyPlan,
    ApplyResult,
    QueryPlan,
    QueryResult,
    StrategyContext,
    TransformResult,
)

from . import _aql, _synthetic, _transformer, _validator


# ── Module-level constants ────────────────────────────────────────────────────

_PACK_DIR = Path(__file__).parent

MANIFEST_PATH  = _PACK_DIR / "manifest.json"
SCHEMA_PATH    = _PACK_DIR / "schema.json"
DEFAULTS_PATH  = _PACK_DIR / "defaults.json"

MANIFEST = StrategyManifest(**json.loads(MANIFEST_PATH.read_text(encoding="utf-8")))

_KNOWN_OPS = frozenset({
    "co_ingest",
    "co_validate",
    "synthetic_generate_batch",
})


# ── Config helpers ────────────────────────────────────────────────────────────

def _deep_merge(base: dict, overlay: dict) -> dict:
    result = dict(base or {})
    for k, v in (overlay or {}).items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _load_defaults() -> dict:
    if DEFAULTS_PATH.exists():
        return json.loads(DEFAULTS_PATH.read_text(encoding="utf-8"))
    return {}


def _resolve_config(ctx: StrategyContext) -> dict:
    """Merge caller config over strategy defaults."""
    return _deep_merge(_load_defaults(), ctx.config or {})


def _object_coll(cfg: dict) -> str:
    objects_coll, _ = _transformer.resolve_collection_names(cfg)
    return objects_coll


def _search_coll(cfg: dict) -> str:
    _, search_coll = _transformer.resolve_collection_names(cfg)
    return search_coll


# ── MongoDB helpers ───────────────────────────────────────────────────────────

def _get_motor_db(ctx: StrategyContext):
    """Return the Motor async database from the storage adapter, or None."""
    storage = (ctx.adapters or {}).get("storage")
    return getattr(storage, "db", None) if storage else None


def _resolve_pymongo(ctx: StrategyContext) -> tuple[str, str]:
    """Fallback: get URI + database from bindings or env vars."""
    bindings  = ctx.bindings if isinstance(ctx.bindings, dict) else {}
    db_bind   = bindings.get("db") if isinstance(bindings.get("db"), dict) else {}

    uri      = db_bind.get("uri") or os.getenv("MONGODB_URI")
    database = (
        db_bind.get("database")
        or db_bind.get("name")
        or os.getenv("MONGODB_DB")
        or (ctx.config or {}).get("database_name")
    )

    if not uri or not database:
        raise KehrnelError(
            code="BINDINGS_NOT_RESOLVED",
            status=400,
            message=(
                "MongoDB bindings not resolved for contextobjects.rps. "
                "Provide bindings.db.uri + database, or MONGODB_URI / MONGODB_DB env vars."
            ),
            details={"has_uri": bool(uri), "has_database": bool(database)},
        )
    return str(uri), str(database)


def _sanitize_doc(doc: dict) -> dict:
    """Serialize ObjectId to str for JSON-safe output."""
    from bson import ObjectId  # type: ignore[import]
    result: dict = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            result[k] = str(v)
        elif isinstance(v, dict):
            result[k] = _sanitize_doc(v)
        elif isinstance(v, list):
            result[k] = [_sanitize_doc(i) if isinstance(i, dict) else (str(i) if isinstance(i, type(ObjectId())) else i) for i in v]
        else:
            result[k] = v
    return result


# ── Index plan ────────────────────────────────────────────────────────────────

def _build_index_plan(cfg: dict) -> dict:
    """Return the full collections + indexes artifact dict."""
    obj_coll  = _object_coll(cfg)
    srch_coll = _search_coll(cfg)
    fn  = _transformer.resolve_field_names(cfg)
    OT  = fn["OT"]
    RID = fn["RID"]
    CN  = fn["CN"]
    SN  = fn["SN"]
    P   = fn["P"]
    NID = fn["NID"]
    V   = fn["V"]

    return {
        "collections": [obj_coll, srch_coll],
        "indexes": [
            # primary collection
            {"collection": obj_coll, "keys": [(OT, 1)],           "options": {"name": "idx_ot"}},
            {"collection": obj_coll, "keys": [(RID, 1)],          "options": {"name": "idx_rid", "unique": True}},
            {"collection": obj_coll, "keys": [(f"{CN}.{P}", 1)],  "options": {"name": "idx_cn_path"}},
            {"collection": obj_coll, "keys": [(f"{CN}.{NID}", 1)],"options": {"name": "idx_cn_nid"}},
            {"collection": obj_coll, "keys": [("meta.created", 1)], "options": {"name": "idx_created"}},
            # search collection
            {"collection": srch_coll, "keys": [(OT, 1)],                    "options": {"name": "idx_search_ot"}},
            {"collection": srch_coll, "keys": [(RID, 1)],                   "options": {"name": "idx_search_rid"}},
            {"collection": srch_coll, "keys": [(f"{SN}.{P}", 1), (f"{SN}.{V}", 1)], "options": {"name": "idx_search_path_val"}},
            {"collection": srch_coll, "keys": [(f"{SN}.{NID}", 1), (f"{SN}.{V}", 1)], "options": {"name": "idx_search_nid_val"}},
        ],
        "search_indexes": [],
    }


# ── Main strategy class ───────────────────────────────────────────────────────

class ContextObjectsRPSStrategy(StrategyPlugin):
    """Kehrnel strategy for ContextObjects Reversed-Path Search dual-collection."""

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        self.manifest = manifest
        if SCHEMA_PATH.exists():
            self.manifest.config_schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        if DEFAULTS_PATH.exists():
            self.manifest.default_config = _load_defaults()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def validate_config(self, ctx: StrategyContext | Dict[str, Any]) -> None:
        raw = ctx.config if isinstance(ctx, StrategyContext) else (ctx or {})
        cfg = _deep_merge(_load_defaults(), raw)

        obj_coll  = _object_coll(cfg)
        srch_coll = _search_coll(cfg)

        errors: list[str] = []
        if not obj_coll:
            errors.append("collections.objects.name is required")
        if not srch_coll:
            errors.append("collections.search.name is required")
        if obj_coll and srch_coll and obj_coll == srch_coll:
            errors.append("collections.objects.name and collections.search.name must be different")

        if errors:
            raise KehrnelError(
                code="CONFIG_INVALID",
                status=400,
                message="Invalid contextobjects.rps configuration",
                details={"errors": errors},
            )

    # ── Plan / Apply ──────────────────────────────────────────────────────────

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        cfg = _resolve_config(ctx)
        return ApplyPlan(artifacts=_build_index_plan(cfg))

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        index_admin = (ctx.adapters or {}).get("index_admin")
        created:  list[str] = []
        warnings: list[str] = []
        skipped:  list[Any] = []

        artifacts = plan.artifacts if isinstance(plan, ApplyPlan) else (plan or {}).get("artifacts", {})
        if not isinstance(artifacts, dict):
            artifacts = {}

        for coll in artifacts.get("collections", []):
            if index_admin:
                await index_admin.ensure_collection(coll)
                created.append(coll)
            else:
                skipped.append({"collection": coll, "reason": "index_admin adapter not available"})

        for idx in artifacts.get("indexes", []):
            if index_admin:
                res = await index_admin.ensure_indexes(
                    idx["collection"],
                    [{"keys": idx["keys"], "options": idx.get("options", {})}],
                )
                warnings.extend(res.get("warnings", []))
            else:
                skipped.append({"collection": idx["collection"], "reason": "index_admin adapter not available"})

        return ApplyResult(created=created, warnings=warnings, skipped=[s for s in skipped if s])

    # ── Transform / Ingest ────────────────────────────────────────────────────

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        cfg      = _resolve_config(ctx)
        instance = _payload_to_instance(payload)
        try:
            primary_doc, search_doc = _transformer.transform(instance, cfg)
        except ValueError as exc:
            raise KehrnelError(
                code="TRANSFORM_ERROR",
                status=400,
                message=str(exc),
            ) from exc

        # Set MongoDB _id from recordId for idempotent upserts
        fn   = _transformer.resolve_field_names(cfg)
        r_id = primary_doc.get(fn["RID"])
        if r_id:
            primary_doc["_id"] = r_id
            search_doc["_id"]  = r_id

        return TransformResult(base=primary_doc, search=search_doc)

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg      = _resolve_config(ctx)
        instance = _payload_to_instance(payload)
        dry_run  = bool(payload.get("dry_run"))

        try:
            primary_doc, search_doc = _transformer.transform(instance, cfg)
        except ValueError as exc:
            raise KehrnelError(code="INGEST_ERROR", status=400, message=str(exc)) from exc

        fn   = _transformer.resolve_field_names(cfg)
        r_id = primary_doc.get(fn["RID"])
        if r_id:
            primary_doc["_id"] = r_id
            search_doc["_id"]  = r_id

        if dry_run:
            return {"ok": True, "dry_run": True, "record_id": r_id}

        motor_db = _get_motor_db(ctx)
        if motor_db is None:
            raise KehrnelError(
                code="STORAGE_UNAVAILABLE",
                status=500,
                message="Motor storage adapter not available for contextobjects.rps ingest",
            )

        obj_coll  = _object_coll(cfg)
        srch_coll = _search_coll(cfg)
        filter_   = {"_id": r_id} if r_id else {}

        await motor_db[obj_coll].replace_one(filter_, primary_doc, upsert=True)

        search_enabled = (cfg.get("collections") or {}).get("search", {}).get("enabled", True)
        if search_enabled:
            await motor_db[srch_coll].replace_one(filter_, search_doc, upsert=True)

        return {"ok": True, "record_id": r_id, "inserted": True}

    # ── Query ─────────────────────────────────────────────────────────────────

    async def compile_query(
        self,
        ctx:    StrategyContext,
        domain: str,
        query:  Dict[str, Any],
    ) -> QueryPlan:
        cfg = _resolve_config(ctx)

        aql = (
            query.get("aql")
            or query.get("raw_aql")
            or query.get("query")
            or ""
        )
        if not isinstance(aql, str) or not aql.strip():
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="contextobjects.rps compile_query requires an 'aql' string",
            )

        try:
            compiled = _aql.compile_aql(aql, cfg)
        except _aql.AQLCompileError as exc:
            raise KehrnelError(
                code="AQL_COMPILE_ERROR",
                status=400,
                message=f"AQL compile error: {exc}",
                details={"aql": aql},
            ) from exc

        return QueryPlan(
            engine="contextobjects.rps",
            plan={
                "pipeline":      compiled["pipeline"],
                "collection":    compiled["collection"],
                "select_fields": compiled["select_fields"],
                "aql":           aql,
            },
        )

    async def execute_query(
        self,
        ctx:  StrategyContext,
        plan: QueryPlan,
    ) -> QueryResult:
        qp = plan if isinstance(plan, QueryPlan) else QueryPlan(**plan)

        if qp.engine != "contextobjects.rps":
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message=f"Unsupported query engine '{qp.engine}' (expected contextobjects.rps)",
            )

        plan_body     = qp.plan if isinstance(qp.plan, dict) else {}
        pipeline      = plan_body.get("pipeline")
        collection    = plan_body.get("collection")

        if not pipeline or not isinstance(pipeline, list):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="Query plan missing pipeline")
        if not collection or not isinstance(collection, str):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="Query plan missing collection")

        motor_db = _get_motor_db(ctx)

        if motor_db is not None:
            rows = await _run_aggregate_motor(motor_db, collection, pipeline)
        else:
            uri, database = _resolve_pymongo(ctx)
            rows = await asyncio.to_thread(_run_aggregate_sync, uri, database, collection, pipeline)

        return QueryResult(
            engine_used="contextobjects.rps",
            rows=rows,
            explain={
                "collection": collection,
                "returned":   len(rows),
            },
        )

    # ── Ops ───────────────────────────────────────────────────────────────────

    async def run_op(
        self,
        ctx:     StrategyContext,
        op:      str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        if op not in _KNOWN_OPS:
            raise KehrnelError(
                code="OP_NOT_SUPPORTED",
                status=400,
                message=f"contextobjects.rps does not support op '{op}'",
                details={"known_ops": sorted(_KNOWN_OPS)},
            )

        if op == "co_ingest":
            return await _op_co_ingest(ctx, payload)

        if op == "co_validate":
            return _op_co_validate(payload)

        if op == "synthetic_generate_batch":
            return await _op_synthetic_generate(ctx, payload)

        raise NotImplementedError(f"op '{op}' is declared but not implemented")


# ── Op implementations ────────────────────────────────────────────────────────

async def _op_co_ingest(ctx: StrategyContext, payload: dict) -> dict:
    """Ingest one or more ContextObject instances."""
    cfg     = _resolve_config(ctx)
    dry_run = bool(payload.get("dry_run"))

    raw_instances = payload.get("instances")
    if not isinstance(raw_instances, list):
        single = payload.get("instance")
        raw_instances = [single] if isinstance(single, dict) else []

    if not raw_instances:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="co_ingest requires 'instances' (list) or 'instance' (object) in payload",
        )

    motor_db = _get_motor_db(ctx) if not dry_run else None
    if not dry_run and motor_db is None:
        raise KehrnelError(
            code="STORAGE_UNAVAILABLE",
            status=500,
            message="Motor storage adapter not available",
        )

    fn         = _transformer.resolve_field_names(cfg)
    obj_coll   = _object_coll(cfg)
    srch_coll  = _search_coll(cfg)
    search_on  = (cfg.get("collections") or {}).get("search", {}).get("enabled", True)

    inserted = updated = 0
    warnings: list[str] = []

    for raw in raw_instances:
        if not isinstance(raw, dict):
            warnings.append(f"Skipped non-object entry: {type(raw).__name__}")
            continue

        try:
            primary_doc, search_doc = _transformer.transform(raw, cfg)
        except ValueError as exc:
            warnings.append(f"Transform failed for record '{raw.get('recordId', '?')}': {exc}")
            continue

        r_id = primary_doc.get(fn["RID"])
        if r_id:
            primary_doc["_id"] = r_id
            search_doc["_id"]  = r_id

        if dry_run:
            inserted += 1
            continue

        filt = {"_id": r_id} if r_id else {}
        res  = await motor_db[obj_coll].replace_one(filt, primary_doc, upsert=True)
        if res.upserted_id:
            inserted += 1
        else:
            updated += 1

        if search_on:
            await motor_db[srch_coll].replace_one(filt, search_doc, upsert=True)

    return {
        "ok":       True,
        "dry_run":  dry_run,
        "inserted": inserted,
        "updated":  updated,
        "warnings": warnings,
    }


def _op_co_validate(payload: dict) -> dict:
    """Validate a single ContextObject instance against its schema."""
    instance = payload.get("instance")
    schema   = payload.get("schema")

    if not isinstance(instance, dict):
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="co_validate requires 'instance' (object) in payload",
        )
    if not isinstance(schema, dict):
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="co_validate requires 'schema' (object) in payload",
        )

    result = _validator.validate(instance, schema)
    return result.to_dict()


async def _op_synthetic_generate(ctx: StrategyContext, payload: dict) -> dict:
    """Generate synthetic ContextObject instances from a schema."""
    cfg      = _resolve_config(ctx)
    dry_run  = bool(payload.get("dry_run", False))
    do_ingest = bool(payload.get("ingest", True))
    count    = int(payload.get("count", 10))
    schema   = payload.get("schema")

    # If no schema in payload, try to look it up from the model catalog
    if not isinstance(schema, dict):
        object_type = payload.get("object_type") or payload.get("objectType")
        if object_type:
            schema = await _load_schema_from_catalog(ctx, object_type, payload)

    if not isinstance(schema, dict):
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message=(
                "synthetic_generate_batch requires a 'schema' dict or 'object_type' "
                "that resolves from the model catalog."
            ),
        )

    opts = {
        "seed":             payload.get("seed"),
        "include_optional": payload.get("include_optional", True),
        "version":          payload.get("version", "1.0"),
    }
    instances = _synthetic.generate(schema, count=count, opts=opts)

    if dry_run or not do_ingest:
        return {
            "ok":        True,
            "dry_run":   True,
            "count":     len(instances),
            "inserted":  0,
            "instances": instances,
            "warnings":  [],
        }

    # Ingest generated instances
    ingest_result = await _op_co_ingest(ctx, {"instances": instances})
    ingest_result["count"]     = len(instances)
    ingest_result["instances"] = instances

    return ingest_result


async def _load_schema_from_catalog(
    ctx:         StrategyContext,
    object_type: str,
    payload:     dict,
) -> dict | None:
    """Attempt to load a semantic model schema from the MongoDB model catalog."""
    motor_db = _get_motor_db(ctx)
    if motor_db is None:
        return None

    model_source = payload.get("model_source") or {}
    catalog_coll = str(model_source.get("catalog_collection") or "user-data-models")

    try:
        doc = await motor_db[catalog_coll].find_one(
            {"$or": [{"id": object_type}, {"domainData.id": object_type}, {"name": object_type}]},
            {"domainData": 1, "schema": 1, "nodes": 1},
        )
        if doc is None:
            return None

        # Try common paths where the schema might be stored
        for path in ("domainData.schema", "domainData", "schema"):
            cur: Any = doc
            for part in path.split("."):
                cur = (cur or {}).get(part)
            if isinstance(cur, dict) and cur.get("nodes"):
                return cur

        return None
    except Exception:
        return None


# ── Motor aggregation ─────────────────────────────────────────────────────────

async def _run_aggregate_motor(motor_db: Any, collection: str, pipeline: list) -> list[dict]:
    rows: list[dict] = []
    async for doc in motor_db[collection].aggregate(pipeline):
        try:
            rows.append(_sanitize_doc(dict(doc)))
        except Exception:
            rows.append(dict(doc))
    return rows


def _run_aggregate_sync(uri: str, database: str, collection: str, pipeline: list) -> list[dict]:
    from pymongo import MongoClient  # type: ignore[import]
    client = MongoClient(uri)
    try:
        db   = client[database]
        coll = db[collection]
        rows = list(coll.aggregate(pipeline))
        return [_sanitize_doc(dict(r)) for r in rows]
    finally:
        client.close()


# ── Payload helpers ───────────────────────────────────────────────────────────

def _payload_to_instance(payload: dict) -> dict:
    """Extract a ContextObject instance from an ingest/transform payload."""
    if not isinstance(payload, dict):
        return {}

    for key in ("instance", "contextObject", "object"):
        if isinstance(payload.get(key), dict):
            return payload[key]

    # If payload itself looks like an instance (has objectType/recordId), use it directly
    if payload.get("objectType") or payload.get("recordId"):
        return payload

    return payload

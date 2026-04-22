from __future__ import annotations

import copy
import inspect
import json
import os
import random
import tempfile
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

from kehrnel.engine.core.plugin import StrategyPlugin
from kehrnel.engine.core.types import ApplyPlan, ApplyResult, TransformResult, QueryPlan, QueryResult, StrategyContext
from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.engine.core.explain import enrich_explain
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.synthetic_model_catalog import resolve_links, resolve_model_specs
from kehrnel.engine.domains.openehr.templates.generator import kehrnelGenerator
from kehrnel.engine.domains.openehr.templates.parser import TemplateParser
from kehrnel.engine.domains.openehr.aql.aql_to_ast import AQLToASTParser
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener
from kehrnel.engine.strategies.openehr.rps_dual.index_definition_builder import (
    build_search_index_definition_from_mappings,
)
from kehrnel.engine.strategies.openehr.rps_dual.query.executor import execute as execute_query_plan
from kehrnel.engine.strategies.openehr.rps_dual.services.codes_service import atcode_to_token
from kehrnel.engine.strategies.openehr.rps_dual.services.codes_service import get_codes
from kehrnel.engine.strategies.openehr.rps_dual.services.shortcuts_service import get_shortcuts
from kehrnel.engine.strategies.openehr.rps_dual.config import (
    normalize_config,
    normalize_bulk_config,
    RPSDualConfig,
    BulkConfig,
    build_flattener_config,
    build_coding_opts,
)
from kehrnel.engine.strategies.openehr.rps_dual.config_resolver import resolve_uri_async
from kehrnel.engine.strategies.openehr.rps_dual.query.compiler import (
    build_query_pipeline,
    build_query_pipeline_from_ast,
    build_runtime_strategy,
)


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


MANIFEST_PATH = Path(__file__).parent / "manifest.json"
SCHEMA_PATH = Path(__file__).parent / "schema.json"
DEFAULTS_PATH = Path(__file__).parent / "defaults.json"
BULK_SCHEMA_PATH = Path(__file__).parent / "ingest" / "bulk_schema.json"
BULK_DEFAULTS_PATH = Path(__file__).parent / "ingest" / "bulk_defaults.json"


MANIFEST = StrategyManifest(**load_json(MANIFEST_PATH))

_SUPPORTED_QUERY_SAFE_ENCODING_PROFILES = {
    "profile.codedpath",
    "profile.search_shortcuts",
}

_INGEST_CONTROL_KEYS = {
    "data_mode",
    "debug",
    "documents",
    "domain",
    "dry_run",
    "file_path",
    "sink",
    "source",
    "strategy",
    "strategy_id",
}


def _normalize_bootstrap_mode(value: Any, *, default: str) -> str:
    mode = str(value or "").strip().lower()
    return mode if mode in {"none", "ensure", "seed"} else default


def _dictionary_bootstrap_modes(strategy_cfg: RPSDualConfig, payload: Dict[str, Any] | None = None) -> Dict[str, str]:
    payload = payload or {}
    defaults = getattr(strategy_cfg.bootstrap, "dictionariesOnActivate", None)
    default_codes = getattr(defaults, "codes", "ensure")
    default_shortcuts = getattr(defaults, "shortcuts", "seed")
    return {
        "codes": _normalize_bootstrap_mode(payload.get("codes"), default=default_codes),
        "shortcuts": _normalize_bootstrap_mode(payload.get("shortcuts"), default=default_shortcuts),
    }


def _bind_query_params(node: Any, params: Dict[str, Any], missing: set[str]) -> Any:
    if isinstance(node, dict):
        bound = {}
        for key, value in node.items():
            if key == "value" and isinstance(value, str) and value.startswith("$"):
                name = value[1:]
                if name in params:
                    bound[key] = params[name]
                else:
                    missing.add(name)
                    bound[key] = value
            else:
                bound[key] = _bind_query_params(value, params, missing)
        return bound
    if isinstance(node, list):
        return [_bind_query_params(item, params, missing) for item in node]
    return node


def _allow_local_file_ingest() -> bool:
    return os.getenv("KEHRNEL_ALLOW_LOCAL_FILE_INPUTS", "false").lower() in ("1", "true", "yes")


def _local_file_inputs_base_dir() -> Path:
    raw = (os.getenv("KEHRNEL_LOCAL_FILE_INPUTS_BASE_DIR") or "").strip()
    if not raw:
        return Path.cwd().resolve()
    return Path(raw).expanduser().resolve()


def _validate_local_ingest_path(file_path: str) -> Path:
    path = Path(file_path)
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    base = _local_file_inputs_base_dir()
    try:
        path.relative_to(base)
    except ValueError as exc:
        raise ValueError("Provided path is outside the allowed inputs directory") from exc
    if not path.exists() or not path.is_file():
        raise FileNotFoundError("File not found")
    if path.suffix.lower() not in {".json", ".ndjson"}:
        raise ValueError("Only .json and .ndjson files are allowed for ingest")
    return path


def _load_ingest_documents_from_path(file_path: str) -> list[Dict[str, Any]]:
    if not _allow_local_file_ingest():
        raise ValueError(
            "Local file ingest is disabled. Enable KEHRNEL_ALLOW_LOCAL_FILE_INPUTS=true to use file_path."
        )

    path = _validate_local_ingest_path(file_path)
    suffix = path.suffix.lower()

    if suffix == ".ndjson":
        documents: list[Dict[str, Any]] = []
        for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number} of {path.name}") from exc
            if not isinstance(parsed, dict):
                raise ValueError(f"Each NDJSON line in {path.name} must be a JSON object")
            documents.append(parsed)
        if not documents:
            raise ValueError(f"No documents found in {path.name}")
        return documents

    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"The file {path.name} is not valid JSON") from exc

    if isinstance(parsed, dict) and isinstance(parsed.get("documents"), list):
        documents = parsed["documents"]
    elif isinstance(parsed, list):
        documents = parsed
    elif isinstance(parsed, dict):
        documents = [parsed]
    else:
        raise ValueError(f"The file {path.name} must contain a JSON object or array of objects")

    if not documents:
        raise ValueError(f"No documents found in {path.name}")
    if not all(isinstance(item, dict) for item in documents):
        raise ValueError(f"Every document in {path.name} must be a JSON object")
    return documents


class RPSDualStrategy(StrategyPlugin):
    def __init__(self, manifest: StrategyManifest = MANIFEST):
        self.manifest = manifest
        self.strategy_base_dir = Path(__file__).parent
        self.schema = load_json(SCHEMA_PATH)
        self.defaults = load_json(DEFAULTS_PATH)
        # Load bulk config schemas
        self.bulk_schema = load_json(BULK_SCHEMA_PATH) if BULK_SCHEMA_PATH.exists() else {}
        self.bulk_defaults = load_json(BULK_DEFAULTS_PATH) if BULK_DEFAULTS_PATH.exists() else {}
        # hydrate manifest schemas/defaults
        self.manifest.config_schema = self.schema
        self.manifest.default_config = self.defaults
        self.normalized_config: RPSDualConfig | None = None
        self.normalized_bulk_config: BulkConfig | None = None

    def _strategy_root_dir(self) -> Path:
        base_dir = getattr(self, "strategy_base_dir", None)
        if isinstance(base_dir, Path):
            return base_dir
        if isinstance(base_dir, str) and base_dir.strip():
            return Path(base_dir)
        return Path(__file__).parent

    async def validate_config(self, ctx: StrategyContext | Dict[str, Any]) -> None:
        raw_config = ctx.config if isinstance(ctx, StrategyContext) else ctx
        strategy_cfg = normalize_config(raw_config or {})
        errors: list[str] = []

        comp_profile = (strategy_cfg.collections.compositions.encodingProfile or "").strip().lower()
        if comp_profile not in _SUPPORTED_QUERY_SAFE_ENCODING_PROFILES:
            errors.append(
                "collections.compositions.encodingProfile must be one of "
                f"{sorted(_SUPPORTED_QUERY_SAFE_ENCODING_PROFILES)} for consistent ingest/query behavior."
            )

        search_profile = (strategy_cfg.collections.search.encodingProfile or "").strip().lower()
        if search_profile not in _SUPPORTED_QUERY_SAFE_ENCODING_PROFILES:
            errors.append(
                "collections.search.encodingProfile must be one of "
                f"{sorted(_SUPPORTED_QUERY_SAFE_ENCODING_PROFILES)}."
            )

        if errors:
            raise KehrnelError(
                code="CONFIG_INVALID",
                status=400,
                message="Unsupported openehr.rps_dual configuration for consistent ingest/query behavior.",
                details={"errors": errors},
            )

        self.normalized_config = strategy_cfg
        return None

    async def _resolve_mappings_content(self, ctx: StrategyContext, strategy_cfg: RPSDualConfig) -> Dict[str, Any]:
        mappings_content = (ctx.meta or {}).get("mappings") if ctx and ctx.meta else None
        mappings_ref = strategy_cfg.transform.mappings
        if mappings_content is None and mappings_ref is not None:
            db = getattr((ctx.adapters or {}).get("storage"), "db", None)
            mappings_content = await resolve_uri_async(mappings_ref, db, self._strategy_root_dir())
        if mappings_content is None:
            return {"templates": []}
        return mappings_content

    def _default_search_index_definition(self, strategy_cfg: RPSDualConfig) -> Dict[str, Any]:
        return {
            "mappings": {
                "dynamic": False,
                "fields": {
                    strategy_cfg.fields.document.ehr_id: {
                        "type": "token",
                    },
                    strategy_cfg.fields.document.tid: {
                        "type": "token",
                    },
                    strategy_cfg.fields.document.sort_time: {
                        "type": "date",
                    },
                    strategy_cfg.fields.document.sn: {
                        "type": "embeddedDocuments",
                        "fields": {
                            strategy_cfg.fields.node.p: {
                                "type": "string",
                                "analyzer": "lucene.keyword",
                            },
                            strategy_cfg.fields.node.data: {
                                "type": "document",
                                "dynamic": True,
                            },
                        },
                    },
                },
            },
        }

    async def _resolve_search_index_definition(self, ctx: StrategyContext, strategy_cfg: RPSDualConfig) -> Dict[str, Any]:
        search_cfg = strategy_cfg.collections.search
        db = getattr((ctx.adapters or {}).get("storage"), "db", None)

        try:
            mappings_content = await self._resolve_mappings_content(ctx, strategy_cfg)
            build_result = await build_search_index_definition_from_mappings(
                strategy_cfg,
                mappings_content,
                db=db,
                shortcuts=await get_shortcuts(ctx),
            )
            built_definition = build_result.get("definition")
            metadata = build_result.get("metadata") or {}
            if isinstance(built_definition, dict) and built_definition.get("mappings") and metadata.get("data_field_count", 0) > 0:
                return built_definition
        except Exception:
            pass

        definition = None
        if search_cfg.atlasIndex and search_cfg.atlasIndex.definition:
            try:
                definition = await resolve_uri_async(search_cfg.atlasIndex.definition, db, self._strategy_root_dir())
            except Exception:
                definition = None
        if isinstance(definition, dict) and definition.get("mappings"):
            return definition
        return self._default_search_index_definition(strategy_cfg)

    @staticmethod
    def _normalize_artifacts(plan: ApplyPlan | Dict[str, Any] | None) -> Dict[str, Any]:
        if isinstance(plan, dict):
            artifacts = plan.get("artifacts")
            return artifacts if isinstance(artifacts, dict) else {}
        artifacts = getattr(plan, "artifacts", None)
        return artifacts if isinstance(artifacts, dict) else {}

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        cfg = ctx.config
        strategy_cfg = normalize_config(cfg or {})
        artifacts = {"collections": [], "indexes": [], "search_indexes": []}
        comp = strategy_cfg.collections.compositions
        search = strategy_cfg.collections.search
        codes = strategy_cfg.collections.codes
        shortcuts = strategy_cfg.collections.shortcuts
        if comp.name:
            artifacts["collections"].append(comp.name)
        if search.enabled and search.name:
            artifacts["collections"].append(search.name)
        if codes.name:
            artifacts["collections"].append(codes.name)
        if shortcuts.name:
            artifacts["collections"].append(shortcuts.name)
        artifacts["collections"] = list(dict.fromkeys(artifacts["collections"]))
        if search.enabled and search.name:
            if search.atlasIndex and search.atlasIndex.name:
                definition = await self._resolve_search_index_definition(ctx, strategy_cfg)
                artifacts["search_indexes"].append(
                    {"collection": search.name, "name": search.atlasIndex.name, "definition": definition}
                )
        # index plans from pack_spec storage indexes
        artifacts = self._augment_indexes_from_spec(ctx, artifacts)
        return ApplyPlan(artifacts=artifacts)

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        storage = (ctx.adapters or {}).get("storage")
        index_admin = (ctx.adapters or {}).get("index_admin")
        atlas_search = (ctx.adapters or {}).get("atlas_search") or (ctx.adapters or {}).get("text_search")
        created = []
        warnings = []
        skipped = []
        artifacts = self._normalize_artifacts(plan)
        for coll in artifacts.get("collections", []):
            if index_admin:
                await index_admin.ensure_collection(coll)
                created.append(coll)
        for idx in artifacts.get("indexes", []):
            if index_admin:
                res = await index_admin.ensure_indexes(
                    idx.get("collection"),
                    [{"keys": idx.get("keys", []), "options": idx.get("options", {}) or {}}],
                )
                warnings.extend(res.get("warnings", []))
            else:
                skipped.append({"collection": idx.get("collection"), "reason": "index_admin adapter not available"})
        for si in artifacts.get("search_indexes", []):
            if atlas_search:
                res = await atlas_search.ensure_search_index(si["collection"], si["name"], si.get("definition", {}))
                warnings.extend(res.get("warnings", []))
            else:
                skipped.append({"collection": si.get("collection"), "reason": "atlas_search adapter not available"})
        return ApplyResult(created=created, warnings=warnings, skipped=[s for s in skipped if s])

    def _augment_indexes_from_spec(self, ctx: StrategyContext, artifacts: Dict[str, Any]) -> Dict[str, Any]:
        """Merge indexes defined in pack_spec storage indexes into the plan."""
        spec = getattr(self.manifest, "pack_spec", None) or {}
        stores = (spec.get("storage") or {}).get("stores") or (spec.get("storageModel") or {}).get("stores") or []
        store_profiles = (ctx.meta or {}).get("store_profiles") or {}
        collections_cfg = ctx.config.get("collections", {}) if isinstance(ctx.config, dict) else {}

        def resolve_collection(store: Dict[str, Any]) -> str | None:
            role = store.get("role") or store.get("id")
            if role and role in store_profiles:
                return store_profiles[role].get("collection")
            # fallback to config
            if role == "store:compositions":
                return (collections_cfg.get("compositions") or {}).get("name")
            if role == "store:search":
                return (collections_cfg.get("search") or {}).get("name")
            return store.get("destinationType")

        for store in stores:
            if not isinstance(store, dict):
                continue
            coll = resolve_collection(store)
            for idx in store.get("indexes") or []:
                if not isinstance(idx, dict):
                    continue
                idx_type = idx.get("type")
                if idx_type == "btree":
                    fields = idx.get("fields") or []
                    keys = [(f, 1) for f in fields]
                    if coll and keys:
                        artifacts["indexes"].append({"collection": coll, "keys": keys, "options": idx.get("options", {})})
                elif idx_type == "wildcard":
                    field = idx.get("field") or "data"
                    if coll:
                        artifacts["indexes"].append({"collection": coll, "keys": [(f"{field}.$**", 1)], "options": idx.get("options", {})})
                elif idx_type == "search":
                    if coll:
                        artifacts["search_indexes"].append(
                            {
                                "collection": coll,
                                "name": idx.get("name") or store.get("role") or "search_index",
                                "definition": idx.get("definition", {}),
                                "storedSource": idx.get("storedSource"),
                            }
                        )
        return artifacts

    async def _build_flattener_for_context(self, ctx: StrategyContext) -> CompositionFlattener:
        cfg = ctx.config or {}
        adapters = ctx.adapters or {}
        storage = adapters.get("storage")

        # Normalize configs using new structure
        strategy_cfg = normalize_config(cfg)
        bulk_cfg = normalize_bulk_config((ctx.meta or {}).get("bulk_config", {}))

        # Build flattener config from normalized models
        flattener_config = build_flattener_config(strategy_cfg, bulk_cfg)
        coding_cfg = build_coding_opts(strategy_cfg)
        mappings_content = await self._resolve_mappings_content(ctx, strategy_cfg)

        mappings_path = str(self._strategy_root_dir() / "ingest" / "config" / "flattener_mappings_f.jsonc")

        # Try to reuse raw motor database from the storage adapter if available
        db = getattr(storage, "db", None)

        return await CompositionFlattener.create(
            db=db,
            config=flattener_config,
            mappings_path=mappings_path,
            mappings_content=mappings_content,
            field_map=None,
            coding_opts=coding_cfg,
        )

    @staticmethod
    def _payload_to_composition_object(payload: Dict[str, Any] | Any) -> Any:
        if not isinstance(payload, dict):
            return payload
        comp_obj = payload.get("canonicalJSON")
        if comp_obj is not None:
            return comp_obj
        comp_obj = payload.get("composition")
        if comp_obj is not None:
            return comp_obj
        comp_obj = payload.get("compositionJSON")
        if comp_obj is not None:
            return comp_obj
        stripped = {k: v for k, v in payload.items() if k not in _INGEST_CONTROL_KEYS}
        return stripped or payload

    @classmethod
    def _build_raw_ingest_document(cls, payload: Dict[str, Any] | Any) -> Dict[str, Any]:
        comp_obj = cls._payload_to_composition_object(payload)
        if isinstance(payload, dict):
            source_envelope = {k: v for k, v in payload.items() if k not in _INGEST_CONTROL_KEYS}
            return {
                "_id": payload.get("_id") or "comp-1",
                "ehr_id": payload.get("ehr_id") or "ehr-1",
                "composition_version": payload.get("composition_version"),
                "time_committed": payload.get("time_committed"),
                "time_created": payload.get("time_created"),
                "canonicalJSON": comp_obj,
                "_source_envelope": source_envelope,
            }
        return {
            "_id": "comp-1",
            "ehr_id": "ehr-1",
            "composition_version": None,
            "time_committed": None,
            "time_created": None,
            "canonicalJSON": comp_obj,
            "_source_envelope": None,
        }

    @classmethod
    def _expand_ingest_payload(cls, payload: Dict[str, Any]) -> tuple[str, str, list[Dict[str, Any]]]:
        if not isinstance(payload, dict):
            return "single", "payload", [cls._build_raw_ingest_document(payload)]

        file_path = payload.get("file_path")
        documents = payload.get("documents")
        if file_path and documents is not None:
            raise ValueError("Use either file_path or documents, not both")

        if file_path:
            loaded = _load_ingest_documents_from_path(str(file_path))
            return "batch", "file", [cls._build_raw_ingest_document(item) for item in loaded]

        if documents is not None:
            if not isinstance(documents, list) or not documents:
                raise ValueError("payload.documents must be a non-empty list of documents")
            if not all(isinstance(item, dict) for item in documents):
                raise ValueError("Every item in payload.documents must be an object")
            return "batch", "documents", [cls._build_raw_ingest_document(item) for item in documents]

        return "single", "payload", [cls._build_raw_ingest_document(payload)]

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        flattener = await self._build_flattener_for_context(ctx)
        raw_doc = self._build_raw_ingest_document(payload)
        base_doc, search_doc = flattener.transform_composition(raw_doc)

        return TransformResult(base=base_doc, search=search_doc, meta={})

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        flattener = await self._build_flattener_for_context(ctx)
        mode, source, raw_docs = self._expand_ingest_payload(payload)

        transformed: list[TransformResult] = []
        for raw_doc in raw_docs:
            base_doc, search_doc = flattener.transform_composition(raw_doc)
            transformed.append(TransformResult(base=base_doc, search=search_doc, meta={}))

        storage = (ctx.adapters or {}).get("storage")
        cfg = ctx.config or {}
        comp_name = cfg.get("collections", {}).get("compositions", {}).get("name")
        search_cfg = cfg.get("collections", {}).get("search", {}) or {}
        search_enabled = search_cfg.get("enabled", True)
        search_name = search_cfg.get("name")

        inserted = {}
        inserted_counts = {"base": 0, "search": 0}
        base_docs = [item.base for item in transformed if item.base]
        search_docs = [item.search for item in transformed if item.search]

        if storage and comp_name and base_docs:
            if len(base_docs) == 1:
                await storage.insert_one(comp_name, base_docs[0])
            else:
                insert_many = getattr(storage, "insert_many", None)
                if callable(insert_many):
                    await insert_many(comp_name, base_docs)
                else:
                    for doc in base_docs:
                        await storage.insert_one(comp_name, doc)
            inserted["base"] = comp_name
            inserted_counts["base"] = len(base_docs)
        if storage and search_enabled and search_name and search_docs:
            if len(search_docs) == 1:
                await storage.insert_one(search_name, search_docs[0])
            else:
                insert_many = getattr(storage, "insert_many", None)
                if callable(insert_many):
                    await insert_many(search_name, search_docs)
                else:
                    for doc in search_docs:
                        await storage.insert_one(search_name, doc)
            inserted["search"] = search_name
            inserted_counts["search"] = len(search_docs)
        flush_codes = getattr(flattener, "flush_codes_to_db", None)
        if callable(flush_codes):
            maybe_coro = flush_codes()
            if inspect.isawaitable(maybe_coro):
                await maybe_coro
        if mode == "single":
            tf = transformed[0]
            return {"inserted": inserted, "base": tf.base, "search": tf.search}
        return {
            "mode": mode,
            "source": source,
            "processed": len(raw_docs),
            "generated": {"base": len(base_docs), "search": len(search_docs)},
            "inserted": inserted,
            "inserted_counts": inserted_counts,
        }

    async def reverse_transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reverse a flattened base document back to a nested composition (best effort).
        """
        from kehrnel.engine.strategies.openehr.rps_dual.ingest.unflattener import CompositionUnflattener

        cfg = ctx.config or {}

        # Normalize configs using new structure
        strategy_cfg = normalize_config(cfg)
        # For reverse transform, role should default to "secondary" (read-only)
        bulk_cfg = normalize_bulk_config({"role": "secondary", **(ctx.meta or {}).get("bulk_config", {})})

        # Build config for unflattener
        flattener_config = build_flattener_config(strategy_cfg, bulk_cfg)
        coding_cfg = build_coding_opts(strategy_cfg)

        unflattener = await CompositionUnflattener.create(
            db=getattr((ctx.adapters or {}).get("storage"), "db", None),
            config=flattener_config,
            mappings_path=str(Path(__file__).parent / "ingest" / "config" / "flattener_mappings_f.jsonc"),
            mappings_content=(ctx.meta or {}).get("mappings") if ctx and ctx.meta else None,
            coding_opts=coding_cfg,
        )
        base_doc = payload.get("base") if isinstance(payload, dict) else payload
        if not isinstance(base_doc, dict):
            raise ValueError("Payload must include flattened base document under 'base'")
        return {"composition": unflattener.unflatten(base_doc)}

    async def compile_query(self, ctx: StrategyContext, domain: str, query: Dict[str, Any]) -> QueryPlan:
        debug = False
        query_payload = query
        cfg_model = normalize_config(ctx.config)
        explain_domain = (getattr(ctx.manifest, "domain", None) or domain or "").lower() or None
        shortcuts_res = await get_shortcuts(ctx)
        codes_res = await get_codes(ctx)
        warnings = []
        if shortcuts_res.get("missing"):
            warnings.append({"code": "dict_missing_shortcuts", "message": "Shortcuts dictionary missing", "details": {"source": shortcuts_res.get("source")}})
        if codes_res.get("missing"):
            warnings.append({"code": "dict_missing_codes", "message": "Codes dictionary missing", "details": {"source": codes_res.get("source")}})
        runtime_strategy = build_runtime_strategy(cfg_model)
        storage = (ctx.adapters or {}).get("storage") if ctx else None
        motor_db = getattr(storage, "db", None) if storage else None

        def _execution_contract(stage_name: str, schema_cfgs: Dict[str, Any]) -> tuple[str, str | None]:
            if stage_name == "$search":
                return "text_search_dual", (schema_cfgs.get("search") or {}).get("collection")
            return "mongo_pipeline", (schema_cfgs.get("composition") or {}).get("collection")

        if isinstance(query, dict):
            debug = bool(query.get("debug"))
            raw_aql = query.get("raw_aql")
            if isinstance(raw_aql, str) and raw_aql.strip():
                params = query.get("params") or query.get("parameters") or {}
                try:
                    raw_ast = AQLToASTParser(raw_aql).parse()
                except Exception as exc:
                    raise KehrnelError(
                        code="INVALID_AQL",
                        status=400,
                        message=f"AQL parsing failed: {exc}",
                        details={"aql": raw_aql if debug else None},
                    ) from exc

                missing_params: set[str] = set()
                ast_doc = _bind_query_params(deepcopy(raw_ast), params, missing_params)
                if missing_params:
                    raise KehrnelError(
                        code="MISSING_QUERY_PARAMETERS",
                        status=400,
                        message="This query requires parameter values before it can be compiled or executed.",
                        details={
                            "missingParameters": sorted(missing_params),
                            "mode": "raw_aql_strategy",
                        },
                    )

                engine, pipeline, stage0, schema_cfgs, ast_doc, builder_info = await build_query_pipeline_from_ast(
                    ast_doc,
                    cfg_model,
                    db=motor_db,
                    shortcut_map=shortcuts_res.get("items") or {},
                    strategy=runtime_strategy,
                    raw_cfg=ctx.config if isinstance(ctx.config, dict) else None,
                )
                scope = builder_info.get("scope") or "patient"
                if scope == "cross_patient":
                    post_match = [stage for stage in pipeline[1:] if "$match" in stage]
                    if post_match:
                        warnings.append({"code": "partial_pushdown", "message": "Some predicates evaluated after $search", "details": {"post_match_stages": len(post_match)}})
                execution_engine, collection = _execution_contract(stage0, schema_cfgs)
                plan_dict = {
                    "engine": execution_engine,
                    "pipeline": pipeline,
                    "scope": scope,
                    "collection": collection,
                }
                plan_dict["dicts"] = {
                    "codes": {"source": codes_res.get("source"), "missing": codes_res.get("missing", False)},
                    "shortcuts": {"source": shortcuts_res.get("source"), "missing": shortcuts_res.get("missing", False)},
                }
                plan_dict.setdefault("warnings", [])
                plan_dict["warnings"].extend(warnings)
                explain = {
                    "dicts": plan_dict["dicts"],
                    "warnings": plan_dict["warnings"],
                    "engine": "query_engine",
                    "stage0": stage0,
                    "schema": schema_cfgs,
                    "ast": ast_doc if debug else None,
                    "builder": {**builder_info, "compiler_engine": engine, "execution_engine": execution_engine},
                    "rawAql": raw_aql if debug else None,
                    "parameters": params if debug else None,
                    "mode": "raw_aql_strategy",
                }
                explain = enrich_explain(
                    explain,
                    ctx,
                    domain=explain_domain or "openehr",
                    engine="query_engine",
                    scope=scope,
                )
                plan_dict["explain"] = explain
                return QueryPlan(engine=plan_dict["engine"], plan=plan_dict, explain=explain)
            # The IR dataclass does not accept extra keys like "debug".
            query_payload = {k: v for k, v in query.items() if k != "debug"}

        ir = AqlQueryIR(**query_payload) if not isinstance(query, AqlQueryIR) else query
        # Build query pipeline using the query compiler
        engine, pipeline, stage0, schema_cfgs, ast_doc, builder_info = await build_query_pipeline(
            ir,
            cfg_model,
            db=motor_db,
            shortcut_map=shortcuts_res.get("items") or {},
            strategy=runtime_strategy,
            raw_cfg=ctx.config if isinstance(ctx.config, dict) else None,
        )
        if ir.scope == "cross_patient":
            post_match = [stage for stage in pipeline[1:] if "$match" in stage]
            if post_match:
                warnings.append({"code": "partial_pushdown", "message": "Some predicates evaluated after $search", "details": {"post_match_stages": len(post_match)}})
        execution_engine, collection = _execution_contract(stage0, schema_cfgs)
        plan_dict = {
            "engine": execution_engine,
            "pipeline": pipeline,
            "collection": collection,
        }
        plan_dict["dicts"] = {
            "codes": {"source": codes_res.get("source"), "missing": codes_res.get("missing", False)},
            "shortcuts": {"source": shortcuts_res.get("source"), "missing": shortcuts_res.get("missing", False)},
        }
        plan_dict.setdefault("warnings", [])
        plan_dict["warnings"].extend(warnings)
        explain = {
            "dicts": plan_dict["dicts"],
            "warnings": plan_dict["warnings"],
            "engine": "query_engine",
            "stage0": stage0,
            "schema": schema_cfgs,
            "ast": ast_doc if debug else None,
            "builder": {**builder_info, "compiler_engine": engine, "execution_engine": execution_engine},
        }
        explain = enrich_explain(
            explain,
            ctx,
            domain=explain_domain or "openehr",
            engine="query_engine",
            scope=ir.scope,
        )
        plan_dict["explain"] = explain
        return QueryPlan(engine=plan_dict["engine"], plan=plan_dict, explain=explain)

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        return await execute_query_plan(ctx, plan)

    async def run_op(self, ctx: StrategyContext, op: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        op_lower = op.lower()
        cfg = ctx.config
        strategy_cfg = normalize_config(cfg)
        adapters = ctx.adapters or {}
        index_admin = adapters.get("index_admin")
        storage = adapters.get("storage")
        atlas = adapters.get("atlas_search") or adapters.get("text_search")

        if op_lower == "ensure_dictionaries":
            created = []
            warnings = []
            dict_name = strategy_cfg.collections.codes.name
            shortcuts_name = strategy_cfg.collections.shortcuts.name
            dict_seed = strategy_cfg.collections.codes.seed
            shortcuts_seed = strategy_cfg.collections.shortcuts.seed
            modes = _dictionary_bootstrap_modes(strategy_cfg, payload)
            if index_admin:
                if dict_name and modes["codes"] != "none":
                    await index_admin.ensure_collection(dict_name)
                    created.append(dict_name)
                if shortcuts_name and modes["shortcuts"] != "none":
                    await index_admin.ensure_collection(shortcuts_name)
                    created.append(shortcuts_name)
            else:
                warnings.append("index_admin adapter not available; cannot ensure collections")
            if not storage:
                return {
                    "ok": True,
                    "created": created,
                    "seeded": {"codes": 0, "shortcuts": 0},
                    "modes": modes,
                    "warnings": warnings + ["storage adapter not available; cannot seed dictionaries"],
                }

            # Prefer upsert via raw Motor DB if present (seed ops should be idempotent).
            motor_db = getattr(storage, "db", None)

            async def _upsert(coll: str, doc: Dict[str, Any]) -> None:
                if not coll or not isinstance(doc, dict) or not doc.get("_id"):
                    return
                if motor_db is not None:
                    await motor_db[coll].replace_one({"_id": doc["_id"]}, doc, upsert=True)
                    return
                existing = await storage.find_one(coll, {"_id": doc["_id"]})
                if not existing:
                    await storage.insert_one(coll, doc)

            async def _seed_from_uri(coll: str, seed_uri: Any) -> int:
                if not coll or not seed_uri:
                    return 0
                try:
                    payload = await resolve_uri_async(seed_uri, motor_db, self._strategy_root_dir())
                except Exception as exc:
                    warnings.append(f"failed to resolve seed {seed_uri}: {exc}")
                    return 0
                if isinstance(payload, list):
                    count = 0
                    for doc in payload:
                        if isinstance(doc, dict) and doc.get("_id"):
                            await _upsert(coll, doc)
                            count += 1
                    return count
                if isinstance(payload, dict) and payload.get("_id"):
                    await _upsert(coll, payload)
                    return 1
                warnings.append(f"seed {seed_uri} did not resolve to a document or list of documents")
                return 0

            seeded = {
                "codes": await _seed_from_uri(dict_name, dict_seed) if modes["codes"] == "seed" else 0,
                "shortcuts": await _seed_from_uri(shortcuts_name, shortcuts_seed) if modes["shortcuts"] == "seed" else 0,
            }

            # If seeds are missing/unresolvable, ensure minimal docs exist with expected IDs.
            # Codes dictionary default id used by flattener is "ar_code".
            if dict_name and modes["codes"] == "seed" and seeded["codes"] == 0:
                await _upsert(dict_name, {"_id": "ar_code", "at": {}})
            if shortcuts_name and modes["shortcuts"] == "seed" and seeded["shortcuts"] == 0:
                await _upsert(shortcuts_name, {"_id": "shortcuts", "keys": {}, "values": {}})

            return {"ok": True, "created": created, "seeded": seeded, "modes": modes, "warnings": warnings}

        if op_lower == "rebuild_codes":
            dict_name = strategy_cfg.collections.codes.name
            dict_seed = strategy_cfg.collections.codes.seed
            if not storage or not dict_name:
                return {"ok": False, "warnings": ["storage adapter not available or dictionary not configured"]}
            motor_db = getattr(storage, "db", None)
            warnings = []
            try:
                seed_payload = await resolve_uri_async(dict_seed, motor_db, self._strategy_root_dir()) if dict_seed else None
            except Exception as exc:
                return {"ok": False, "warnings": [f"failed to resolve codes seed: {exc}"]}
            if motor_db is None:
                return {"ok": False, "warnings": ["raw motor db not available; cannot rebuild codes idempotently"]}
            updated = 0
            if isinstance(seed_payload, list):
                for doc in seed_payload:
                    if isinstance(doc, dict) and doc.get("_id"):
                        await motor_db[dict_name].replace_one({"_id": doc["_id"]}, doc, upsert=True)
                        updated += 1
            elif isinstance(seed_payload, dict) and seed_payload.get("_id"):
                await motor_db[dict_name].replace_one({"_id": seed_payload["_id"]}, seed_payload, upsert=True)
                updated = 1
            else:
                warnings.append("codes seed did not resolve to a document or list of documents")

            cache = (ctx.meta or {}).get("dict_cache") if ctx else None
            if cache is not None:
                cache.pop("codes", None)
            return {"ok": True, "updated": updated, "warnings": warnings}

        if op_lower == "rebuild_shortcuts":
            shortcuts_name = strategy_cfg.collections.shortcuts.name
            shortcuts_seed = strategy_cfg.collections.shortcuts.seed
            if not storage:
                return {"ok": False, "warnings": ["storage adapter not available"]}
            motor_db = getattr(storage, "db", None)
            if motor_db is None:
                return {"ok": False, "warnings": ["raw motor db not available; cannot rebuild shortcuts idempotently"]}
            warnings = []
            try:
                seed_payload = await resolve_uri_async(shortcuts_seed, motor_db, self._strategy_root_dir()) if shortcuts_seed else None
            except Exception as exc:
                return {"ok": False, "warnings": [f"failed to resolve shortcuts seed: {exc}"]}
            if isinstance(seed_payload, dict) and seed_payload.get("_id"):
                await motor_db[shortcuts_name].replace_one({"_id": seed_payload["_id"]}, seed_payload, upsert=True)
            else:
                warnings.append("shortcuts seed did not resolve to a document; creating empty shortcuts doc")
                await motor_db[shortcuts_name].replace_one({"_id": "shortcuts"}, {"_id": "shortcuts", "keys": {}, "values": {}}, upsert=True)
            cache = (ctx.meta or {}).get("dict_cache") if ctx else None
            if cache is not None:
                cache.pop("shortcuts", None)
            return {"ok": True, "updated": 1, "warnings": warnings}

        if op_lower == "ensure_atlas_search_index":
            search_coll = strategy_cfg.collections.search
            atlas_idx = search_coll.atlasIndex
            index_name = (payload.get("index_name") if isinstance(payload, dict) else None) or (atlas_idx.name if atlas_idx else None)
            definition = await self._resolve_search_index_definition(ctx, strategy_cfg)
            if atlas and search_coll.name and index_name:
                res = await atlas.ensure_search_index(search_coll.name, index_name, definition)
                return {"ok": True, "result": res}
            return {"ok": False, "warnings": ["atlas_search adapter not available or search collection/index not configured"]}

        if op_lower == "build_search_index_definition":
            mappings_content = await self._resolve_mappings_content(ctx, strategy_cfg)
            result = await build_search_index_definition_from_mappings(
                strategy_cfg,
                mappings_content,
                db=getattr(storage, "db", None) if storage else None,
                shortcuts=await get_shortcuts(ctx),
                include_stored_source=bool(payload.get("include_stored_source", True)),
            )
            return {
                "ok": True,
                "definition": result.get("definition"),
                "metadata": result.get("metadata"),
                "warnings": result.get("warnings", []),
            }

        if op_lower == "rebuild_slim_search_collection":
            if not storage:
                return {"ok": False, "warnings": ["storage adapter not available"]}
            comp_coll = strategy_cfg.collections.compositions.name
            search_coll_name = strategy_cfg.collections.search.name
            if not comp_coll or not search_coll_name:
                return {"ok": False, "warnings": ["collections not configured"]}
            batch_size = int(payload.get("batch_size", 100)) if payload else 100
            docs = await storage.aggregate(comp_coll, [{"$limit": batch_size}])
            # Get bundle reference from search collection seed if available
            bundle_id = None
            seed_ref = strategy_cfg.collections.search.atlasIndex
            if seed_ref and isinstance(seed_ref.definition, str):
                bundle_id = seed_ref.definition
            bundle = await self._maybe_load_bundle(bundle_id, ctx)
            inserted = 0
            for doc in docs:
                search_doc = self._apply_bundle_to_composition(bundle, doc, strategy_cfg)
                await storage.insert_one(search_coll_name, search_doc)
                inserted += 1
            warnings = []
            atlas_idx = strategy_cfg.collections.search.atlasIndex
            if atlas and atlas_idx and atlas_idx.name:
                definition = await self._resolve_search_index_definition(ctx, strategy_cfg)
                res = await atlas.ensure_search_index(search_coll_name, atlas_idx.name, definition)
                warnings.extend(res.get("warnings", []))
            return {"ok": True, "processed": len(docs), "inserted": inserted, "warnings": warnings}

        if op_lower == "fetch_native_composition":
            if not storage:
                raise KehrnelError(
                    code="STORAGE_NOT_AVAILABLE",
                    status=503,
                    message="storage adapter not available for native composition lookup",
                )

            comp_coll = strategy_cfg.collections.compositions.name
            if not comp_coll:
                raise KehrnelError(
                    code="COLLECTION_NOT_CONFIGURED",
                    status=400,
                    message="compositions collection is not configured",
                )

            uid = str(payload.get("uid") or payload.get("composition_uid") or "").strip()
            if not uid:
                raise KehrnelError(
                    code="INVALID_INPUT",
                    status=400,
                    message="uid is required",
                )

            uid_candidates = [uid]
            base_uid = uid.split("::", 1)[0].strip()
            if base_uid and base_uid not in uid_candidates:
                uid_candidates.append(base_uid)

            uid_match = []
            for candidate in uid_candidates:
                uid_match.extend([
                    {"version": candidate},
                    {"uid": candidate},
                    {"_id": candidate},
                    {"cn.data.uid.v": candidate},
                    {"cn.data.uid.value": candidate},
                ])
                try:
                    uid_match.append({"_id": uuid.UUID(candidate)})
                except Exception:
                    pass

            query: Dict[str, Any] = {"$or": uid_match}

            ehr_id = payload.get("ehr_id")
            if ehr_id is not None and str(ehr_id).strip():
                ehr_candidates = [ehr_id]
                try:
                    ehr_candidates.append(uuid.UUID(str(ehr_id)))
                except Exception:
                    pass
                query = {
                    "$and": [
                        {"$or": uid_match},
                        {"$or": [{"ehr_id": candidate} for candidate in ehr_candidates]},
                    ]
                }

            doc = await storage.find_one(comp_coll, query)
            if not isinstance(doc, dict):
                raise KehrnelError(
                    code="COMPOSITION_NOT_FOUND",
                    status=404,
                    message=f"native composition not found for uid '{uid}'",
                )

            return {
                "ok": True,
                "composition": json.loads(json.dumps(doc, default=str)),
            }

        if op_lower == "list_native_ehrs":
            if not storage:
                raise KehrnelError(
                    code="STORAGE_NOT_AVAILABLE",
                    status=503,
                    message="storage adapter not available for native EHR lookup",
                )

            comp_coll = strategy_cfg.collections.compositions.name
            if not comp_coll:
                raise KehrnelError(
                    code="COLLECTION_NOT_CONFIGURED",
                    status=400,
                    message="compositions collection is not configured",
                )

            limit = max(1, min(int(payload.get("limit", 1000) or 1000), 5000))
            records = await storage.aggregate(
                comp_coll,
                [
                    {"$match": {"ehr_id": {"$exists": True, "$ne": None}}},
                    {
                        "$group": {
                            "_id": "$ehr_id",
                            "time_created": {"$max": "$creation_date"},
                        }
                    },
                    {"$sort": {"time_created": -1, "_id": 1}},
                    {"$limit": limit},
                    {"$project": {"_id": 0, "ehr_id": "$_id", "time_created": 1}},
                ],
            )

            return {
                "ok": True,
                "records": json.loads(json.dumps(records, default=str)),
            }

        if op_lower == "list_native_compositions":
            if not storage:
                raise KehrnelError(
                    code="STORAGE_NOT_AVAILABLE",
                    status=503,
                    message="storage adapter not available for native composition lookup",
                )

            comp_coll = strategy_cfg.collections.compositions.name
            if not comp_coll:
                raise KehrnelError(
                    code="COLLECTION_NOT_CONFIGURED",
                    status=400,
                    message="compositions collection is not configured",
                )

            ehr_id = payload.get("ehr_id")
            if ehr_id is None or not str(ehr_id).strip():
                raise KehrnelError(
                    code="INVALID_INPUT",
                    status=400,
                    message="ehr_id is required",
                )

            limit = max(1, min(int(payload.get("limit", 500) or 500), 2000))
            ehr_candidates = [ehr_id]
            try:
                ehr_candidates.append(uuid.UUID(str(ehr_id)))
            except Exception:
                pass

            records = await storage.aggregate(
                comp_coll,
                [
                    {"$match": {"$or": [{"ehr_id": candidate} for candidate in ehr_candidates]}},
                    {
                        "$project": {
                            "_id": 0,
                            "uid": {"$ifNull": ["$version", {"$toString": "$_id"}]},
                            "templateId": {"$ifNull": ["$template", "$template_id"]},
                            "creation_date": "$creation_date",
                            "name": {
                                "$let": {
                                    "vars": {"root": {"$first": "$cn"}},
                                    "in": {
                                        "$ifNull": [
                                            "$$root.data.n.v",
                                            {"$ifNull": ["$$root.data.n.value", "$template"]},
                                        ]
                                    },
                                }
                            },
                        }
                    },
                    {"$sort": {"creation_date": -1, "uid": 1}},
                    {"$limit": limit},
                ],
            )

            return {
                "ok": True,
                "records": json.loads(json.dumps(records, default=str)),
            }

        if op_lower == "synthetic_generate_batch":
            if not storage:
                raise KehrnelError(
                    code="STORAGE_NOT_AVAILABLE",
                    status=503,
                    message="storage adapter not available for synthetic generation",
                )
            patient_count = int(payload.get("patient_count") or payload.get("patients") or 0)
            if patient_count <= 0:
                raise KehrnelError(
                    code="INVALID_INPUT",
                    status=400,
                    message="patient_count must be > 0",
                )
            source_collection = payload.get("source_collection") or "samples"
            source_database = payload.get("source_database") or payload.get("source_db")
            dry_run = bool(payload.get("dry_run", False))
            plan_only = bool(payload.get("plan_only", False))
            generation_mode = str(payload.get("generation_mode") or "auto").strip().lower()
            store_canonical = bool(payload.get("store_canonical", False))
            canonical_collection = str(payload.get("canonical_collection") or "compositions_canonical_synthetic")
            write_batch_size = max(10, int(payload.get("write_batch_size", 250) or 250))
            comp_collection = strategy_cfg.collections.compositions.name
            search_collection = strategy_cfg.collections.search.name
            search_enabled = bool(strategy_cfg.collections.search.enabled and search_collection)
            flattener = await self._build_flattener_for_context(ctx)

            progress_cb = (ctx.meta or {}).get("progress_cb")
            should_cancel = (ctx.meta or {}).get("should_cancel")

            models = payload.get("models")
            templates = payload.get("templates")
            model_source = payload.get("model_source") or {}
            source_templates = payload.get("source_templates")
            source_sample_size = int(payload.get("source_sample_size", 200) or 200)
            source_min_per_patient = int(payload.get("source_min_per_patient", 1) or 1)
            source_max_per_patient = int(payload.get("source_max_per_patient", source_min_per_patient) or source_min_per_patient)
            source_filter = payload.get("source_filter")
            template_field = strategy_cfg.fields.document.tid

            model_specs: list[Dict[str, Any]] = []
            if isinstance(models, list) and models:
                model_specs = await resolve_model_specs(
                    storage,
                    model_source=model_source if isinstance(model_source, dict) else {},
                    requested_models=models,
                    domain=(getattr(ctx.manifest, "domain", "") or "openEHR"),
                    strategy_id=getattr(ctx.manifest, "id", None),
                )
            elif isinstance(templates, list) and templates:
                for entry in templates:
                    if not isinstance(entry, dict):
                        raise KehrnelError(code="INVALID_INPUT", status=400, message="each template entry must be an object")
                    template_id = str(entry.get("template_id") or entry.get("templateId") or "").strip()
                    if not template_id:
                        raise KehrnelError(code="INVALID_INPUT", status=400, message="template_id is required in each template entry")
                    min_per = int(entry.get("min_per_patient", entry.get("min", 1)))
                    max_per = int(entry.get("max_per_patient", entry.get("max", min_per)))
                    if min_per < 0 or max_per < min_per:
                        raise KehrnelError(
                            code="INVALID_INPUT",
                            status=400,
                            message=f"invalid range for template {template_id}: min={min_per}, max={max_per}",
                        )
                    sample_size = int(entry.get("sample_pool_size", 25) or 25)
                    model_specs.append(
                        {
                            "model_id": template_id,
                            "template_id": template_id,
                            "min": min_per,
                            "max": max_per,
                            "weight": float(entry.get("weight", 1.0)),
                            "sample_size": max(1, sample_size),
                            "catalog": {},
                        }
                    )
            elif str(generation_mode or "").lower() in ("from_source", "source", "auto"):
                # Source-instance mode: derive templates from existing source collection.
                discovered_template_ids: list[str] = []
                if isinstance(source_templates, list) and source_templates:
                    discovered_template_ids = [str(t).strip() for t in source_templates if str(t).strip()]
                else:
                    match_stage: Dict[str, Any] = {}
                    if isinstance(source_filter, dict) and source_filter:
                        match_stage = source_filter
                    pipeline = (
                        [{"$match": match_stage}] if match_stage else []
                    ) + [
                        {
                            "$project": {
                                "template_id": {
                                    "$ifNull": [
                                        "$template_id",
                                        {
                                            "$ifNull": [
                                                "$template_name",
                                                {
                                                    "$ifNull": [
                                                        f"${template_field}",
                                                        {
                                                            "$ifNull": [
                                                                "$canonicalJSON.archetype_details.template_id.value",
                                                                "$archetype_details.template_id.value",
                                                            ]
                                                        },
                                                    ]
                                                },
                                            ]
                                        },
                                    ]
                                }
                            }
                        },
                        {"$match": {"template_id": {"$type": "string", "$ne": ""}}},
                        {"$group": {"_id": "$template_id"}},
                        {"$limit": max(1, source_sample_size)},
                    ]
                    rows = await storage.aggregate(source_collection, pipeline) if not source_database else await _aggregate_from_database(
                        storage=storage,
                        database_name=str(source_database),
                        collection_name=source_collection,
                        pipeline=pipeline,
                    )
                    discovered_template_ids = [str(r.get("_id")).strip() for r in rows if str(r.get("_id") or "").strip()]

                if not discovered_template_ids:
                    raise KehrnelError(
                        code="SOURCE_TEMPLATE_NOT_FOUND",
                        status=404,
                        message=f"No templates discovered in source {source_database + '.' if source_database else ''}{source_collection}",
                    )

                if source_min_per_patient < 0 or source_max_per_patient < source_min_per_patient:
                    raise KehrnelError(
                        code="INVALID_INPUT",
                        status=400,
                        message=f"invalid source range: min={source_min_per_patient}, max={source_max_per_patient}",
                    )

                for template_id in discovered_template_ids:
                    model_specs.append(
                        {
                            "model_id": template_id,
                            "template_id": template_id,
                            "min": source_min_per_patient,
                            "max": source_max_per_patient,
                            "weight": 1.0,
                            "sample_size": max(1, source_sample_size),
                            "catalog": {},
                        }
                    )
            else:
                raise KehrnelError(
                    code="INVALID_INPUT",
                    status=400,
                    message="Provide payload.models, payload.templates, or source-based mode (generation_mode=from_source).",
                )

            links = await resolve_links(
                storage,
                model_source=model_source if isinstance(model_source, dict) else {},
                model_ids=[str(m.get("model_id")) for m in model_specs],
                explicit_links=payload.get("links") if "links" in payload else None,
            )

            template_specs = []
            samples_by_template: Dict[str, list[Dict[str, Any]]] = {}
            template_generators: Dict[str, Any] = {}
            estimated_docs = 0
            for entry in model_specs:
                catalog = entry.get("catalog") or {}
                template_id = str(
                    entry.get("template_id")
                    or catalog.get("template_id")
                    or catalog.get("templateId")
                    or ((catalog.get("template") or {}).get("id") if isinstance(catalog.get("template"), dict) else None)
                    or entry.get("model_id")
                    or ""
                ).strip()
                if not template_id:
                    raise KehrnelError(code="INVALID_INPUT", status=400, message=f"template_id unresolved for model_id={entry.get('model_id')}")
                min_per = int(entry.get("min", 1))
                max_per = int(entry.get("max", min_per))
                sample_size = int(entry.get("sample_size", 25) or 25)
                estimated_docs += patient_count * ((min_per + max_per) // 2)
                template_specs.append(
                    {
                        "model_id": str(entry.get("model_id") or template_id),
                        "template_id": template_id,
                        "min": min_per,
                        "max": max_per,
                        "sample_size": max(1, sample_size),
                        "catalog": catalog,
                    }
                )

            if estimated_docs <= 0:
                estimated_docs = patient_count

            for spec in template_specs:
                template_id = spec["template_id"]
                sample_docs: list[Dict[str, Any]] = []
                prefer_source = generation_mode in ("auto", "from_source", "source")
                prefer_models = generation_mode in ("auto", "from_models", "models")

                if prefer_source:
                    match_conditions = [
                        {"template_id": template_id},
                        {"template_name": template_id},
                        {template_field: template_id},
                        {"canonicalJSON.archetype_details.template_id.value": template_id},
                        {"archetype_details.template_id.value": template_id},
                    ]
                    if isinstance(source_filter, dict) and source_filter:
                        source_match: Dict[str, Any] = {"$and": [source_filter, {"$or": match_conditions}]}
                    else:
                        source_match = {"$or": match_conditions}
                    sample_docs = await storage.aggregate(
                        source_collection,
                        [
                            {"$match": source_match},
                            {"$sample": {"size": spec["sample_size"]}},
                        ],
                    ) if not source_database else await _aggregate_from_database(
                        storage=storage,
                        database_name=str(source_database),
                        collection_name=source_collection,
                        pipeline=[
                            {"$match": source_match},
                            {"$sample": {"size": spec["sample_size"]}},
                        ],
                    )

                if sample_docs:
                    samples_by_template[template_id] = sample_docs
                    continue

                if prefer_models:
                    model_doc = spec.get("catalog") or {}
                    gen = _build_canonical_generator_from_model(model_doc)
                    if gen is not None:
                        template_generators[template_id] = gen
                        continue

                raise KehrnelError(
                    code="SOURCE_TEMPLATE_NOT_FOUND",
                    status=404,
                    message=(
                        f"No generation source for template_id={template_id}. "
                        f"Tried source {source_database + '.' if source_database else ''}{source_collection} "
                        f"and model definition in model_source."
                    ),
                )

            estimated_base_bytes = 0
            estimated_search_bytes = 0
            estimated_canonical_bytes = 0
            # Suppress empty search documents: only write/estimate search docs when
            # the flattener produced at least one search node (i.e., analytics rules exist).
            sn_field = getattr(getattr(cfg, "fields", None), "document", None)
            sn_field = getattr(sn_field, "sn", None) or "sn"

            def _has_search_nodes(doc: Any) -> bool:
                if not isinstance(doc, dict):
                    return False
                nodes = doc.get(sn_field)
                if isinstance(nodes, list):
                    return len(nodes) > 0
                # Backward/alternative field names used by some runtimes.
                for candidate in ("sn", "nodes"):
                    nodes = doc.get(candidate)
                    if isinstance(nodes, list):
                        return len(nodes) > 0
                return False

            for spec in template_specs:
                template_id = spec["template_id"]
                canonical_probe = None
                if template_id in samples_by_template:
                    canonical_probe = _extract_canonical_composition(samples_by_template[template_id][0])
                elif template_id in template_generators:
                    canonical_probe = template_generators[template_id]()
                if not canonical_probe:
                    continue
                probe_doc = {
                    "_id": str(uuid.uuid4()),
                    "ehr_id": f"synthetic-ehr-{uuid.uuid4()}",
                    "template_id": template_id,
                    "canonicalJSON": canonical_probe,
                }
                probe_base, probe_search = flattener.transform_composition(probe_doc)
                avg_docs_for_template = max(1, patient_count * ((spec["min"] + spec["max"]) // 2))
                if store_canonical and canonical_probe:
                    estimated_canonical_bytes += _json_size_bytes(probe_doc) * avg_docs_for_template
                if probe_base:
                    estimated_base_bytes += _json_size_bytes(probe_base) * avg_docs_for_template
                if search_enabled and probe_search and _has_search_nodes(probe_search):
                    estimated_search_bytes += _json_size_bytes(probe_search) * avg_docs_for_template

            if plan_only:
                return {
                    "ok": True,
                    "plan_only": True,
                    "patients": patient_count,
                    "estimated_docs": estimated_docs,
                    "estimated_canonical_bytes": estimated_canonical_bytes,
                    "estimated_base_bytes": estimated_base_bytes,
                    "estimated_search_bytes": estimated_search_bytes,
                    "estimated_total_bytes": estimated_canonical_bytes + estimated_base_bytes + estimated_search_bytes,
                    "source_collection": source_collection,
                    "source_database": source_database,
                    "models": [{"model_id": s["model_id"], "template_id": s["template_id"], "min": s["min"], "max": s["max"]} for s in template_specs],
                    "links": links,
                }

            inserted_canonical = 0
            inserted_base = 0
            inserted_search = 0
            generated_docs = 0
            by_template: Dict[str, int] = {}
            by_model: Dict[str, int] = {}
            link_applied_count = 0
            last_progress = -1
            canonical_batch: list[Dict[str, Any]] = []
            base_batch: list[Dict[str, Any]] = []
            search_batch: list[Dict[str, Any]] = []

            async def _flush_batches(force: bool = False) -> None:
                nonlocal inserted_canonical, inserted_base, inserted_search
                if dry_run:
                    return
                if store_canonical and canonical_collection and canonical_batch and (force or len(canonical_batch) >= write_batch_size):
                    await storage.insert_many(canonical_collection, canonical_batch)
                    inserted_canonical += len(canonical_batch)
                    canonical_batch.clear()
                if comp_collection and base_batch and (force or len(base_batch) >= write_batch_size):
                    await storage.insert_many(comp_collection, base_batch)
                    inserted_base += len(base_batch)
                    base_batch.clear()
                if search_enabled and search_collection and search_batch and (force or len(search_batch) >= write_batch_size):
                    await storage.insert_many(search_collection, search_batch)
                    inserted_search += len(search_batch)
                    search_batch.clear()

            await _emit_progress(
                progress_cb,
                progress=1,
                phase="running",
                stats={
                    "patients_total": patient_count,
                    "estimated_docs": estimated_docs,
                    "patientCount": patient_count,
                    "generatedPatients": 0,
                    "generatedDocuments": 0,
                    "modelCount": len(template_specs),
                    "linksApplied": 0,
                },
            )

            for i in range(patient_count):
                if _is_canceled(should_cancel):
                    raise KehrnelError(code="JOB_CANCELED", status=499, message="Synthetic batch canceled by user")

                ehr_id = f"synthetic-ehr-{uuid.uuid4()}"
                repeats: Dict[str, int] = {}
                model_to_template: Dict[str, str] = {}
                for spec in template_specs:
                    model_id = spec["model_id"]
                    template_id = spec["template_id"]
                    repeats[model_id] = random.randint(spec["min"], spec["max"])
                    model_to_template[model_id] = template_id

                for link in links:
                    from_id = str(link.get("from") or link.get("from_model_id") or "").strip()
                    to_id = str(link.get("to") or link.get("to_model_id") or "").strip()
                    if not from_id or not to_id:
                        continue
                    if repeats.get(from_id, 0) <= 0:
                        continue
                    probability = float(link.get("probability", 1.0))
                    if random.random() > max(0.0, min(1.0, probability)):
                        continue
                    min_to = int(link.get("min_to_per_patient", 1))
                    repeats[to_id] = max(repeats.get(to_id, 0), min_to)
                    link_applied_count += 1

                for model_id, repeat in repeats.items():
                    template_id = model_to_template.get(model_id)
                    if not template_id:
                        continue
                    for _ in range(repeat):
                        if template_id in samples_by_template:
                            src_doc = random.choice(samples_by_template[template_id])
                            canonical = _extract_canonical_composition(src_doc)
                        else:
                            canonical = template_generators[template_id]()
                        if not canonical:
                            continue
                        raw_doc = {
                            "_id": str(uuid.uuid4()),
                            "ehr_id": ehr_id,
                            "template_id": template_id,
                            "canonicalJSON": canonical,
                        }
                        if not dry_run and store_canonical and canonical_collection:
                            canonical_batch.append(raw_doc)
                        transformed_base, transformed_search = flattener.transform_composition(raw_doc)
                        if not dry_run and comp_collection and transformed_base:
                            base_batch.append(transformed_base)
                        if not dry_run and search_enabled and transformed_search and _has_search_nodes(transformed_search):
                            search_batch.append(transformed_search)
                        await _flush_batches()
                        generated_docs += 1
                        by_template[template_id] = by_template.get(template_id, 0) + 1
                        by_model[model_id] = by_model.get(model_id, 0) + 1

                progress = min(99, int((generated_docs / max(1, estimated_docs)) * 100))
                if progress != last_progress:
                    await _emit_progress(
                        progress_cb,
                        progress=progress,
                        phase=f"patient {i + 1}/{patient_count}",
                        stats={
                            "patients_completed": i + 1,
                            "generated_docs": generated_docs,
                            "inserted_canonical": inserted_canonical,
                            "inserted_base": inserted_base,
                            "inserted_search": inserted_search,
                            "links_applied": link_applied_count,
                            "patientCount": patient_count,
                            "generatedPatients": i + 1,
                            "generatedDocuments": generated_docs,
                            "modelCount": len(template_specs),
                            "linksApplied": link_applied_count,
                        },
                    )
                    last_progress = progress

            await _flush_batches(force=True)
            await _emit_progress(
                progress_cb,
                progress=100,
                phase="completed",
                stats={
                    "patients_completed": patient_count,
                    "generated_docs": generated_docs,
                    "inserted_canonical": inserted_canonical,
                    "inserted_base": inserted_base,
                    "inserted_search": inserted_search,
                    "links_applied": link_applied_count,
                    "patientCount": patient_count,
                    "generatedPatients": patient_count,
                    "generatedDocuments": generated_docs,
                    "modelCount": len(template_specs),
                    "linksApplied": link_applied_count,
                },
            )
            return {
                "ok": True,
                "dry_run": dry_run,
                "plan_only": False,
                "source_collection": source_collection,
                "source_database": source_database,
                "target": {
                    "canonical": canonical_collection if store_canonical else None,
                    "compositions": comp_collection,
                    "search": search_collection if search_enabled else None,
                },
                "patients": patient_count,
                "generated_docs": generated_docs,
                "inserted_canonical": inserted_canonical,
                "inserted_base": inserted_base,
                "inserted_search": inserted_search,
                "links_applied": link_applied_count,
                "by_template": by_template,
                "by_model": by_model,
            }

        raise ValueError(f"Strategy op '{op}' not supported")

    def _apply_bundle_to_composition(self, bundle: Dict[str, Any], comp: Dict[str, Any], cfg: RPSDualConfig) -> Dict[str, Any]:
        # Get field names from config
        cn_field = cfg.fields.document.cn
        sn_field = cfg.fields.document.sn
        ehr_id_field = cfg.fields.document.ehr_id
        comp_id_field = cfg.fields.document.comp_id
        template_field = cfg.fields.document.tid

        nodes = comp.get(cn_field, []) or comp.get("cn", [])
        search_nodes = []
        templates = (bundle.get("payload") or {}).get("templates") or []
        for tpl in templates:
            tid = tpl.get("templateId")
            rules = tpl.get("rules") or []
            analytics = tpl.get("analytics_fields") or []
            for node in nodes:
                p = node.get("p") or ""
                data = node.get("data") or {}
                matched = False
                if rules:
                    for rule in rules:
                        when = rule.get("when") or {}
                        chain = when.get("pathChain") or []
                        if chain and all(token in p for token in chain):
                            matched = True
                            break
                elif analytics:
                    matched = True
                if matched:
                    sn_entry = {"p": p, template_field: tid}
                    for field in (rule.get("copy") if matched and rules else []) or []:
                        if field == "p":
                            sn_entry["p"] = p
                        else:
                            sn_entry[field] = _pluck(data, field)
                    if analytics:
                        sn_entry["analytics"] = [{a.get("name"): _pluck(data, a.get("path", ""))} for a in analytics]
                    search_nodes.append(sn_entry)
        return {
            sn_field: search_nodes,
            ehr_id_field: comp.get(ehr_id_field, comp.get("ehr_id")),
            comp_id_field: comp.get(comp_id_field, comp.get("_id")),
            template_field: templates[0].get("templateId") if templates else comp.get(template_field, comp.get("tid")),
        }


    async def _maybe_load_bundle(self, bundle_id: str | None, ctx: StrategyContext) -> Dict[str, Any]:
        if not bundle_id:
            return {}
        store = (ctx.meta or {}).get("bundle_store") if ctx else None
        if store:
            return store.get_bundle(bundle_id)
        path = self._strategy_root_dir() / "bundles" / f"{bundle_id}.json"
        if path.exists():
            import json as _json
            return _json.loads(path.read_text(encoding="utf-8"))
        raise KehrnelError(code="BUNDLE_NOT_FOUND", status=404, message=f"Bundle not found: {bundle_id}")


def _pluck(data: Dict[str, Any], path: str) -> Any:
    if not path:
        return None
    cur: Any = data
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur.get(part)
        else:
            return None
    return cur


async def _aggregate_from_database(
    *,
    storage: Any,
    database_name: str,
    collection_name: str,
    pipeline: list[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    db = getattr(storage, "db", None)
    client = getattr(db, "client", None) if db is not None else None
    if client is None:
        raise KehrnelError(code="STORAGE_NOT_AVAILABLE", status=503, message="Mongo client not available")
    cursor = client[str(database_name)][collection_name].aggregate(pipeline)
    return [doc async for doc in cursor]


def _extract_canonical_composition(doc: Dict[str, Any]) -> Dict[str, Any] | None:
    if not isinstance(doc, dict):
        return None
    if isinstance(doc.get("canonicalJSON"), dict):
        return copy.deepcopy(doc["canonicalJSON"])
    if isinstance(doc.get("composition"), dict):
        return copy.deepcopy(doc["composition"])
    if doc.get("_type") == "COMPOSITION":
        return copy.deepcopy(doc)
    return None


def _build_canonical_generator_from_model(model_doc: Dict[str, Any]):
    if not isinstance(model_doc, dict):
        return None
    xml_payload = (
        (((model_doc.get("domainData") or {}).get("source") or {}).get("xml") if isinstance(model_doc.get("domainData"), dict) else None)
        or model_doc.get("opt")
        or model_doc.get("template")
    )
    if not isinstance(xml_payload, str) or "<" not in xml_payload:
        return None

    with tempfile.NamedTemporaryFile(suffix=".opt", delete=False, mode="w", encoding="utf-8") as tmp:
        tmp.write(xml_payload)
        tmp_path = Path(tmp.name)

    parser = TemplateParser(tmp_path)
    try:
        tmp_path.unlink(missing_ok=True)
    except Exception:
        pass
    gen = kehrnelGenerator(parser)

    def _create():
        return gen.generate_random()

    return _create


def _json_size_bytes(obj: Dict[str, Any]) -> int:
    try:
        return len(json.dumps(obj, ensure_ascii=False).encode("utf-8"))
    except Exception:
        return 0


async def _emit_progress(cb: Any, *, progress: int | None = None, phase: str | None = None, stats: Dict[str, Any] | None = None) -> None:
    if not cb:
        return
    result = cb(progress=progress, phase=phase, stats=stats)
    if inspect.isawaitable(result):
        await result


def _is_canceled(cb: Any) -> bool:
    if not cb:
        return False
    try:
        return bool(cb())
    except Exception:
        return False

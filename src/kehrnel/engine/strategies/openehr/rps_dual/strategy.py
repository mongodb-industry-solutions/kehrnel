from __future__ import annotations

import base64
import copy
import hashlib
import inspect
import json
import os
import random
import re
import tempfile
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from bson import json_util
from fastapi import HTTPException

from kehrnel.api.bridge.app.core.config_models import (
    CompositionCollectionNames,
    FieldMappingConfig,
)
from kehrnel.api.domains.openehr.composition.models import CompositionCreate
from kehrnel.api.domains.openehr.composition.service import (
    add_composition,
    update_composition,
)
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
    flatten_search_index_field_types,
)
from kehrnel.engine.strategies.openehr.rps_dual.id_encoding import (
    atlas_search_type_for_id_encoding,
    normalize_id_encoding,
)
from kehrnel.engine.strategies.openehr.rps_dual.query.executor import execute as execute_query_plan
from kehrnel.engine.strategies.openehr.rps_dual.query.executor import (
    _normalize_result_value,
    _uses_uuid_binary_ids,
)
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.value_formatter import ValueFormatter
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
from kehrnel.engine.strategies.openehr.rps_dual.query.pagination import render_effective_aql
from kehrnel.engine.strategies.openehr.rps_dual.projection_readiness import (
    atlas_index_readiness,
    build_semantic_projection_manifest,
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

# Path separators the query compiler can safely escape and round-trip. ":" is the
# default; "." is retained for backward compatibility. "/" is used by IBM
# exact-model HDL environments.
_SUPPORTED_QUERY_SAFE_SEPARATORS = {".", "/", ":"}

_INGEST_CONTROL_KEYS = {
    "__activation_precondition",
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
    "strategy_protocol",
}

_SEARCH_REBUILD_CURSOR_VERSION = 1
_SEARCH_REBUILD_MAX_BATCH_SIZE = 1000
_SEARCH_REBUILD_STATE_COLLECTION = "_kehrnel_projection_state"
_SEARCH_PROJECTION_CONSISTENCY_MODEL = (
    "governed-writer-mutation-fence/v1"
)
_SEARCH_PROJECTION_ALGORITHM_VERSION = "rps-dual-projector/v2"


def _is_sha256_digest(value: str) -> bool:
    return (
        len(value) == 71
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    )


def _search_rebuild_projection_revision(descriptor: Dict[str, Any]) -> str:
    canonical = json.dumps(
        descriptor,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _mapping_template_provenance(mappings_content: Any) -> Dict[str, Dict[str, str]]:
    """Extract optional OPT/catalog provenance without changing mapping semantics."""
    if isinstance(mappings_content, list):
        templates = mappings_content
        inherited: Dict[str, Any] = {}
    elif isinstance(mappings_content, dict):
        templates = mappings_content.get("templates")
        if not isinstance(templates, list) and (
            isinstance(mappings_content.get("analyticsTemplate"), dict)
            or isinstance(mappings_content.get("fields"), list)
        ):
            templates = [mappings_content]
        inherited = (
            mappings_content.get("provenance")
            if isinstance(mappings_content.get("provenance"), dict)
            else {}
        )
        inherited = {**inherited, **mappings_content}
    else:
        return {}
    if not isinstance(templates, list):
        return {}

    result: Dict[str, Dict[str, str]] = {}
    for raw_template in templates:
        if not isinstance(raw_template, dict):
            continue
        analytics = raw_template.get("analyticsTemplate")
        candidate = analytics if isinstance(analytics, dict) else raw_template
        template_id = str(
            candidate.get("templateId")
            or candidate.get("template_id")
            or candidate.get("id")
            or raw_template.get("templateId")
            or raw_template.get("template_id")
            or ""
        ).strip()
        if not template_id:
            continue
        local = (
            candidate.get("provenance")
            if isinstance(candidate.get("provenance"), dict)
            else {}
        )
        source_digest = (
            local.get("templateSourceDigest")
            or local.get("sourceDigest")
            or candidate.get("templateSourceDigest")
            or candidate.get("sourceDigest")
            or inherited.get("templateSourceDigest")
            or inherited.get("sourceDigest")
        )
        catalog_digest = (
            local.get("catalogDigest")
            or candidate.get("catalogDigest")
            or inherited.get("catalogDigest")
        )
        provenance: Dict[str, str] = {}
        if isinstance(source_digest, str) and source_digest.strip():
            provenance["templateSourceDigest"] = source_digest.strip()
        if isinstance(catalog_digest, str) and catalog_digest.strip():
            provenance["catalogDigest"] = catalog_digest.strip()
        if provenance:
            result[template_id] = provenance
    return result


def _normalize_projection_template_scope(raw_template_ids: Any) -> list[str] | None:
    """Normalize an optional logical scope within the shared search projection.

    ``None`` retains the historical all-mapped-templates rebuild.  A non-empty
    list creates a template-scoped readiness receipt without changing the
    strategy, collection, or Atlas Search index used by RPS Dual.
    """

    if raw_template_ids is None:
        return None
    if not isinstance(raw_template_ids, list) or not raw_template_ids:
        raise KehrnelError(
            code="SEARCH_REBUILD_SCOPE_INVALID",
            status=400,
            message="template_ids must be a non-empty array when provided.",
        )
    if len(raw_template_ids) > 128:
        raise KehrnelError(
            code="SEARCH_REBUILD_SCOPE_INVALID",
            status=400,
            message="template_ids may contain at most 128 template identifiers.",
        )
    normalized: list[str] = []
    for raw_template_id in raw_template_ids:
        if not isinstance(raw_template_id, str):
            raise KehrnelError(
                code="SEARCH_REBUILD_SCOPE_INVALID",
                status=400,
                message="Every template_ids entry must be a string.",
            )
        template_id = raw_template_id.strip()
        if not template_id or len(template_id) > 512:
            raise KehrnelError(
                code="SEARCH_REBUILD_SCOPE_INVALID",
                status=400,
                message="Every template_ids entry must contain 1 to 512 characters.",
            )
        normalized.append(template_id)
    return sorted(set(normalized))


def _projection_scope_key(template_ids: list[str] | None) -> str:
    if template_ids is None:
        return "all"
    digest = hashlib.sha256(
        json.dumps(template_ids, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"templates:{digest}"


def _projection_scope_document(template_ids: list[str] | None) -> Dict[str, Any]:
    if template_ids is None:
        return {"kind": "all-mapped-templates"}
    return {"kind": "templates", "templateIds": list(template_ids)}


def _projection_state_id(
    environment_id: str,
    target_collection: str,
    template_ids: list[str] | None = None,
) -> str:
    base = f"openehr.rps_dual:{environment_id}:{target_collection}"
    scope_key = _projection_scope_key(template_ids)
    return base if scope_key == "all" else f"{base}:scope:{scope_key}"


def _mapped_template_filter(
    template_field: str,
    template_ids: list[str],
    *,
    negate: bool = False,
) -> Dict[str, Any]:
    """Build the MongoDB predicate matching the flattener's version fallback.

    ``CompositionFlattener`` accepts an exact template id or another ``.v*``
    version in the same template family.  Reconciliation must use precisely the
    same population or it could certify a source document that the projector
    considers governed while silently excluding it from the coverage proof.
    """

    families = sorted(
        {
            template_id.rsplit(".v", 1)[0]
            for template_id in template_ids
            if ".v" in template_id
        }
    )
    clauses: list[Dict[str, Any]] = [
        {template_field: {"$in": sorted(set(template_ids))}}
    ]
    clauses.extend(
        {
            template_field: {
                "$regex": rf"^{re.escape(family)}(?:\.v[^.]*)?$"
            }
        }
        for family in families
    )
    return {"$nor" if negate else "$or": clauses}


def _matched_count(result: Any) -> int | None:
    value = getattr(result, "matched_count", None)
    if value is None and isinstance(result, dict):
        value = result.get("matched_count", result.get("matchedCount"))
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _encode_search_rebuild_cursor(
    source_id: Any,
    *,
    source_collection: str,
    target_collection: str,
    projection_revision: str,
    scope_key: str = "all",
) -> str:
    payload = {
        "version": _SEARCH_REBUILD_CURSOR_VERSION,
        "sourceCollection": source_collection,
        "targetCollection": target_collection,
        "projectionRevision": projection_revision,
        "scopeKey": scope_key,
        "afterId": source_id,
    }
    serialized = json_util.dumps(
        payload,
        json_options=json_util.CANONICAL_JSON_OPTIONS,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(serialized).decode("ascii").rstrip("=")


def _decode_search_rebuild_cursor(
    token: Any,
    *,
    source_collection: str,
    target_collection: str,
    projection_revision: str,
    scope_key: str = "all",
) -> Any:
    if not isinstance(token, str) or not token or len(token) > 8192:
        raise KehrnelError(
            code="SEARCH_REBUILD_CURSOR_INVALID",
            status=400,
            message="The slim-search rebuild cursor is invalid.",
        )
    try:
        padding = "=" * (-len(token) % 4)
        decoded = base64.b64decode(
            (token + padding).encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
        cursor = json_util.loads(decoded.decode("utf-8"))
    except Exception as exc:
        raise KehrnelError(
            code="SEARCH_REBUILD_CURSOR_INVALID",
            status=400,
            message="The slim-search rebuild cursor is malformed or unsupported.",
        ) from exc

    if not isinstance(cursor, dict) or cursor.get("version") != _SEARCH_REBUILD_CURSOR_VERSION:
        raise KehrnelError(
            code="SEARCH_REBUILD_CURSOR_INVALID",
            status=400,
            message="The slim-search rebuild cursor version is unsupported.",
        )
    expected = {
        "sourceCollection": source_collection,
        "targetCollection": target_collection,
        "projectionRevision": projection_revision,
        "scopeKey": scope_key,
    }
    mismatches = {
        key: {
            "expected": value,
            "actual": cursor.get(key, "all") if key == "scopeKey" else cursor.get(key),
        }
        for key, value in expected.items()
        if (cursor.get(key, "all") if key == "scopeKey" else cursor.get(key))
        != value
    }
    if mismatches:
        raise KehrnelError(
            code="SEARCH_REBUILD_CURSOR_MISMATCH",
            status=409,
            message=(
                "The slim-search rebuild cursor belongs to a different collection or "
                "projection revision. Restart the rebuild without a cursor."
            ),
            details={"mismatches": mismatches},
        )
    if "afterId" not in cursor or cursor["afterId"] is None:
        raise KehrnelError(
            code="SEARCH_REBUILD_CURSOR_INVALID",
            status=400,
            message="The slim-search rebuild cursor does not contain a source identifier.",
        )
    return cursor["afterId"]


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


def _extract_query_pagination_options(query: Dict[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(query, dict):
        return {}
    pagination = query.get("pagination") if isinstance(query.get("pagination"), dict) else {}
    options: Dict[str, Any] = {}
    if pagination:
        options["pagination"] = pagination
    for key in ("pageToken", "searchAfter", "searchBefore"):
        if query.get(key) is not None:
            options[key] = query.get(key)
    return options


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
        self._query_compile_caches: Dict[str, Dict[str, Any]] = {}

    def _compile_cache_for_context(self, ctx: StrategyContext) -> Dict[str, Any]:
        env_id = getattr(ctx, "environment_id", None) or "default"
        cache = self._query_compile_caches.get(env_id)
        if isinstance(cache, dict):
            return cache
        if len(self._query_compile_caches) >= 16:
            oldest_key = next(iter(self._query_compile_caches), None)
            if oldest_key is not None:
                self._query_compile_caches.pop(oldest_key, None)
        cache = {}
        self._query_compile_caches[env_id] = cache
        return cache

    async def validate_config(self, ctx: StrategyContext | Dict[str, Any]) -> None:
        raw_config = ctx.config if isinstance(ctx, StrategyContext) else ctx
        strategy_cfg = normalize_config(raw_config or {})
        errors: list[str] = []

        if strategy_cfg.paths.separator not in _SUPPORTED_QUERY_SAFE_SEPARATORS:
            errors.append(
                "paths.separator must be one of "
                f"{sorted(_SUPPORTED_QUERY_SAFE_SEPARATORS)} because query compilation must be able to "
                "round-trip stored paths."
            )

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
        if mappings_content is None:
            mappings_ref = (
                mappings_ref
                or "file://ingest/config/flattener_mappings_f.jsonc"
            )
            db = getattr((ctx.adapters or {}).get("storage"), "db", None)
            mappings_content = await resolve_uri_async(mappings_ref, db, Path(__file__).parent)
        if not isinstance(mappings_content, (dict, list)):
            raise KehrnelError(
                code="MAPPINGS_INVALID",
                status=400,
                message="The active RPS Dual mapping source must resolve to an object or array.",
                details={"mappingRef": mappings_ref},
            )
        return mappings_content

    def _default_search_index_definition(self, strategy_cfg: RPSDualConfig) -> Dict[str, Any]:
        return {
            "mappings": {
                "dynamic": False,
                "fields": {
                    strategy_cfg.fields.document.ehr_id: {
                        "type": atlas_search_type_for_id_encoding(
                            strategy_cfg.ids.ehr_id
                        ),
                    },
                    strategy_cfg.fields.document.comp_id: {
                        "type": atlas_search_type_for_id_encoding(
                            strategy_cfg.ids.composition_id
                        ),
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
                definition = await resolve_uri_async(search_cfg.atlasIndex.definition, db, Path(__file__).parent)
            except Exception:
                definition = None
        if isinstance(definition, dict) and definition.get("mappings"):
            return definition
        return self._default_search_index_definition(strategy_cfg)

    async def _build_active_search_projection(
        self,
        ctx: StrategyContext,
        strategy_cfg: RPSDualConfig,
    ) -> Dict[str, Any]:
        """Resolve the one canonical projection contract used by rebuild and preflight.

        The returned descriptor is deliberately derived from the active mapping
        rules and the index generated from those rules.  It never treats an Atlas
        Search index definition as if it were a transformation mapping.
        """
        mappings_content = await self._resolve_mappings_content(ctx, strategy_cfg)
        projection_meta = dict(ctx.meta or {})
        projection_meta["mappings"] = mappings_content
        bulk_config = dict(projection_meta.get("bulk_config") or {})
        # Rebuilding existing encoded documents must never allocate new
        # dictionary codes while compiling the projection rules.
        bulk_config["role"] = "secondary"
        projection_meta["bulk_config"] = bulk_config
        projection_ctx = StrategyContext(
            environment_id=ctx.environment_id,
            config=ctx.config,
            bindings=ctx.bindings,
            adapters=ctx.adapters,
            manifest=ctx.manifest,
            trace_id=ctx.trace_id,
            logger=ctx.logger,
            meta=projection_meta,
        )
        flattener = await self._build_flattener_for_context(projection_ctx)

        active_templates = {
            template_id: deepcopy(fields)
            for template_id, fields in sorted(flattener.template_fields.items())
            if isinstance(fields, list) and fields
        }
        invalid_templates = []
        for template_id, fields in active_templates.items():
            compiled_rules = flattener._compiled_rules_for_template(template_id)
            if len(compiled_rules) != len(fields):
                invalid_templates.append(
                    {
                        "templateId": template_id,
                        "fieldCount": len(fields),
                        "compiledRuleCount": len(compiled_rules),
                    }
                )
        if not active_templates or invalid_templates:
            raise KehrnelError(
                code="SEARCH_REBUILD_CONFIG_INVALID",
                status=400,
                message="The active projection mapping is empty or cannot be compiled completely.",
                details={
                    "templateCount": len(active_templates),
                    "invalidTemplates": invalid_templates,
                },
            )

        template_provenance = {
            **flattener.template_provenance,
            **_mapping_template_provenance(mappings_content),
        }
        active_mapping_templates = []
        for template_id, fields in active_templates.items():
            entry: Dict[str, Any] = {
                "templateId": template_id,
                "fields": fields,
            }
            if template_id in template_provenance:
                entry["provenance"] = template_provenance[template_id]
            active_mapping_templates.append(entry)
        active_mappings = {"templates": active_mapping_templates}
        index_build = await build_search_index_definition_from_mappings(
            strategy_cfg,
            active_mappings,
            db=getattr((ctx.adapters or {}).get("storage"), "db", None),
            shortcuts={"keys": flattener.shortcut_keys},
        )
        index_definition = index_build.get("definition")
        index_metadata = index_build.get("metadata") or {}
        if not isinstance(index_definition, dict) or int(
            index_metadata.get("data_field_count") or 0
        ) < 1:
            raise KehrnelError(
                code="SEARCH_REBUILD_CONFIG_INVALID",
                status=400,
                message="The active projection mapping produced no searchable fields.",
                details={"warnings": index_build.get("warnings", [])},
            )

        mapping_digest = _search_rebuild_projection_revision(active_mappings)
        index_definition_digest = _search_rebuild_projection_revision(index_definition)
        template_paths = {
            template_id: sorted(
                {
                    str(field.get("path"))
                    for field in fields
                    if isinstance(field, dict) and field.get("path")
                }
            )
            for template_id, fields in active_templates.items()
        }
        projection_descriptor = {
            "contract": "openehr.rps-dual.search-projection/v1",
            "projectionAlgorithmVersion": _SEARCH_PROJECTION_ALGORITHM_VERSION,
            "mappings": active_mappings,
            "mappingDigest": mapping_digest,
            "indexDefinition": index_definition,
            "indexDefinitionDigest": index_definition_digest,
            "pathSeparator": strategy_cfg.paths.separator,
            "compositionEncodingProfile": (
                strategy_cfg.collections.compositions.encodingProfile
            ),
            "searchEncodingProfile": strategy_cfg.collections.search.encodingProfile,
            "fields": strategy_cfg.fields.model_dump(mode="json"),
            "ids": strategy_cfg.ids.model_dump(mode="json"),
            "shortcuts": {
                "keys": flattener.shortcut_keys,
                "values": flattener.shortcut_vals,
            },
            "codes": flattener.code_book,
        }
        return {
            "mappingsContent": mappings_content,
            "activeMappings": active_mappings,
            "mappingDigest": mapping_digest,
            "indexDefinition": index_definition,
            "indexDefinitionDigest": index_definition_digest,
            "indexMetadata": index_metadata,
            "projectionDescriptor": projection_descriptor,
            "projectionRevision": _search_rebuild_projection_revision(
                projection_descriptor
            ),
            "templatePaths": template_paths,
            "templateProvenance": template_provenance,
            "flattener": flattener,
            "warnings": list(index_build.get("warnings") or []),
        }

    async def _invalidate_projection_readiness(
        self,
        ctx: StrategyContext,
        strategy_cfg: RPSDualConfig,
        *,
        reason: str,
    ) -> Dict[str, Any] | None:
        """Atomically fence governed base writes before they can become visible.

        RPS Dual writes its base and search collections separately.  Until that
        pair is made transactional, a prior readiness receipt must be invalidated
        *before* the base mutation: otherwise a failed process or sidecar write
        can leave semantic queries observing an incomplete projection.  The
        monotonically increasing sequence is also the compare-and-set fence used
        by terminal rebuild publication.

        Direct/external writes that bypass this strategy are outside the proven
        consistency model.  Such integrations must invalidate this receipt (or
        quiesce their writers) before a rebuild may be certified.
        """

        search_cfg = strategy_cfg.collections.search
        if not search_cfg.enabled or not search_cfg.name:
            return None
        storage = (ctx.adapters or {}).get("storage")
        update_one = getattr(storage, "update_one", None)
        update_many = getattr(storage, "update_many", None)
        find_one = getattr(storage, "find_one", None)
        if (
            not callable(update_one)
            or not callable(update_many)
            or not callable(find_one)
        ):
            raise KehrnelError(
                code="SEARCH_PROJECTION_FENCE_UNAVAILABLE",
                status=503,
                message=(
                    "Governed RPS Dual writes require atomic global and scoped "
                    "projection-readiness invalidation before mutating the base "
                    "collection."
                ),
            )

        state_id = _projection_state_id(ctx.environment_id, search_cfg.name)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        await update_one(
            _SEARCH_REBUILD_STATE_COLLECTION,
            {"_id": state_id},
            {
                "$setOnInsert": {
                    "createdAt": now,
                },
                "$set": {
                    "contract": "openehr.rps-dual.search-projection-readiness/v1",
                    "environmentId": ctx.environment_id,
                    "strategyId": "openehr.rps_dual",
                    "sourceCollection": strategy_cfg.collections.compositions.name,
                    "targetCollection": search_cfg.name,
                    "projection.state": "stale",
                    "projection.complete": False,
                    "projection.completedAt": None,
                    "reconciliation.safe": False,
                    "reconciliation.complete": False,
                    "invalidation": {
                        "reason": reason,
                        "invalidatedAt": now,
                    },
                    "consistency.model": _SEARCH_PROJECTION_CONSISTENCY_MODEL,
                    "consistency.governedWritersOnly": True,
                    "consistency.externalWriterPolicy": (
                        "external writers must invalidate this receipt before mutation"
                    ),
                },
                "$inc": {
                    "mutationSequence": 1,
                    "activeMutations": 1,
                },
            },
            upsert=True,
        )
        # Template-scoped rebuilds use the same physical projection.  Invalidate
        # every scoped receipt before the base write becomes visible; incrementing
        # its sequence also prevents a concurrent scoped rebuild from publishing
        # a stale checkpoint with compare-and-set.
        await update_many(
            _SEARCH_REBUILD_STATE_COLLECTION,
            {
                "environmentId": ctx.environment_id,
                "strategyId": "openehr.rps_dual",
                "sourceCollection": strategy_cfg.collections.compositions.name,
                "targetCollection": search_cfg.name,
                "scope.kind": "templates",
            },
            {
                "$set": {
                    "projection.state": "stale",
                    "projection.complete": False,
                    "projection.completedAt": None,
                    "reconciliation.safe": False,
                    "reconciliation.complete": False,
                    "invalidation": {
                        "reason": reason,
                        "invalidatedAt": now,
                    },
                },
                "$inc": {"mutationSequence": 1},
            },
            upsert=False,
        )
        state = await find_one(
            _SEARCH_REBUILD_STATE_COLLECTION,
            {"_id": state_id},
        )
        sequence = (
            state.get("mutationSequence") if isinstance(state, dict) else None
        )
        active_mutations = (
            state.get("activeMutations") if isinstance(state, dict) else None
        )
        if (
            not isinstance(sequence, int)
            or sequence < 1
            or not isinstance(active_mutations, int)
            or active_mutations < 1
        ):
            raise KehrnelError(
                code="SEARCH_PROJECTION_FENCE_FAILED",
                status=503,
                message="The projection mutation fence could not be verified.",
            )
        return {
            "stateId": state_id,
            "mutationSequence": sequence,
            "invalidatedAt": now,
            "reason": reason,
        }

    async def _complete_projection_mutation(
        self,
        ctx: StrategyContext,
        strategy_cfg: RPSDualConfig,
        *,
        reason: str,
    ) -> None:
        """Close a governed-writer fence while keeping readiness stale."""

        search_cfg = strategy_cfg.collections.search
        if not search_cfg.enabled or not search_cfg.name:
            return
        storage = (ctx.adapters or {}).get("storage")
        update_one = getattr(storage, "update_one", None)
        find_one = getattr(storage, "find_one", None)
        if not callable(update_one) or not callable(find_one):
            raise KehrnelError(
                code="SEARCH_PROJECTION_FENCE_UNAVAILABLE",
                status=503,
                message="The projection mutation fence cannot be completed.",
            )
        state_id = _projection_state_id(ctx.environment_id, search_cfg.name)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        result = await update_one(
            _SEARCH_REBUILD_STATE_COLLECTION,
            {"_id": state_id, "activeMutations": {"$gt": 0}},
            {
                "$set": {
                    "projection.state": "stale",
                    "projection.complete": False,
                    "reconciliation.safe": False,
                    "invalidation.completedAt": now,
                    "invalidation.reason": reason,
                },
                "$inc": {
                    "mutationSequence": 1,
                    "activeMutations": -1,
                },
            },
            upsert=False,
        )
        if _matched_count(result) == 0:
            raise KehrnelError(
                code="SEARCH_PROJECTION_FENCE_FAILED",
                status=503,
                message="No active projection mutation fence could be completed.",
            )
        state = await find_one(_SEARCH_REBUILD_STATE_COLLECTION, {"_id": state_id})
        if (
            not isinstance(state, dict)
            or not isinstance(state.get("activeMutations"), int)
            or state["activeMutations"] < 0
        ):
            raise KehrnelError(
                code="SEARCH_PROJECTION_FENCE_FAILED",
                status=503,
                message="The projection mutation fence could not be completed safely.",
            )

    async def _ensure_projection_mutation_fence(
        self,
        ctx: StrategyContext,
        strategy_cfg: RPSDualConfig,
        *,
        template_ids: list[str] | None = None,
    ) -> tuple[str, int]:
        """Create/read the atomic fence used by a rebuild without invalidating it."""

        storage = (ctx.adapters or {}).get("storage")
        update_one = getattr(storage, "update_one", None)
        find_one = getattr(storage, "find_one", None)
        if not callable(update_one) or not callable(find_one):
            raise KehrnelError(
                code="SEARCH_REBUILD_FENCE_UNAVAILABLE",
                status=503,
                message=(
                    "The storage adapter must support atomic update_one and find_one "
                    "for projection reconciliation."
                ),
            )
        search_name = strategy_cfg.collections.search.name
        state_id = _projection_state_id(
            ctx.environment_id,
            search_name,
            template_ids,
        )
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        await update_one(
            _SEARCH_REBUILD_STATE_COLLECTION,
            {"_id": state_id},
            {
                "$setOnInsert": {
                    "contract": "openehr.rps-dual.search-projection-readiness/v1",
                    "environmentId": ctx.environment_id,
                    "strategyId": "openehr.rps_dual",
                    "sourceCollection": strategy_cfg.collections.compositions.name,
                    "targetCollection": search_name,
                    "scope": _projection_scope_document(template_ids),
                    "mutationSequence": 0,
                    "activeMutations": 0,
                    "createdAt": now,
                }
            },
            upsert=True,
        )
        state = await find_one(_SEARCH_REBUILD_STATE_COLLECTION, {"_id": state_id})
        if not isinstance(state, dict):
            raise KehrnelError(
                code="SEARCH_REBUILD_FENCE_UNAVAILABLE",
                status=503,
                message="The projection mutation fence could not be initialized.",
            )
        sequence = state.get("mutationSequence")
        if not isinstance(sequence, int) or sequence < 0:
            # Upgrade a legacy receipt without overwriting a concurrent ingest.
            await update_one(
                _SEARCH_REBUILD_STATE_COLLECTION,
                {
                    "_id": state_id,
                    "mutationSequence": {"$exists": False},
                },
                {"$set": {"mutationSequence": 0}},
                upsert=False,
            )
            state = await find_one(
                _SEARCH_REBUILD_STATE_COLLECTION,
                {"_id": state_id},
            )
            sequence = (
                state.get("mutationSequence") if isinstance(state, dict) else None
            )
        active_mutations = state.get("activeMutations")
        if active_mutations is None:
            await update_one(
                _SEARCH_REBUILD_STATE_COLLECTION,
                {
                    "_id": state_id,
                    "activeMutations": {"$exists": False},
                },
                {"$set": {"activeMutations": 0}},
                upsert=False,
            )
            state = await find_one(
                _SEARCH_REBUILD_STATE_COLLECTION,
                {"_id": state_id},
            )
            active_mutations = (
                state.get("activeMutations") if isinstance(state, dict) else None
            )
        if not isinstance(sequence, int) or sequence < 0:
            raise KehrnelError(
                code="SEARCH_REBUILD_FENCE_UNAVAILABLE",
                status=503,
                message="The projection mutation fence has an invalid sequence.",
            )
        if not isinstance(active_mutations, int) or active_mutations != 0:
            raise KehrnelError(
                code="SEARCH_REBUILD_MUTATION_IN_PROGRESS",
                status=409,
                message=(
                    "A governed base mutation is still active (or a prior writer "
                    "did not close its fence). Reconcile that writer before rebuilding."
                ),
                details={"activeMutations": active_mutations},
            )
        return state_id, sequence

    async def _publish_projection_state(
        self,
        storage: Any,
        *,
        state_id: str,
        mutation_sequence: int,
        rebuild_owner: str,
        state_document: Dict[str, Any],
    ) -> None:
        """Publish a checkpoint iff no governed base mutation crossed the scan."""

        result = await storage.replace_one(
            _SEARCH_REBUILD_STATE_COLLECTION,
            {
                "_id": state_id,
                "mutationSequence": mutation_sequence,
                "activeMutations": 0,
                "rebuildLease.active": True,
                "rebuildLease.owner": rebuild_owner,
            },
            state_document,
            upsert=False,
        )
        matched = _matched_count(result)
        if matched == 0:
            raise KehrnelError(
                code="SEARCH_REBUILD_CONCURRENT_MUTATION",
                status=409,
                message=(
                    "A governed base mutation crossed the projection rebuild. "
                    "The stale receipt was preserved; restart the rebuild."
                ),
            )
        # Some test/custom adapters do not expose matched_count.  Verify the
        # exact published run marker rather than inferring success.
        observed = await storage.find_one(
            _SEARCH_REBUILD_STATE_COLLECTION,
            {"_id": state_id},
        )
        if (
            not isinstance(observed, dict)
            or observed.get("mutationSequence") != mutation_sequence
            or observed.get("rebuildRunId") != state_document.get("rebuildRunId")
        ):
            raise KehrnelError(
                code="SEARCH_REBUILD_CONCURRENT_MUTATION",
                status=409,
                message=(
                    "The projection rebuild checkpoint lost its mutation fence. "
                    "The projection remains unready."
                ),
            )

    async def _claim_projection_rebuild(
        self,
        storage: Any,
        *,
        state_id: str,
        mutation_sequence: int,
        rebuild_run_id: str,
        previous_state: Dict[str, Any],
        projection_summary: Dict[str, Any],
    ) -> tuple[str, int]:
        """Atomically publish ``building`` and acquire the single rebuild lease."""

        update_one = getattr(storage, "update_one", None)
        if not callable(update_one):
            raise KehrnelError(
                code="SEARCH_REBUILD_FENCE_UNAVAILABLE",
                status=503,
                message="The rebuild lease requires atomic update_one support.",
            )
        owner = str(uuid.uuid4())
        claimed_mutation_sequence = mutation_sequence + 1
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        claim_filter: Dict[str, Any] = {
            "_id": state_id,
            "mutationSequence": mutation_sequence,
            "activeMutations": 0,
            "$or": [
                {"rebuildLease.active": {"$exists": False}},
                {"rebuildLease.active": False},
            ],
        }
        prior_run = previous_state.get("rebuildRunId")
        if isinstance(prior_run, str) and prior_run:
            claim_filter["rebuildRunId"] = prior_run
        else:
            claim_filter["rebuildRunId"] = {"$exists": False}
        prior_checkpoint = previous_state.get("checkpoint")
        if isinstance(prior_checkpoint, dict) and isinstance(
            prior_checkpoint.get("updatedAt"), str
        ):
            claim_filter["checkpoint.updatedAt"] = prior_checkpoint["updatedAt"]
        else:
            claim_filter["checkpoint"] = {"$exists": False}

        result = await update_one(
            _SEARCH_REBUILD_STATE_COLLECTION,
            claim_filter,
            {
                "$set": {
                    "rebuildRunId": rebuild_run_id,
                    "rebuildLease": {
                        "active": True,
                        "owner": owner,
                        "acquiredAt": now,
                    },
                    "projection": {
                        **projection_summary,
                        "state": "building",
                        "complete": False,
                        "completedAt": None,
                    },
                    "reconciliation": {
                        "complete": False,
                        "safe": False,
                        "sourceToTargetCoverage": "pending",
                        "targetToSourceCoverage": "pending",
                        "mappedEmptyCount": 0,
                    },
                    "consistency": {
                        "model": _SEARCH_PROJECTION_CONSISTENCY_MODEL,
                        "mutationSequence": claimed_mutation_sequence,
                        "activeMutations": 0,
                        "governedWritersOnly": True,
                        "publication": "compare-and-set",
                    },
                },
                "$inc": {"mutationSequence": 1},
            },
            upsert=False,
        )
        if _matched_count(result) == 0:
            raise KehrnelError(
                code="SEARCH_REBUILD_ALREADY_RUNNING",
                status=409,
                message=(
                    "Another rebuild or governed write changed the projection state. "
                    "Reload its checkpoint before retrying."
                ),
            )
        observed = await storage.find_one(
            _SEARCH_REBUILD_STATE_COLLECTION,
            {"_id": state_id},
        )
        lease = observed.get("rebuildLease") if isinstance(observed, dict) else None
        if (
            not isinstance(lease, dict)
            or lease.get("active") is not True
            or lease.get("owner") != owner
            or observed.get("mutationSequence") != claimed_mutation_sequence
        ):
            raise KehrnelError(
                code="SEARCH_REBUILD_ALREADY_RUNNING",
                status=409,
                message="The projection rebuild lease could not be acquired.",
            )
        return owner, claimed_mutation_sequence

    async def _abandon_projection_rebuild(
        self,
        storage: Any,
        *,
        state_id: str,
        rebuild_owner: str,
        reason: str,
    ) -> None:
        """Release this invocation's lease while preserving a non-ready state."""

        update_one = getattr(storage, "update_one", None)
        if not callable(update_one):
            return
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        await update_one(
            _SEARCH_REBUILD_STATE_COLLECTION,
            {
                "_id": state_id,
                "rebuildLease.active": True,
                "rebuildLease.owner": rebuild_owner,
            },
            {
                "$set": {
                    "rebuildLease.active": False,
                    "rebuildLease.releasedAt": now,
                    "rebuildLease.releaseReason": reason,
                    "projection.state": "stale",
                    "projection.complete": False,
                    "reconciliation.safe": False,
                    "reconciliation.complete": False,
                }
            },
            upsert=False,
        )

    async def _semantic_projection_preflight(
        self,
        ctx: StrategyContext,
        strategy_cfg: RPSDualConfig,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Prove that one semantic plan can safely query the live projection."""

        if payload.get("contract") != "openehr-semantic-projection-preflight/v1":
            raise KehrnelError(
                code="OPENEHR_SEMANTIC_PROJECTION_PREFLIGHT_INVALID",
                status=400,
                message="A supported semantic projection preflight contract is required.",
            )
        template_id = str(payload.get("templateId") or "").strip()
        template_source_digest = str(
            payload.get("templateSourceDigest") or ""
        ).strip()
        catalog_digest = str(payload.get("catalogDigest") or "").strip()
        plan_digest = str(payload.get("planDigest") or "").strip()
        raw_required_paths = payload.get("requiredPaths")
        if (
            not template_id
            or len(template_id) > 512
            or not _is_sha256_digest(template_source_digest)
            or not _is_sha256_digest(catalog_digest)
            or not _is_sha256_digest(plan_digest)
            or not isinstance(raw_required_paths, list)
            or not raw_required_paths
            or len(raw_required_paths) > 256
            or any(
                not isinstance(path, str)
                or not path.startswith("/")
                or len(path) > 4096
                for path in raw_required_paths
            )
        ):
            raise KehrnelError(
                code="OPENEHR_SEMANTIC_PROJECTION_PREFLIGHT_INVALID",
                status=400,
                message="The semantic projection preflight request is invalid.",
            )
        required_paths = sorted(set(raw_required_paths))

        storage = (ctx.adapters or {}).get("storage")
        atlas = (ctx.adapters or {}).get("atlas_search") or (
            ctx.adapters or {}
        ).get("text_search")
        search_cfg = strategy_cfg.collections.search
        atlas_cfg = search_cfg.atlasIndex
        if (
            not storage
            or not callable(getattr(storage, "find_one", None))
            or not search_cfg.enabled
            or not search_cfg.name
            or atlas_cfg is None
            or not atlas_cfg.name
        ):
            raise KehrnelError(
                code="OPENEHR_SEMANTIC_PROJECTION_NOT_READY",
                status=409,
                message="The semantic projection storage or Atlas Search index is not configured.",
            )

        projection = await self._build_active_search_projection(ctx, strategy_cfg)
        available_paths = projection["templatePaths"].get(template_id, [])
        template_provenance = (
            projection.get("templateProvenance", {}).get(template_id)
            if isinstance(projection.get("templateProvenance"), dict)
            else None
        )
        template_provenance = (
            template_provenance if isinstance(template_provenance, dict) else {}
        )
        manifest = build_semantic_projection_manifest(
            environment_id=ctx.environment_id,
            template_id=template_id,
            template_source_digest=template_source_digest,
            catalog_digest=catalog_digest,
            plan_digest=plan_digest,
            mapping_content=projection["activeMappings"],
            projection_revision=projection["projectionRevision"],
            required_paths=required_paths,
            available_paths=available_paths,
            atlas_index_name=atlas_cfg.name,
            atlas_index_definition=projection["indexDefinition"],
        )

        scoped_template_ids = [template_id]
        state_id = _projection_state_id(
            ctx.environment_id,
            search_cfg.name,
            scoped_template_ids,
        )
        state_document = await storage.find_one(
            _SEARCH_REBUILD_STATE_COLLECTION,
            {"_id": state_id},
        )
        # A historical/global rebuild remains a valid fallback.  Once a
        # template-scoped receipt exists it is authoritative for that template,
        # including when stale, so a previous global receipt cannot mask it.
        if not state_document:
            state_id = _projection_state_id(ctx.environment_id, search_cfg.name)
            state_document = await storage.find_one(
                _SEARCH_REBUILD_STATE_COLLECTION,
                {"_id": state_id},
            )
        persisted_projection = (
            state_document.get("projection")
            if isinstance(state_document, dict)
            and isinstance(state_document.get("projection"), dict)
            else {}
        )
        checkpoint = (
            state_document.get("checkpoint")
            if isinstance(state_document, dict)
            and isinstance(state_document.get("checkpoint"), dict)
            else None
        )
        reconciliation = (
            state_document.get("reconciliation")
            if isinstance(state_document, dict)
            and isinstance(state_document.get("reconciliation"), dict)
            else {}
        )
        consistency = (
            state_document.get("consistency")
            if isinstance(state_document, dict)
            and isinstance(state_document.get("consistency"), dict)
            else {}
        )
        rebuild_lease = (
            state_document.get("rebuildLease")
            if isinstance(state_document, dict)
            and isinstance(state_document.get("rebuildLease"), dict)
            else {}
        )
        persisted_scope = (
            state_document.get("scope")
            if isinstance(state_document, dict)
            and isinstance(state_document.get("scope"), dict)
            else None
        )
        scope_matches = bool(
            # Legacy/global receipts predate the explicit scope field.
            persisted_scope is None
            or persisted_scope.get("kind") == "all-mapped-templates"
            or (
                persisted_scope.get("kind") == "templates"
                and template_id in (persisted_scope.get("templateIds") or [])
            )
        )

        state_shape_matches = bool(
            isinstance(state_document, dict)
            and state_document.get("contract")
            == "openehr.rps-dual.search-projection-readiness/v1"
            and state_document.get("environmentId") == ctx.environment_id
            and state_document.get("strategyId") == "openehr.rps_dual"
            and state_document.get("sourceCollection")
            == strategy_cfg.collections.compositions.name
            and state_document.get("targetCollection") == search_cfg.name
            and scope_matches
        )
        revision_matches = bool(
            state_shape_matches
            and persisted_projection.get("projectionAlgorithmVersion")
            == _SEARCH_PROJECTION_ALGORITHM_VERSION
            and persisted_projection.get("revision")
            == projection["projectionRevision"]
            and persisted_projection.get("mappingDigest")
            == projection["mappingDigest"]
            and persisted_projection.get("indexDefinitionDigest")
            == projection["indexDefinitionDigest"]
        )
        integrity_proven = bool(
            reconciliation.get("complete") is True
            and reconciliation.get("safe") is True
            and reconciliation.get("sourceToTargetCoverage") == "verified"
            and reconciliation.get("targetToSourceCoverage") == "verified"
            and reconciliation.get("mappedEmptyCount") == 0
            and consistency.get("model") == _SEARCH_PROJECTION_CONSISTENCY_MODEL
            and consistency.get("governedWritersOnly") is True
            and consistency.get("activeMutations") == 0
            and rebuild_lease.get("active") is False
            and isinstance(state_document.get("mutationSequence"), int)
            and state_document.get("activeMutations") == 0
            and isinstance(checkpoint, dict)
            and checkpoint.get("mutationSequence")
            == state_document.get("mutationSequence")
            and consistency.get("mutationSequence")
            == state_document.get("mutationSequence")
        )
        rebuild_complete = bool(
            revision_matches
            and persisted_projection.get("complete") is True
            and persisted_projection.get("state") == "ready"
            and integrity_proven
        )
        if not state_document:
            rebuild_state = "missing"
        elif not revision_matches:
            rebuild_state = "stale"
        else:
            persisted_state = str(persisted_projection.get("state") or "")
            rebuild_state = (
                persisted_state
                if persisted_state in {"ready", "building", "index_pending"}
                else "stale"
            )

        atlas_state = "adapter_unavailable"
        atlas_verified = False
        list_indexes = getattr(atlas, "list_search_indexes", None)
        if callable(list_indexes):
            try:
                indexes = await list_indexes(search_cfg.name)
            except Exception:
                atlas_state = "configured_unverified"
            else:
                if isinstance(indexes, list):
                    atlas_state, atlas_verified = atlas_index_readiness(
                        indexes,
                        index_name=atlas_cfg.name,
                        expected_definition_digest=manifest[
                            "atlasIndexDefinitionDigest"
                        ],
                    )
                else:
                    atlas_state = "configured_unverified"

        reasons: list[str] = []
        if not template_provenance:
            reasons.append("projection_provenance_missing")
        else:
            if (
                template_provenance.get("templateSourceDigest")
                != template_source_digest
            ):
                reasons.append("projection_template_source_digest_mismatch")
            if template_provenance.get("catalogDigest") != catalog_digest:
                reasons.append("projection_catalog_digest_mismatch")
        if manifest["missingPaths"]:
            reasons.append("required_projection_paths_missing")
        if rebuild_lease.get("active") is True:
            reasons.append("projection_rebuild_in_progress")
        if rebuild_state == "missing":
            reasons.append("projection_rebuild_receipt_missing")
        elif rebuild_state == "stale":
            reasons.append("projection_rebuild_receipt_stale")
        elif not rebuild_complete:
            reasons.append("projection_rebuild_incomplete")
        if state_document and not integrity_proven:
            reasons.append("projection_integrity_unproven")
        if not atlas_verified:
            reasons.append("atlas_search_index_not_verified_ready")

        return {
            "contract": "openehr-semantic-projection-readiness/v1",
            "ready": not reasons,
            "manifest": manifest,
            "rebuildState": rebuild_state,
            "rebuildComplete": rebuild_complete,
            "rebuildInProgress": rebuild_lease.get("active") is True,
            "rebuildWatermark": deepcopy(checkpoint),
            "projectionScope": deepcopy(persisted_scope),
            "reconciliation": deepcopy(reconciliation),
            "consistency": deepcopy(consistency),
            "atlasIndexState": atlas_state,
            "atlasIndexLiveVerified": atlas_verified,
            "reasons": sorted(set(reasons)),
        }

    async def _operational_probe(
        self,
        ctx: StrategyContext,
        strategy_cfg: RPSDualConfig,
        payload: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Observe the active physical contract without reading clinical payloads."""

        check_ids = (
            "runtime_connectivity",
            "required_collections",
            "storage_schema",
            "managed_indexes",
            "derived_projection",
            "data_freshness",
            "reconciliation_integrity",
        )
        storage = (ctx.adapters or {}).get("storage")
        database = getattr(storage, "db", None)
        if storage is None or database is None:
            detail = "The active openEHR strategy has no observable MongoDB adapter."
            return {
                "state": "unavailable",
                "schemaState": "unavailable",
                "indexState": "unavailable",
                "checks": [
                    {"checkId": check_id, "state": "unavailable", "detail": detail}
                    for check_id in check_ids
                ],
            }

        async def resolve(value):
            return await value if inspect.isawaitable(value) else value

        try:
            await resolve(database.command("ping"))
        except Exception:
            detail = "The active openEHR strategy could not reach its MongoDB target."
            return {
                "state": "unavailable",
                "schemaState": "unavailable",
                "indexState": "unavailable",
                "checks": [
                    {"checkId": check_id, "state": "unavailable", "detail": detail}
                    for check_id in check_ids
                ],
            }

        plan = await self.plan(ctx)
        artifacts = self._normalize_artifacts(plan)
        required_collections = set(artifacts.get("collections") or [])
        try:
            collection_names = set(await resolve(database.list_collection_names()))
            collections_present = required_collections <= collection_names
            collections_state = "passed" if collections_present else "failed"
        except Exception:
            collections_present = False
            collections_state = "unavailable"

        # RPS Dual deliberately supports multiple physical encodings. The schema
        # check therefore validates the activated, normalized contract rather than
        # pretending that every clinical document has one JSON Schema.
        schema_coherent = bool(
            strategy_cfg.collections.compositions.name
            and (
                not strategy_cfg.collections.search.enabled
                or (
                    strategy_cfg.collections.search.name
                    and strategy_cfg.collections.search.name
                    != strategy_cfg.collections.compositions.name
                )
            )
        )

        expected_index_names: dict[str, set[str]] = {}
        for index in artifacts.get("indexes") or []:
            name = str(((index.get("options") or {}).get("name") or "")).strip()
            collection = str(index.get("collection") or "").strip()
            if name and collection:
                expected_index_names.setdefault(collection, set()).add(name)
        btree_indexes_observed = True
        btree_indexes_match = True
        try:
            for collection, expected_names in expected_index_names.items():
                actual = await resolve(database[collection].index_information())
                actual_names = set(actual) if isinstance(actual, dict) else set()
                if not expected_names <= actual_names:
                    btree_indexes_match = False
        except Exception:
            btree_indexes_observed = False
            btree_indexes_match = False

        projection_state = "passed"
        projection_detail = "No derived search projection is required by this activation."
        freshness_target = (payload or {}).get("freshness_target_seconds")
        observed_freshness = 0 if isinstance(freshness_target, int) else None
        freshness_state = "passed"
        freshness_detail = "No derived projection freshness boundary applies to this activation."
        reconciliation_state = "passed"
        reconciliation_detail = "No derived representation requires reconciliation."
        reconciliation_divergence_count: int | None = 0
        atlas_observed = True
        atlas_matches = True
        if strategy_cfg.collections.search.enabled:
            search_cfg = strategy_cfg.collections.search
            atlas_cfg = search_cfg.atlasIndex
            projection_state = "failed"
            projection_detail = "The active openEHR derived projection is stale or incomplete."
            freshness_state = "failed"
            freshness_detail = "The active openEHR projection is not caught up to governed writes."
            reconciliation_state = "failed"
            reconciliation_detail = "The active openEHR projection is not fully reconciled."
            try:
                expected_projection = await self._build_active_search_projection(
                    ctx, strategy_cfg
                )
                state_document = await storage.find_one(
                    _SEARCH_REBUILD_STATE_COLLECTION,
                    {
                        "_id": _projection_state_id(
                            ctx.environment_id,
                            search_cfg.name,
                        )
                    },
                )
                persisted = (
                    state_document.get("projection")
                    if isinstance(state_document, dict)
                    and isinstance(state_document.get("projection"), dict)
                    else {}
                )
                reconciliation = (
                    state_document.get("reconciliation")
                    if isinstance(state_document, dict)
                    and isinstance(state_document.get("reconciliation"), dict)
                    else {}
                )
                consistency = (
                    state_document.get("consistency")
                    if isinstance(state_document, dict)
                    and isinstance(state_document.get("consistency"), dict)
                    else {}
                )
                checkpoint = (
                    state_document.get("checkpoint")
                    if isinstance(state_document, dict)
                    and isinstance(state_document.get("checkpoint"), dict)
                    else {}
                )
                reconciliation_ready = bool(
                    reconciliation.get("safe") is True
                    and reconciliation.get("complete") is True
                    and reconciliation.get("sourceToTargetCoverage") == "verified"
                    and reconciliation.get("targetToSourceCoverage") == "verified"
                    and int(reconciliation.get("mappedEmptyCount") or 0) == 0
                )
                projection_ready = bool(
                    persisted.get("state") == "ready"
                    and persisted.get("complete") is True
                    and persisted.get("revision")
                    == expected_projection["projectionRevision"]
                    and persisted.get("mappingDigest")
                    == expected_projection["mappingDigest"]
                    and persisted.get("indexDefinitionDigest")
                    == expected_projection["indexDefinitionDigest"]
                    and reconciliation_ready
                    and consistency.get("activeMutations") == 0
                )
                reconciliation_state = "passed" if reconciliation_ready else "failed"
                reconciliation_detail = (
                    "The derived projection has verified source-to-target and target-to-source coverage."
                    if reconciliation_ready
                    else "The derived projection has incomplete or divergent reconciliation evidence."
                )
                checkpoint_sequence = checkpoint.get("mutationSequence")
                consistency_sequence = consistency.get("mutationSequence")
                caught_up = bool(
                    projection_ready
                    and isinstance(checkpoint_sequence, int)
                    and checkpoint_sequence == consistency_sequence
                    and consistency.get("activeMutations") == 0
                )
                freshness_state = "passed" if caught_up else "failed"
                observed_freshness = 0 if caught_up else None
                reconciliation_divergence_count = 0 if reconciliation_ready else None
                freshness_detail = (
                    "The projection is caught up at mutation sequence "
                    f"{checkpoint_sequence}; observed governed-write lag is 0 seconds"
                    + (
                        f" against a {freshness_target}-second target."
                        if isinstance(freshness_target, int)
                        else "; no bounded freshness target was declared."
                    )
                    if caught_up
                    else "The projection checkpoint does not match the active mutation sequence."
                )
                if projection_ready:
                    projection_state = "passed"
                    projection_detail = (
                        "The active openEHR derived projection matches its mapping and index revision."
                    )
            except Exception:
                projection_state = "unavailable"
                projection_detail = (
                    "The active openEHR projection revision could not be observed."
                )
                freshness_state = "unavailable"
                freshness_detail = "The openEHR projection checkpoint could not be observed."
                reconciliation_state = "unavailable"
                reconciliation_detail = "The openEHR reconciliation receipt could not be observed."
                observed_freshness = None
                reconciliation_divergence_count = None

            atlas = (ctx.adapters or {}).get("atlas_search") or (
                ctx.adapters or {}
            ).get("text_search")
            list_indexes = getattr(atlas, "list_search_indexes", None)
            if atlas_cfg is not None and atlas_cfg.name and callable(list_indexes):
                try:
                    indexes = await list_indexes(search_cfg.name)
                    _atlas_state, atlas_matches = atlas_index_readiness(
                        indexes,
                        index_name=atlas_cfg.name,
                        expected_definition_digest=expected_projection[
                            "indexDefinitionDigest"
                        ],
                    )
                except Exception:
                    atlas_observed = False
                    atlas_matches = False
            else:
                atlas_observed = False
                atlas_matches = False

        indexes_observed = btree_indexes_observed and atlas_observed
        indexes_match = btree_indexes_match and atlas_matches
        index_state = (
            "passed"
            if indexes_observed and indexes_match
            else "failed"
            if indexes_observed
            else "unavailable"
        )
        checks = [
            {
                "checkId": "runtime_connectivity",
                "state": "passed",
                "detail": "The active openEHR strategy reached its configured MongoDB target.",
            },
            {
                "checkId": "required_collections",
                "state": collections_state,
                "detail": (
                    "Every collection required by the active openEHR plan exists."
                    if collections_state == "passed"
                    else "One or more collections required by the active openEHR plan are missing or unobservable."
                ),
            },
            {
                "checkId": "storage_schema",
                "state": "passed" if schema_coherent else "failed",
                "detail": (
                    "The activated openEHR encoding and collection contract is coherent."
                    if schema_coherent
                    else "The activated openEHR encoding or collection contract is incoherent."
                ),
            },
            {
                "checkId": "managed_indexes",
                "state": index_state,
                "detail": (
                    "Every managed B-tree and Atlas Search index matches the active plan."
                    if index_state == "passed"
                    else "Managed index evidence is drifted or unavailable."
                ),
            },
            {
                "checkId": "derived_projection",
                "state": projection_state,
                "detail": projection_detail,
            },
            {
                "checkId": "data_freshness",
                "state": freshness_state,
                "detail": freshness_detail,
            },
            {
                "checkId": "reconciliation_integrity",
                "state": reconciliation_state,
                "detail": reconciliation_detail,
            },
        ]
        all_passed = all(check["state"] == "passed" for check in checks)
        all_unavailable = all(check["state"] == "unavailable" for check in checks)
        return {
            "state": (
                "healthy" if all_passed else "unavailable" if all_unavailable else "degraded"
            ),
            "schemaState": (
                "in_sync"
                if collections_state == "passed" and schema_coherent
                else "drifted"
                if collections_state == "failed" or not schema_coherent
                else "unavailable"
            ),
            "indexState": {
                "passed": "in_sync",
                "failed": "drifted",
                "unavailable": "unavailable",
            }[index_state],
            "freshnessTargetSeconds": (
                freshness_target if isinstance(freshness_target, int) else None
            ),
            "observedFreshnessSeconds": observed_freshness,
            "reconciliationDivergenceCount": reconciliation_divergence_count,
            "checks": checks,
        }


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
        # These B-tree indexes are part of the projection-maintenance contract,
        # not query-model preferences.  They keep final source/sidecar
        # reconciliation bounded for large, heterogeneous repositories.
        if comp.name:
            artifacts["indexes"].extend(
                [
                    {
                        "collection": comp.name,
                        "keys": [(strategy_cfg.fields.document.comp_id, 1)],
                        "options": {"name": "kehrnel_projection_source_comp_id"},
                    },
                    {
                        "collection": comp.name,
                        "keys": [(strategy_cfg.fields.document.tid, 1)],
                        "options": {"name": "kehrnel_projection_source_template"},
                    },
                ]
            )
        if search.enabled and search.name:
            artifacts["indexes"].append(
                {
                    "collection": search.name,
                    "keys": [(strategy_cfg.fields.document.tid, 1)],
                    "options": {"name": "kehrnel_projection_target_template"},
                }
            )
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
        try:
            strategy_cfg = normalize_config(ctx.config or {})
        except Exception:
            strategy_cfg = None
        document_fields = getattr(getattr(strategy_cfg, "fields", None), "document", None)
        node_fields = getattr(getattr(strategy_cfg, "fields", None), "node", None)

        document_aliases = {
            "ehr_id": getattr(document_fields, "ehr_id", "ehr_id"),
            "comp_id": getattr(document_fields, "comp_id", "comp_id"),
            "tid": getattr(document_fields, "tid", "tid"),
            "v": getattr(document_fields, "v", "v"),
            "time_c": getattr(document_fields, "time_committed", "time_c"),
            "time_committed": getattr(document_fields, "time_committed", "time_c"),
            "sort_time": getattr(document_fields, "sort_time", "sort_time"),
            "cn": getattr(document_fields, "cn", "cn"),
            "sn": getattr(document_fields, "sn", "sn"),
        }
        node_aliases = {
            "p": getattr(node_fields, "p", "p"),
            "pi": getattr(node_fields, "pi", "pi"),
            "data": getattr(node_fields, "data", "data"),
        }
        node_array_fields = {document_aliases["cn"], document_aliases["sn"]}

        def resolve_index_field(field: str) -> str:
            if not isinstance(field, str) or not field:
                return field
            parts = field.split(".")
            parts[0] = document_aliases.get(parts[0], parts[0])
            if len(parts) > 1 and parts[0] in node_array_fields:
                parts[1] = node_aliases.get(parts[1], parts[1])
            return ".".join(parts)

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

        def plan_metadata(idx: Dict[str, Any], logical_fields: list[str] | None = None) -> Dict[str, Any]:
            metadata: Dict[str, Any] = {
                "id": idx.get("id"),
                "type": idx.get("type"),
                "requirement": idx.get("requirement"),
                "purpose": idx.get("purpose"),
                "workloads": idx.get("workloads"),
                "configResolved": idx.get("configResolved"),
                "fieldsFromConfig": idx.get("fieldsFromConfig"),
            }
            if logical_fields:
                metadata["logicalFields"] = logical_fields
            return {key: value for key, value in metadata.items() if value not in (None, [], {})}

        def search_plan_metadata(search: Dict[str, Any]) -> Dict[str, Any]:
            metadata: Dict[str, Any] = {
                "kind": search.get("kind"),
                "requirement": search.get("requirement"),
                "condition": search.get("condition"),
                "purpose": search.get("purpose"),
                "workloads": search.get("workloads"),
                "configResolved": search.get("configResolved"),
            }
            return {key: value for key, value in metadata.items() if value not in (None, [], {})}

        for store in stores:
            if not isinstance(store, dict):
                continue
            coll = resolve_collection(store)
            search_contract = store.get("search")
            if coll and isinstance(search_contract, dict):
                for search_index in artifacts.get("search_indexes", []):
                    if search_index.get("collection") == coll:
                        search_index.update(search_plan_metadata(search_contract))
            for idx in store.get("indexes") or []:
                if not isinstance(idx, dict):
                    continue
                if idx.get("autoCreate") is False:
                    continue
                idx_type = idx.get("type")
                if idx_type == "btree":
                    fields = idx.get("fields") or []
                    keys = [(resolve_index_field(f), 1) for f in fields]
                    if coll and keys:
                        artifacts["indexes"].append(
                            {
                                "collection": coll,
                                "keys": keys,
                                "options": idx.get("options", {}),
                                **plan_metadata(idx, fields),
                            }
                        )
                elif idx_type == "wildcard":
                    field = resolve_index_field(idx.get("field") or "data")
                    if coll:
                        artifacts["indexes"].append(
                            {
                                "collection": coll,
                                "keys": [(f"{field}.$**", 1)],
                                "options": idx.get("options", {}),
                                **plan_metadata(idx, [idx.get("field") or "data"]),
                            }
                        )
                elif idx_type == "search":
                    if coll:
                        artifacts["search_indexes"].append(
                            {
                                "collection": coll,
                                "name": idx.get("name") or store.get("role") or "search_index",
                                "definition": idx.get("definition", {}),
                                "storedSource": idx.get("storedSource"),
                                **plan_metadata(idx),
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

        mappings_path = str(Path(__file__).parent / "ingest" / "config" / "flattener_mappings_f.jsonc")

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
            return {
                "_id": payload.get("_id") or "comp-1",
                "ehr_id": payload.get("ehr_id") or "ehr-1",
                "composition_version": payload.get("composition_version"),
                "time_committed": payload.get("time_committed"),
                "time_created": payload.get("time_created"),
                "canonicalJSON": comp_obj,
            }
        return {
            "_id": "comp-1",
            "ehr_id": "ehr-1",
            "composition_version": None,
            "time_committed": None,
            "time_created": None,
            "canonicalJSON": comp_obj,
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

        # Base and sidecar writes are not one transaction.  Fence the previously
        # certified projection before the first governed base write, so a crash
        # or failed sidecar insert cannot leave semantic execution falsely ready.
        projection_fenced = bool(
            storage and comp_name and base_docs and search_enabled and search_name
        )
        if projection_fenced:
            await self._invalidate_projection_readiness(
                ctx,
                normalize_config(cfg),
                reason="governed_ingest",
            )

        try:
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
        finally:
            if projection_fenced:
                await self._complete_projection_mutation(
                    ctx,
                    normalize_config(cfg),
                    reason="governed_ingest",
                )
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
        compile_cache = self._compile_cache_for_context(ctx)
        search_index_definition = await self._resolve_search_index_definition(
            ctx,
            cfg_model,
        )
        search_field_types = flatten_search_index_field_types(
            search_index_definition
        )

        def _execution_contract(stage_name: str, schema_cfgs: Dict[str, Any]) -> tuple[str, str | None]:
            if stage_name == "$search":
                return "text_search_dual", (schema_cfgs.get("search") or {}).get("collection")
            return "mongo_pipeline", (schema_cfgs.get("composition") or {}).get("collection")

        if isinstance(query, dict):
            debug = bool(query.get("debug"))
            require_exact_path_correlation = bool(
                query.get("require_exact_path_correlation")
                or query.get("requireExactPathCorrelation")
            )
            pagination_options = _extract_query_pagination_options(query)
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
                    compile_cache=compile_cache,
                    pagination_options=pagination_options,
                    code_items=codes_res.get("items") or {},
                    require_exact_path_correlation=require_exact_path_correlation,
                    search_field_types=search_field_types,
                )
                pagination_info = builder_info.get("pagination") or {}
                warnings.extend(pagination_info.get("warnings") or [])
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
                plan_dict["pagination"] = {
                    key: value for key, value in pagination_info.items() if key != "warnings"
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
                    "effectiveAql": render_effective_aql(raw_aql, pagination_info) if debug else None,
                    "parameters": params if debug else None,
                    "pagination": plan_dict["pagination"],
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
            query_payload = {
                k: v
                for k, v in query.items()
                if k
                not in {
                    "debug",
                    "pageToken",
                    "searchAfter",
                    "searchBefore",
                    "pagination",
                    "require_exact_path_correlation",
                    "requireExactPathCorrelation",
                }
            }
        else:
            pagination_options = {}
            require_exact_path_correlation = False

        ir = AqlQueryIR(**query_payload) if not isinstance(query, AqlQueryIR) else query
        # Build query pipeline using the query compiler
        engine, pipeline, stage0, schema_cfgs, ast_doc, builder_info = await build_query_pipeline(
            ir,
            cfg_model,
            db=motor_db,
            shortcut_map=shortcuts_res.get("items") or {},
            strategy=runtime_strategy,
            compile_cache=compile_cache,
            pagination_options=pagination_options,
            code_items=codes_res.get("items") or {},
            require_exact_path_correlation=require_exact_path_correlation,
            search_field_types=search_field_types,
        )
        pagination_info = builder_info.get("pagination") or {}
        warnings.extend(pagination_info.get("warnings") or [])
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
        plan_dict["pagination"] = {
            key: value for key, value in pagination_info.items() if key != "warnings"
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
            "pagination": plan_dict["pagination"],
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

    @staticmethod
    def _native_composition_config(
        ctx: StrategyContext,
        strategy_cfg: RPSDualConfig,
    ) -> CompositionCollectionNames:
        raw_config = ctx.config if isinstance(ctx.config, dict) else {}
        raw_collections = raw_config.get("collections")
        raw_collections = raw_collections if isinstance(raw_collections, dict) else {}

        def collection_name(key: str, fallback: str) -> str:
            value = raw_collections.get(key)
            if isinstance(value, dict) and isinstance(value.get("name"), str):
                return value["name"]
            return fallback

        binding_db = (ctx.bindings or {}).get("db")
        binding_db = binding_db if isinstance(binding_db, dict) else {}
        database = (
            binding_db.get("database")
            or raw_config.get("database")
            or "openehr_cdr"
        )
        canonical = (
            raw_config.get("canonical_collection")
            or collection_name("canonical", "compositions")
        )
        document_fields = strategy_cfg.fields.document
        node_fields = strategy_cfg.fields.node
        atlas_index = strategy_cfg.collections.search.atlasIndex
        return CompositionCollectionNames(
            database=str(database),
            compositions=str(canonical),
            flatten_compositions=strategy_cfg.collections.compositions.name,
            search_compositions=strategy_cfg.collections.search.name,
            contributions=collection_name("contributions", "contributions"),
            ehr=collection_name("ehr", "ehr"),
            dictionaries=strategy_cfg.collections.codes.name,
            atlas_index_name=atlas_index.name if atlas_index else None,
            merge_search_docs=bool(raw_config.get("search_compositions_merge", False)),
            composition_fields=FieldMappingConfig(
                nodes=document_fields.cn,
                data=node_fields.data,
                path=node_fields.p,
                ehr_id=document_fields.ehr_id,
                comp_id=document_fields.comp_id,
                template_id=document_fields.tid,
                version=document_fields.v,
            ),
            search_fields=FieldMappingConfig(
                nodes=document_fields.sn,
                data=node_fields.data,
                path=node_fields.p,
                ehr_id=document_fields.ehr_id,
                comp_id=document_fields.comp_id,
                template_id=document_fields.tid,
                score="score",
            ),
        )

    async def execute_authoritative_write(
        self,
        ctx: StrategyContext,
        request: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create or correct a native openEHR COMPOSITION.

        This delegates to the same version/contribution transaction used by the
        openEHR API. It is intentionally not implemented through ``ingest``.
        """

        behavior = str(request.get("behavior") or "").lower()
        if behavior not in {"create", "update"}:
            raise KehrnelError(
                code="AUTHORITATIVE_WRITE_BEHAVIOR_UNSUPPORTED",
                status=422,
                message="RPS Dual supports governed create and update writes only.",
                details={"behavior": behavior},
            )
        payload = request.get("requestPayload")
        payload = payload if isinstance(payload, dict) else {}
        ehr_id = payload.get("ehrId")
        composition_payload = payload.get("composition")
        idempotency_key = request.get("idempotencyKey")
        if not isinstance(ehr_id, str) or not ehr_id.strip():
            raise KehrnelError(
                code="AUTHORITATIVE_WRITE_EHR_REQUIRED",
                status=422,
                message="requestPayload.ehrId is required for an openEHR write.",
            )
        if not isinstance(composition_payload, dict):
            raise KehrnelError(
                code="AUTHORITATIVE_WRITE_COMPOSITION_REQUIRED",
                status=422,
                message="requestPayload.composition must be a canonical openEHR COMPOSITION.",
            )
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise KehrnelError(
                code="AUTHORITATIVE_WRITE_IDEMPOTENCY_REQUIRED",
                status=422,
                message="An idempotency key is required for authoritative writes.",
            )

        storage = (ctx.adapters or {}).get("storage")
        db = getattr(storage, "db", None)
        if db is None:
            raise KehrnelError(
                code="STORAGE_NOT_AVAILABLE",
                status=503,
                message="The activated strategy has no MongoDB storage adapter.",
            )

        try:
            composition_create = CompositionCreate.model_validate(composition_payload)
            strategy_cfg = normalize_config(ctx.config or {})
            config = self._native_composition_config(ctx, strategy_cfg)
            flattener = await self._build_flattener_for_context(ctx)
            committer_name = str(
                payload.get("committerName")
                or request.get("requesterRef")
                or "Kehrnel governed runtime"
            )
            if behavior == "create":
                deterministic_id = uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"kehrnel:{ctx.environment_id}:{idempotency_key.strip()}",
                )
                composition_id_encoding = normalize_id_encoding(
                    strategy_cfg.ids.composition_id
                )
                object_id = (
                    deterministic_id.hex[:24]
                    if composition_id_encoding == "objectid"
                    else str(deterministic_id)
                )
                composition = await add_composition(
                    ehr_id=ehr_id.strip(),
                    composition_create=composition_create,
                    db=db,
                    config=config,
                    flattener=flattener,
                    merge_search_docs=config.merge_search_docs,
                    committer_name=committer_name,
                    composition_object_id=object_id,
                )
                change_type = "creation"
            else:
                preceding_uid = payload.get("precedingVersionUid")
                if_match = payload.get("ifMatch")
                if not isinstance(preceding_uid, str) or not preceding_uid.strip():
                    raise KehrnelError(
                        code="AUTHORITATIVE_WRITE_PRECEDING_VERSION_REQUIRED",
                        status=422,
                        message="requestPayload.precedingVersionUid is required for correction.",
                    )
                if not isinstance(if_match, str) or not if_match.strip():
                    raise KehrnelError(
                        code="AUTHORITATIVE_WRITE_IF_MATCH_REQUIRED",
                        status=422,
                        message="requestPayload.ifMatch is required for correction.",
                    )
                composition = await update_composition(
                    ehr_id=ehr_id.strip(),
                    preceding_version_uid=preceding_uid.strip(),
                    if_match=if_match.strip(),
                    new_composition_data=composition_create,
                    db=db,
                    config=config,
                    flattener=flattener,
                    merge_search_docs=config.merge_search_docs,
                    committer_name=committer_name,
                    idempotency_key=idempotency_key.strip(),
                )
                change_type = "modification"
        except KehrnelError:
            raise
        except HTTPException as exc:
            raise KehrnelError(
                code="AUTHORITATIVE_WRITE_REJECTED",
                status=exc.status_code,
                message=str(exc.detail),
            ) from exc
        except Exception as exc:
            raise KehrnelError(
                code="AUTHORITATIVE_WRITE_INVALID",
                status=422,
                message=str(exc),
            ) from exc

        version_uid = composition.uid
        version = version_uid.rsplit("::", 1)[-1]
        return {
            "resourceType": "openEHR COMPOSITION",
            "ehrId": ehr_id.strip(),
            "versionUid": version_uid,
            "version": version,
            "etag": f'"{version_uid}"',
            "changeType": change_type,
            "acknowledgement": "durable_authoritative",
        }

    async def run_op(self, ctx: StrategyContext, op: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        op_lower = op.lower()
        cfg = ctx.config
        try:
            strategy_cfg = normalize_config(cfg)
        except Exception as exc:
            if op_lower == "rebuild_slim_search_collection":
                raise KehrnelError(
                    code="SEARCH_REBUILD_CONFIG_INVALID",
                    status=400,
                    message="The active RPS Dual configuration is invalid for a search rebuild.",
                    details={"reason": str(exc)},
                ) from exc
            raise
        adapters = ctx.adapters or {}
        index_admin = adapters.get("index_admin")
        storage = adapters.get("storage")
        atlas = adapters.get("atlas_search") or adapters.get("text_search")

        if op_lower in {"list_native_ehrs", "list_native_compositions", "fetch_native_composition"}:
            if not storage:
                raise KehrnelError(
                    code="STORAGE_NOT_AVAILABLE",
                    status=503,
                    message="storage adapter not available for native composition browsing",
                )

            collection = strategy_cfg.collections.compositions.name
            fields = strategy_cfg.fields.document
            normalize_binary_uuid = _uses_uuid_binary_ids(ctx.config)

            def _limit(default: int, maximum: int) -> int:
                try:
                    return max(1, min(int(payload.get("limit", default)), maximum))
                except (TypeError, ValueError):
                    return default

            def _normalize(value: Any, key: str) -> Any:
                normalized = _normalize_result_value(
                    value,
                    key,
                    normalize_binary_uuid=normalize_binary_uuid,
                )
                if normalized is None or isinstance(normalized, (str, int, float, bool)):
                    return normalized
                return str(normalized)

            if op_lower == "list_native_ehrs":
                rows = await storage.aggregate(
                    collection,
                    [
                        {"$match": {fields.ehr_id: {"$exists": True, "$ne": None}}},
                        {
                            "$group": {
                                "_id": f"${fields.ehr_id}",
                                "time_created": {"$min": f"${fields.time_committed}"},
                            }
                        },
                        {"$sort": {"_id": 1}},
                        {"$limit": _limit(1000, 5000)},
                    ],
                )
                return {
                    "ok": True,
                    "records": [
                        {
                            "ehr_id": _normalize(row.get("_id"), "ehr_id"),
                            "time_created": row.get("time_created"),
                        }
                        for row in rows
                        if row.get("_id") is not None
                    ],
                }

            ehr_id = str(payload.get("ehr_id") or "").strip()
            if not ehr_id:
                raise KehrnelError(
                    code="INVALID_INPUT",
                    status=400,
                    message="ehr_id is required",
                )
            ehr_value = ValueFormatter.format_id_value(ehr_id, strategy_cfg.ids.ehr_id)

            if op_lower == "list_native_compositions":
                rows = await storage.aggregate(
                    collection,
                    [
                        {"$match": {fields.ehr_id: ehr_value}},
                        {"$sort": {fields.time_committed: -1, fields.comp_id: 1}},
                        {"$limit": _limit(500, 2000)},
                        {
                            "$project": {
                                "_id": 0,
                                "uid": f"${fields.comp_id}",
                                "template_id": f"${fields.tid}",
                                "time_created": f"${fields.time_committed}",
                            }
                        },
                    ],
                )
                return {
                    "ok": True,
                    "records": [
                        {
                            **row,
                            "uid": _normalize(row.get("uid"), "composition_id"),
                        }
                        for row in rows
                        if row.get("uid") is not None
                    ],
                }

            uid = str(payload.get("uid") or "").strip()
            if not uid:
                raise KehrnelError(
                    code="INVALID_INPUT",
                    status=400,
                    message="uid is required",
                )
            uid_value = ValueFormatter.format_id_value(uid, strategy_cfg.ids.composition_id)
            base_doc = await storage.find_one(
                collection,
                {fields.ehr_id: ehr_value, fields.comp_id: uid_value},
                {"_id": 0},
            )
            if not base_doc:
                raise KehrnelError(
                    code="COMPOSITION_NOT_FOUND",
                    status=404,
                    message=f"Composition '{uid}' was not found for EHR '{ehr_id}'",
                )
            normalized_doc = _normalize_result_value(
                base_doc,
                normalize_binary_uuid=normalize_binary_uuid,
            )
            return {"ok": True, "composition": normalized_doc}

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
                    payload = await resolve_uri_async(seed_uri, motor_db, Path(__file__).parent)
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
                seed_payload = await resolve_uri_async(dict_seed, motor_db, Path(__file__).parent) if dict_seed else None
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
                seed_payload = await resolve_uri_async(shortcuts_seed, motor_db, Path(__file__).parent) if shortcuts_seed else None
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

        if op_lower == "prepare_search_projection_dictionary":
            storage = (ctx.adapters or {}).get("storage")
            if not storage or getattr(storage, "db", None) is None:
                raise KehrnelError(
                    code="SEARCH_PROJECTION_DICTIONARY_UNAVAILABLE",
                    status=503,
                    message=(
                        "The storage adapter must expose its MongoDB database to "
                        "prepare the search projection dictionary."
                    ),
                )

            dry_run = bool((payload or {}).get("dry_run", False))
            preparation_meta = dict(ctx.meta or {})
            bulk_config = dict(preparation_meta.get("bulk_config") or {})
            # Dictionary allocation is an explicit maintenance action. Normal
            # projection compilation remains secondary/read-only and fails
            # closed when a mapping has not been prepared.
            bulk_config["role"] = "primary"
            preparation_meta["bulk_config"] = bulk_config
            preparation_ctx = StrategyContext(
                environment_id=ctx.environment_id,
                config=ctx.config,
                bindings=ctx.bindings,
                adapters=ctx.adapters,
                manifest=ctx.manifest,
                trace_id=ctx.trace_id,
                logger=ctx.logger,
                meta=preparation_meta,
            )
            flattener = await self._build_flattener_for_context(preparation_ctx)
            before_ar = len(flattener.code_book.get("ar_code") or {})
            before_at = len(flattener.code_book.get("at") or {})
            invalid_templates: list[Dict[str, Any]] = []
            compiled_field_count = 0
            for template_id, fields in sorted(flattener.template_fields.items()):
                compiled_rules = flattener._compiled_rules_for_template(template_id)
                compiled_field_count += len(compiled_rules)
                if len(compiled_rules) != len(fields):
                    invalid_templates.append(
                        {
                            "templateId": template_id,
                            "fieldCount": len(fields),
                            "compiledRuleCount": len(compiled_rules),
                        }
                    )
            if not flattener.template_fields or invalid_templates:
                raise KehrnelError(
                    code="SEARCH_REBUILD_CONFIG_INVALID",
                    status=400,
                    message=(
                        "The active projection mapping is empty or cannot be "
                        "compiled completely."
                    ),
                    details={
                        "templateCount": len(flattener.template_fields),
                        "invalidTemplates": invalid_templates,
                    },
                )

            after_ar = len(flattener.code_book.get("ar_code") or {})
            after_at = len(flattener.code_book.get("at") or {})
            if not dry_run:
                await flattener.flush_codes_to_db()
            return {
                "ok": True,
                "dryRun": dry_run,
                "templateCount": len(flattener.template_fields),
                "fieldCount": compiled_field_count,
                "dictionary": {
                    "archetypeCodesBefore": before_ar,
                    "archetypeCodesAfter": after_ar,
                    "atCodesBefore": before_at,
                    "atCodesAfter": after_at,
                    "persisted": not dry_run,
                },
            }

        if op_lower == "recover_search_projection_rebuild":
            if not storage:
                raise KehrnelError(
                    code="STORAGE_NOT_AVAILABLE",
                    status=503,
                    message="storage adapter not available for rebuild recovery",
                )
            find_one = getattr(storage, "find_one", None)
            update_one = getattr(storage, "update_one", None)
            if not callable(find_one) or not callable(update_one):
                raise KehrnelError(
                    code="SEARCH_REBUILD_RECOVERY_UNAVAILABLE",
                    status=503,
                    message="Rebuild recovery requires find_one and update_one support.",
                )
            raw_minimum_age = (payload or {}).get("minimum_age_seconds", 300)
            if isinstance(raw_minimum_age, bool):
                raw_minimum_age = 0
            try:
                minimum_age_seconds = int(raw_minimum_age)
            except (TypeError, ValueError) as exc:
                raise KehrnelError(
                    code="SEARCH_REBUILD_RECOVERY_INVALID",
                    status=400,
                    message="minimum_age_seconds must be an integer between 1 and 86400.",
                ) from exc
            if minimum_age_seconds < 1 or minimum_age_seconds > 86400:
                raise KehrnelError(
                    code="SEARCH_REBUILD_RECOVERY_INVALID",
                    status=400,
                    message="minimum_age_seconds must be an integer between 1 and 86400.",
                )
            requested_template_ids = _normalize_projection_template_scope(
                (payload or {}).get("template_ids")
            )
            state_id = _projection_state_id(
                ctx.environment_id,
                strategy_cfg.collections.search.name,
                requested_template_ids,
            )
            state = await find_one(_SEARCH_REBUILD_STATE_COLLECTION, {"_id": state_id})
            lease = state.get("rebuildLease") if isinstance(state, dict) else None
            if not isinstance(lease, dict) or lease.get("active") is not True:
                return {"ok": True, "recovered": False, "reason": "no_active_lease"}
            acquired_at = lease.get("acquiredAt")
            owner = lease.get("owner")
            try:
                acquired = datetime.fromisoformat(
                    str(acquired_at).replace("Z", "+00:00")
                )
            except (TypeError, ValueError) as exc:
                raise KehrnelError(
                    code="SEARCH_REBUILD_RECOVERY_UNSAFE",
                    status=409,
                    message="The active rebuild lease has no verifiable acquisition time.",
                ) from exc
            age_seconds = int((datetime.now(timezone.utc) - acquired).total_seconds())
            if age_seconds < minimum_age_seconds:
                raise KehrnelError(
                    code="SEARCH_REBUILD_LEASE_STILL_FRESH",
                    status=409,
                    message="The active rebuild lease is too recent to recover safely.",
                    details={
                        "ageSeconds": max(age_seconds, 0),
                        "minimumAgeSeconds": minimum_age_seconds,
                    },
                )
            mutation_sequence = state.get("mutationSequence")
            checkpoint = state.get("checkpoint")
            if (
                not isinstance(owner, str)
                or not owner
                or not isinstance(mutation_sequence, int)
                or not isinstance(checkpoint, dict)
            ):
                raise KehrnelError(
                    code="SEARCH_REBUILD_RECOVERY_UNSAFE",
                    status=409,
                    message="The active rebuild lease cannot be reconciled to a checkpoint.",
                )
            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            result = await update_one(
                _SEARCH_REBUILD_STATE_COLLECTION,
                {
                    "_id": state_id,
                    "mutationSequence": mutation_sequence,
                    "activeMutations": 0,
                    "rebuildLease.active": True,
                    "rebuildLease.owner": owner,
                },
                {
                    "$set": {
                        "rebuildLease.active": False,
                        "rebuildLease.releasedAt": now,
                        "rebuildLease.releaseReason": "recovered_interrupted_rebuild",
                        "projection.state": "stale",
                        "projection.complete": False,
                        "reconciliation.safe": False,
                        "reconciliation.complete": False,
                        "checkpoint.mutationSequence": mutation_sequence,
                        "checkpoint.updatedAt": now,
                        "consistency.mutationSequence": mutation_sequence,
                        "consistency.activeMutations": 0,
                    }
                },
                upsert=False,
            )
            if _matched_count(result) == 0:
                raise KehrnelError(
                    code="SEARCH_REBUILD_RECOVERY_RACE",
                    status=409,
                    message="The rebuild lease changed while recovery was attempted.",
                )
            return {
                "ok": True,
                "recovered": True,
                "ageSeconds": max(age_seconds, 0),
                "checkpoint": {
                    "processedCount": checkpoint.get("processedCount"),
                    "hasNextCursor": bool(checkpoint.get("nextCursor")),
                },
            }

        if op_lower == "semantic_projection_preflight":
            return await self._semantic_projection_preflight(
                ctx,
                strategy_cfg,
                payload or {},
            )

        if op_lower == "operational_probe":
            return await self._operational_probe(ctx, strategy_cfg, payload)


        if op_lower == "rebuild_slim_search_collection":
            if not storage:
                raise KehrnelError(
                    code="STORAGE_NOT_AVAILABLE",
                    status=503,
                    message="storage adapter not available for slim-search rebuild",
                )
            comp_coll = strategy_cfg.collections.compositions.name
            search_coll_name = strategy_cfg.collections.search.name
            if (
                not strategy_cfg.collections.search.enabled
                or not comp_coll
                or not search_coll_name
                or comp_coll == search_coll_name
            ):
                raise KehrnelError(
                    code="SEARCH_REBUILD_CONFIG_INVALID",
                    status=400,
                    message="Distinct source and enabled slim-search collections are required.",
                    details={
                        "sourceCollection": comp_coll,
                        "targetCollection": search_coll_name,
                        "searchEnabled": strategy_cfg.collections.search.enabled,
                    },
                )

            replace_one = getattr(storage, "replace_one", None)
            find_one = getattr(storage, "find_one", None)
            if not callable(replace_one):
                raise KehrnelError(
                    code="SEARCH_REBUILD_UPSERT_UNAVAILABLE",
                    status=503,
                    message="The storage adapter must support idempotent replace_one(..., upsert=True).",
                )
            if not callable(find_one):
                raise KehrnelError(
                    code="SEARCH_REBUILD_STATE_UNAVAILABLE",
                    status=503,
                    message="The storage adapter must support find_one for rebuild checkpoints.",
                )

            raw_batch_size = (payload or {}).get("batch_size", 100)
            if isinstance(raw_batch_size, bool):
                raw_batch_size = 0
            try:
                batch_size = int(raw_batch_size)
            except (TypeError, ValueError) as exc:
                raise KehrnelError(
                    code="SEARCH_REBUILD_BATCH_INVALID",
                    status=400,
                    message="batch_size must be an integer between 1 and 1000.",
                ) from exc
            if batch_size < 1 or batch_size > _SEARCH_REBUILD_MAX_BATCH_SIZE:
                raise KehrnelError(
                    code="SEARCH_REBUILD_BATCH_INVALID",
                    status=400,
                    message="batch_size must be an integer between 1 and 1000.",
                    details={"batchSize": batch_size, "maximum": _SEARCH_REBUILD_MAX_BATCH_SIZE},
                )

            try:
                await self.validate_config(ctx)
                projection_bundle = await self._build_active_search_projection(
                    ctx,
                    strategy_cfg,
                )
            except KehrnelError:
                raise
            except Exception as exc:
                raise KehrnelError(
                    code="SEARCH_REBUILD_CONFIG_INVALID",
                    status=400,
                    message="The active projection mapping could not be prepared for rebuild.",
                    details={"reason": str(exc)},
                ) from exc

            flattener = projection_bundle["flattener"]
            active_mappings = projection_bundle["activeMappings"]
            all_active_templates = {
                item["templateId"]: item["fields"]
                for item in active_mappings["templates"]
            }
            requested_template_ids = _normalize_projection_template_scope(
                (payload or {}).get("template_ids")
            )
            if requested_template_ids is not None:
                missing_template_ids = sorted(
                    set(requested_template_ids) - set(all_active_templates)
                )
                if missing_template_ids:
                    raise KehrnelError(
                        code="SEARCH_REBUILD_SCOPE_UNKNOWN",
                        status=400,
                        message=(
                            "Every requested template must have an active Analytics "
                            "projection mapping."
                        ),
                        details={"unknownTemplateIds": missing_template_ids},
                    )
                active_templates = {
                    template_id: all_active_templates[template_id]
                    for template_id in requested_template_ids
                }
            else:
                active_templates = all_active_templates
            scoped_rebuild = requested_template_ids is not None
            scope_key = _projection_scope_key(requested_template_ids)
            scope_document = _projection_scope_document(requested_template_ids)
            index_definition = projection_bundle["indexDefinition"]
            index_metadata = projection_bundle["indexMetadata"]
            projection_revision = projection_bundle["projectionRevision"]
            projection_summary = {
                "contract": "openehr.rps-dual.search-projection-readiness/v1",
                "revision": projection_revision,
                "projectionAlgorithmVersion": _SEARCH_PROJECTION_ALGORITHM_VERSION,
                "mappingDigest": projection_bundle["mappingDigest"],
                "indexDefinitionDigest": projection_bundle["indexDefinitionDigest"],
                "templateIds": sorted(active_templates),
                "templateCount": len(active_templates),
                "fieldCount": sum(len(fields) for fields in active_templates.values()),
                "indexFieldCount": int(index_metadata.get("data_field_count") or 0),
                "templatePaths": {
                    template_id: projection_bundle["templatePaths"].get(
                        template_id,
                        [],
                    )
                    for template_id in active_templates
                },
                "templateProvenance": {
                    template_id: projection_bundle["templateProvenance"].get(
                        template_id,
                        {},
                    )
                    for template_id in active_templates
                },
            }

            input_cursor = (payload or {}).get("cursor")
            after_id = None
            if input_cursor is not None:
                after_id = _decode_search_rebuild_cursor(
                    input_cursor,
                    source_collection=comp_coll,
                    target_collection=search_coll_name,
                    projection_revision=projection_revision,
                    scope_key=scope_key,
                )

            state_id, mutation_sequence = await self._ensure_projection_mutation_fence(
                ctx,
                strategy_cfg,
                template_ids=requested_template_ids,
            )
            previous_state = await find_one(
                _SEARCH_REBUILD_STATE_COLLECTION,
                {"_id": state_id},
            )
            previous_projection = (
                previous_state.get("projection")
                if isinstance(previous_state, dict)
                and isinstance(previous_state.get("projection"), dict)
                else {}
            )
            previous_checkpoint = (
                previous_state.get("checkpoint")
                if isinstance(previous_state, dict)
                and previous_projection.get("revision") == projection_revision
                and isinstance(previous_state.get("checkpoint"), dict)
                else {}
            )
            checkpoint_mutation_sequence = previous_checkpoint.get(
                "mutationSequence"
            )
            checkpoint_fence_matches = bool(
                previous_checkpoint
                and isinstance(checkpoint_mutation_sequence, int)
                and checkpoint_mutation_sequence == mutation_sequence
            )
            if previous_checkpoint and not checkpoint_fence_matches:
                if input_cursor is not None:
                    raise KehrnelError(
                        code="SEARCH_REBUILD_CURSOR_STALE",
                        status=409,
                        message=(
                            "A governed base mutation or interrupted rebuild changed "
                            "the projection after this cursor was issued. Restart the "
                            "rebuild without a cursor."
                        ),
                        details={
                            "checkpointMutationSequence": checkpoint_mutation_sequence,
                            "currentMutationSequence": mutation_sequence,
                        },
                    )
                # A cursorless request is the explicit safe restart path.  Do not
                # reuse counts or a run id from a generation whose fence changed.
                previous_checkpoint = {}
            retrying_current_page = bool(
                previous_checkpoint
                and "inputCursor" in previous_checkpoint
                and previous_checkpoint.get("inputCursor") == input_cursor
                and not (
                    input_cursor is None
                    and previous_projection.get("complete") is True
                )
            )
            continuing_from_previous = bool(
                input_cursor is not None
                and previous_checkpoint
                and previous_checkpoint.get("nextCursor") == input_cursor
            )
            previous_run_id = (
                previous_state.get("rebuildRunId")
                if isinstance(previous_state, dict)
                else None
            )
            rebuild_run_id = (
                previous_run_id
                if (retrying_current_page or continuing_from_previous)
                and isinstance(previous_run_id, str)
                and previous_run_id
                else str(uuid.uuid4())
            )
            if (
                input_cursor is not None
                and not retrying_current_page
                and not continuing_from_previous
            ):
                raise KehrnelError(
                    code="SEARCH_REBUILD_CURSOR_STALE",
                    status=409,
                    message=(
                        "The slim-search rebuild cursor is not the current continuation "
                        "or retry checkpoint. Restart without a cursor."
                    ),
                )

            template_field = strategy_cfg.fields.document.tid
            source_pipeline: list[Dict[str, Any]] = []
            if scoped_rebuild:
                source_pipeline.append(
                    {
                        "$match": _mapped_template_filter(
                            template_field,
                            sorted(active_templates),
                        )
                    }
                )
            if after_id is not None:
                source_pipeline.append({"$match": {"_id": {"$gt": after_id}}})
            source_pipeline.extend(
                [
                    {"$sort": {"_id": 1}},
                    {"$limit": batch_size + 1},
                ]
            )
            fetched = await storage.aggregate(comp_coll, source_pipeline)
            if not isinstance(fetched, list):
                raise KehrnelError(
                    code="SEARCH_REBUILD_SOURCE_INVALID",
                    status=500,
                    message="The storage adapter returned an invalid rebuild batch.",
                )
            has_more = len(fetched) > batch_size
            docs = fetched[:batch_size]

            projected: list[Dict[str, Any]] = []
            skipped_empty = 0
            skipped_unmapped = 0
            mapped_empty = 0
            stale_empty_sidecars = 0
            for doc in docs:
                if not isinstance(doc, dict) or doc.get("_id") is None:
                    raise KehrnelError(
                        code="SEARCH_REBUILD_SOURCE_INVALID",
                        status=422,
                        message="Every source composition must be a document with an _id.",
                    )
                template_id = doc.get(flattener.cf_tmpl)
                template_is_mapped = bool(
                    isinstance(template_id, str)
                    and flattener._get_simple_fields_for(template_id)
                )
                # Existing stores can legitimately contain legacy or otherwise
                # unrelated compositions that predate the active projection
                # catalog. They are outside this rebuild's scope. Preserve them
                # in canonical storage, but still detect a stale sidecar so the
                # final readiness receipt cannot claim unsafe coverage.
                if not template_is_mapped:
                    skipped_empty += 1
                    skipped_unmapped += 1
                    # Do not issue one target lookup per out-of-scope legacy
                    # source.  The bounded target-side reconciliation probe at
                    # completion detects any sidecar whose template is not in
                    # the active mapping catalog and keeps readiness fail-closed.
                    continue
                try:
                    search_doc = flattener.project_search_document(doc)
                except Exception as exc:
                    raise KehrnelError(
                        code="SEARCH_REBUILD_SOURCE_INVALID",
                        status=422,
                        message="A flattened composition could not be projected safely.",
                        details={"sourceId": str(doc.get("_id")), "reason": str(exc)},
                    ) from exc
                if search_doc is None:
                    skipped_empty += 1
                    mapped_empty += 1
                    target_id = doc.get(flattener.cf_cid, doc.get("_id"))
                    if target_id is not None and await find_one(
                        search_coll_name,
                        {"_id": target_id},
                        {"_id": 1},
                    ):
                        stale_empty_sidecars += 1
                    continue
                search_nodes = search_doc.get(strategy_cfg.fields.document.sn)
                if not isinstance(search_nodes, list) or not search_nodes:
                    skipped_empty += 1
                    if template_is_mapped:
                        mapped_empty += 1
                    else:
                        skipped_unmapped += 1
                    target_id = search_doc.get("_id")
                    if target_id is not None and await find_one(
                        search_coll_name,
                        {"_id": target_id},
                        {"_id": 1},
                    ):
                        stale_empty_sidecars += 1
                    continue
                if search_doc.get("_id") is None:
                    raise KehrnelError(
                        code="SEARCH_REBUILD_SOURCE_INVALID",
                        status=422,
                        message="A projected slim-search document has no stable _id.",
                        details={"sourceId": str(doc.get("_id"))},
                    )
                projected.append(search_doc)

            # The non-ready state and exclusive lease are published atomically
            # before the first sidecar changes.  This closes the window in which
            # preflight could otherwise accept a prior ready receipt while a
            # rebuild was already mutating the search collection.
            rebuild_owner, mutation_sequence = await self._claim_projection_rebuild(
                storage,
                state_id=state_id,
                mutation_sequence=mutation_sequence,
                rebuild_run_id=rebuild_run_id,
                previous_state=(
                    previous_state if isinstance(previous_state, dict) else {}
                ),
                projection_summary=projection_summary,
            )

            try:
                replace_many = getattr(storage, "replace_many", None)
                if projected and callable(replace_many):
                    # MongoDB's unordered bulk replacement preserves the same
                    # idempotent _id contract while avoiding one network round
                    # trip per sidecar during large tenant rebuilds.
                    await replace_many(search_coll_name, projected)
                else:
                    for search_doc in projected:
                        await replace_one(
                            search_coll_name,
                            {"_id": search_doc["_id"]},
                            search_doc,
                            upsert=True,
                        )
            except Exception:
                await self._abandon_projection_rebuild(
                    storage,
                    state_id=state_id,
                    rebuild_owner=rebuild_owner,
                    reason="sidecar_write_failed",
                )
                raise

            last_cursor = input_cursor
            if docs:
                last_cursor = _encode_search_rebuild_cursor(
                    docs[-1]["_id"],
                    source_collection=comp_coll,
                    target_collection=search_coll_name,
                    projection_revision=projection_revision,
                    scope_key=scope_key,
                )
            next_cursor = last_cursor if has_more else None
            complete = not has_more

            missing_sidecar_probe: list[Dict[str, Any]] = []
            orphan_probe: list[Dict[str, Any]] = []
            unmapped_sidecar_probe: list[Dict[str, Any]] = []
            if complete:
                id_field = strategy_cfg.fields.document.comp_id
                template_field = strategy_cfg.fields.document.tid
                index_admin = (ctx.adapters or {}).get("index_admin")
                ensure_indexes = getattr(index_admin, "ensure_indexes", None)
                if callable(ensure_indexes):
                    source_index_result = await ensure_indexes(
                        comp_coll,
                        [
                            {
                                "keys": [(id_field, 1)],
                                "options": {
                                    "name": "kehrnel_projection_source_comp_id"
                                },
                            },
                            {
                                "keys": [(template_field, 1)],
                                "options": {
                                    "name": "kehrnel_projection_source_template"
                                },
                            },
                        ],
                    )
                    target_index_result = await ensure_indexes(
                        search_coll_name,
                        [
                            {
                                "keys": [(template_field, 1)],
                                "options": {
                                    "name": "kehrnel_projection_target_template"
                                },
                            }
                        ],
                    )
                    maintenance_warnings = [
                        *list((source_index_result or {}).get("warnings") or []),
                        *list((target_index_result or {}).get("warnings") or []),
                    ]
                    if maintenance_warnings:
                        await self._abandon_projection_rebuild(
                            storage,
                            state_id=state_id,
                            rebuild_owner=rebuild_owner,
                            reason="maintenance_index_failed",
                        )
                        raise KehrnelError(
                            code="SEARCH_REBUILD_MAINTENANCE_INDEX_FAILED",
                            status=503,
                            message=(
                                "Projection reconciliation indexes could not be "
                                "prepared safely."
                            ),
                        )
                mapped_filter = _mapped_template_filter(
                    template_field,
                    sorted(active_templates),
                )
                try:
                    missing_sidecar_probe = await storage.aggregate(
                        comp_coll,
                        [
                            {"$match": mapped_filter},
                            {
                                "$lookup": {
                                    "from": search_coll_name,
                                    "localField": id_field,
                                    "foreignField": "_id",
                                    "as": "__kehrnel_target",
                                }
                            },
                            {"$match": {"__kehrnel_target.0": {"$exists": False}}},
                            {"$sort": {"_id": 1}},
                            {"$limit": 1},
                            {
                                "$project": {
                                    "_id": 1,
                                    id_field: 1,
                                    template_field: 1,
                                }
                            },
                        ],
                    )
                    orphan_probe = await storage.aggregate(
                        search_coll_name,
                        [
                            *([{"$match": mapped_filter}] if scoped_rebuild else []),
                            {
                                "$lookup": {
                                    "from": comp_coll,
                                    "localField": "_id",
                                    "foreignField": id_field,
                                    "as": "__kehrnel_source",
                                }
                            },
                            {"$match": {"__kehrnel_source.0": {"$exists": False}}},
                            {"$sort": {"_id": 1}},
                            {"$limit": 1},
                            {"$project": {"_id": 1}},
                        ],
                    )
                    if not scoped_rebuild:
                        unmapped_sidecar_probe = await storage.aggregate(
                            search_coll_name,
                            [
                                {
                                    "$match": _mapped_template_filter(
                                        template_field,
                                        sorted(active_templates),
                                        negate=True,
                                    )
                                },
                                {"$sort": {"_id": 1}},
                                {"$limit": 1},
                                {
                                    "$project": {
                                        "_id": 1,
                                        id_field: 1,
                                        template_field: 1,
                                    }
                                },
                            ],
                        )
                except Exception:
                    await self._abandon_projection_rebuild(
                        storage,
                        state_id=state_id,
                        rebuild_owner=rebuild_owner,
                        reason="reconciliation_failed",
                    )
                    raise
                if not all(
                    isinstance(probe, list)
                    for probe in (
                        missing_sidecar_probe,
                        orphan_probe,
                        unmapped_sidecar_probe,
                    )
                ):
                    await self._abandon_projection_rebuild(
                        storage,
                        state_id=state_id,
                        rebuild_owner=rebuild_owner,
                        reason="reconciliation_invalid",
                    )
                    raise KehrnelError(
                        code="SEARCH_REBUILD_RECONCILIATION_INVALID",
                        status=500,
                        message="The storage adapter returned an invalid projection coverage probe.",
                    )

            warnings = list(projection_bundle.get("warnings") or [])
            atlas_idx = strategy_cfg.collections.search.atlasIndex
            index_receipt: Dict[str, Any] = {
                "status": "deferred_until_rebuild_complete" if not complete else "not_configured",
                "name": atlas_idx.name if atlas_idx else None,
            }
            previous_stale_empty = int(
                previous_checkpoint.get("staleEmptySidecarCount") or 0
            )
            previous_mapped_empty = int(
                previous_checkpoint.get("mappedEmptyCount") or 0
            )
            previous_skipped_unmapped = int(
                previous_checkpoint.get("skippedUnmappedCount") or 0
            )
            if retrying_current_page:
                total_stale_empty = previous_stale_empty
                total_mapped_empty = previous_mapped_empty
                total_skipped_unmapped = previous_skipped_unmapped
            elif continuing_from_previous:
                total_stale_empty = previous_stale_empty + stale_empty_sidecars
                total_mapped_empty = previous_mapped_empty + mapped_empty
                total_skipped_unmapped = (
                    previous_skipped_unmapped + skipped_unmapped
                )
            else:
                total_stale_empty = stale_empty_sidecars
                total_mapped_empty = mapped_empty
                total_skipped_unmapped = skipped_unmapped
            reconciliation_safe = bool(
                complete
                and not missing_sidecar_probe
                and not orphan_probe
                and not unmapped_sidecar_probe
                and total_stale_empty == 0
                and total_mapped_empty == 0
            )

            if complete and not reconciliation_safe:
                index_receipt = {
                    "status": "deferred_until_projection_reconciled",
                    "name": atlas_idx.name if atlas_idx else None,
                }
            elif complete and atlas and atlas_idx and atlas_idx.name:
                try:
                    res = await atlas.ensure_search_index(
                        search_coll_name,
                        atlas_idx.name,
                        index_definition,
                    )
                except Exception as exc:
                    res = {"created": [], "updated": [], "warnings": [str(exc)]}
                ensure_warnings = list((res or {}).get("warnings") or [])
                warnings.extend(ensure_warnings)
                ensured = bool(
                    isinstance(res, dict)
                    and not ensure_warnings
                    and (
                        res.get("ok") is True
                        or atlas_idx.name in (res.get("created") or [])
                        or atlas_idx.name in (res.get("updated") or [])
                    )
                )
                index_receipt = {
                    "status": "ensured" if ensured else "ensure_pending",
                    "name": atlas_idx.name,
                    "result": res,
                }
            elif complete and atlas_idx and atlas_idx.name:
                index_receipt = {
                    "status": "adapter_unavailable",
                    "name": atlas_idx.name,
                }
                warnings.append(
                    "atlas_search adapter not available; search index was not ensured"
                )

            if retrying_current_page:
                total_processed = int(previous_checkpoint.get("processedCount") or 0)
                total_upserted = int(previous_checkpoint.get("upsertedCount") or 0)
                total_skipped_empty = int(
                    previous_checkpoint.get("skippedEmptyCount") or 0
                )
            elif continuing_from_previous:
                total_processed = int(previous_checkpoint.get("processedCount") or 0) + len(docs)
                total_upserted = int(previous_checkpoint.get("upsertedCount") or 0) + len(
                    projected
                )
                total_skipped_empty = int(
                    previous_checkpoint.get("skippedEmptyCount") or 0
                ) + skipped_empty
            else:
                total_processed = len(docs)
                total_upserted = len(projected)
                total_skipped_empty = skipped_empty

            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            projection_state = (
                "ready"
                if complete
                and reconciliation_safe
                and index_receipt["status"] in {"ensured", "not_configured"}
                else "stale"
                if complete and not reconciliation_safe
                else "index_pending"
                if complete
                else "building"
            )
            completed_at = None
            if complete:
                completed_at = (
                    previous_projection.get("completedAt")
                    if retrying_current_page
                    and isinstance(previous_projection.get("completedAt"), str)
                    else now
                )
            projection_receipt = {
                **projection_summary,
                "state": projection_state,
                "complete": complete,
                "completedAt": completed_at,
            }
            checkpoint = {
                "inputCursor": input_cursor,
                "lastProcessedCursor": last_cursor,
                "nextCursor": next_cursor,
                "processedCount": total_processed,
                "upsertedCount": total_upserted,
                "skippedEmptyCount": total_skipped_empty,
                "skippedUnmappedCount": total_skipped_unmapped,
                "mappedEmptyCount": total_mapped_empty,
                "staleEmptySidecarCount": total_stale_empty,
                "processedThisBatch": len(docs),
                "mutationSequence": mutation_sequence,
                "updatedAt": now,
            }
            reconciliation = {
                "complete": complete,
                "safe": reconciliation_safe,
                "sourceToTargetCoverage": (
                    "verified"
                    if complete and not missing_sidecar_probe
                    else "mismatch"
                    if complete
                    else "pending"
                ),
                "targetToSourceCoverage": (
                    "verified"
                    if complete and not orphan_probe and not unmapped_sidecar_probe
                    else "mismatch"
                    if complete
                    else "pending"
                ),
                "missingSidecarDetected": bool(missing_sidecar_probe),
                "missingSidecarSourceId": (
                    missing_sidecar_probe[0].get("_id")
                    if missing_sidecar_probe
                    and isinstance(missing_sidecar_probe[0], dict)
                    else None
                ),
                "orphanSidecarDetected": bool(orphan_probe),
                "orphanSidecarId": (
                    orphan_probe[0].get("_id")
                    if orphan_probe and isinstance(orphan_probe[0], dict)
                    else None
                ),
                "unmappedSidecarDetected": bool(unmapped_sidecar_probe),
                "unmappedSidecarId": (
                    unmapped_sidecar_probe[0].get("_id")
                    if unmapped_sidecar_probe
                    and isinstance(unmapped_sidecar_probe[0], dict)
                    else None
                ),
                "mappedEmptyCount": total_mapped_empty,
                "skippedUnmappedCount": total_skipped_unmapped,
                "staleEmptySidecarCount": total_stale_empty,
            }
            consistency = {
                "model": _SEARCH_PROJECTION_CONSISTENCY_MODEL,
                "mutationSequence": mutation_sequence,
                "activeMutations": 0,
                "governedWritersOnly": True,
                "publication": "compare-and-set",
                "assumption": (
                    "All base mutations use the RPS Dual governed ingest fence; "
                    "external writers must invalidate the receipt or be quiesced."
                ),
            }
            state_document = {
                "_id": state_id,
                "contract": "openehr.rps-dual.search-projection-readiness/v1",
                "environmentId": ctx.environment_id,
                "strategyId": "openehr.rps_dual",
                "sourceCollection": comp_coll,
                "targetCollection": search_coll_name,
                "scope": scope_document,
                "mutationSequence": mutation_sequence,
                "activeMutations": 0,
                "rebuildRunId": rebuild_run_id,
                "rebuildLease": {
                    "active": False,
                    "owner": rebuild_owner,
                    "releasedAt": now,
                    "releaseReason": "checkpoint_published",
                },
                "projection": projection_receipt,
                "checkpoint": checkpoint,
                "index": index_receipt,
                "reconciliation": reconciliation,
                "consistency": consistency,
            }
            try:
                await self._publish_projection_state(
                    storage,
                    state_id=state_id,
                    mutation_sequence=mutation_sequence,
                    rebuild_owner=rebuild_owner,
                    state_document=state_document,
                )
            except Exception:
                await self._abandon_projection_rebuild(
                    storage,
                    state_id=state_id,
                    rebuild_owner=rebuild_owner,
                    reason="checkpoint_publication_failed",
                )
                raise

            counts = {
                "fetched": len(fetched),
                "processed": len(docs),
                "upserted": len(projected),
                "skippedEmpty": skipped_empty,
                "skippedUnmapped": skipped_unmapped,
                "mappedEmpty": mapped_empty,
            }
            return {
                "ok": True,
                "sourceCollection": comp_coll,
                "targetCollection": search_coll_name,
                "batchSize": batch_size,
                "processed": len(docs),
                "inserted": len(projected),
                "upserted": len(projected),
                "skipped_empty": skipped_empty,
                "complete": complete,
                "next_cursor": next_cursor,
                "counts": counts,
                "watermark": checkpoint,
                "projection": projection_receipt,
                "index": index_receipt,
                "reconciliation": reconciliation,
                "consistency": consistency,
                "state": {
                    "collection": _SEARCH_REBUILD_STATE_COLLECTION,
                    "id": state_id,
                },
                "warnings": warnings,
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

            # Prefer model_source from metadata (has defaults) over payload
            model_source_from_meta = (ctx.meta or {}).get("model_source") if ctx and ctx.meta else None
            model_source_from_payload = payload.get("model_source") if isinstance(payload.get("model_source"), dict) else {}
            model_source = model_source_from_meta if model_source_from_meta else model_source_from_payload

            models = payload.get("models")
            templates = payload.get("templates")
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
                # Convert templates to models format and resolve with catalog
                template_models = []
                for entry in templates:
                    if not isinstance(entry, dict):
                        raise KehrnelError(code="INVALID_INPUT", status=400, message="each template entry must be an object")
                    template_id = str(entry.get("template_id") or entry.get("templateId") or "").strip()
                    if not template_id:
                        raise KehrnelError(code="INVALID_INPUT", status=400, message="template_id is required in each template entry")
                    template_models.append({
                        "model_id": template_id,
                        "template_id": template_id,
                        "min_per_patient": int(entry.get("min_per_patient", entry.get("min", 1))),
                        "max_per_patient": int(entry.get("max_per_patient", entry.get("max", entry.get("min_per_patient", entry.get("min", 1))))),
                        "weight": float(entry.get("weight", 1.0)),
                        "sample_pool_size": int(entry.get("sample_pool_size", 25) or 25),
                    })
                # Resolve models to fetch catalog documents
                model_specs = await resolve_model_specs(
                    storage,
                    model_source=model_source if isinstance(model_source, dict) else {},
                    requested_models=template_models,
                    domain=(getattr(ctx.manifest, "domain", "") or "openEHR"),
                    strategy_id=getattr(ctx.manifest, "id", None),
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
                write_base = bool(
                    comp_collection
                    and base_batch
                    and (force or len(base_batch) >= write_batch_size)
                )
                write_search = bool(
                    search_enabled
                    and search_collection
                    and search_batch
                    and (force or len(search_batch) >= write_batch_size)
                )
                fenced = bool(write_base and search_enabled and search_collection)
                if fenced:
                    await self._invalidate_projection_readiness(
                        ctx,
                        strategy_cfg,
                        reason="governed_synthetic_ingest",
                    )
                try:
                    if write_base:
                        await storage.insert_many(comp_collection, base_batch)
                        inserted_base += len(base_batch)
                        base_batch.clear()
                    if write_search:
                        await storage.insert_many(search_collection, search_batch)
                        inserted_search += len(search_batch)
                        search_batch.clear()
                finally:
                    if fenced:
                        await self._complete_projection_mutation(
                            ctx,
                            strategy_cfg,
                            reason="governed_synthetic_ingest",
                        )

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
        path = Path(__file__).parent / "bundles" / f"{bundle_id}.json"
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
        or model_doc.get("content")
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

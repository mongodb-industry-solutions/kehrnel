"""FHIR runtime bridge: activation config, Mongo bindings, fhir-gen / fhir-mql clients."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr._paths import SPEC_DIR

DEFAULTS_PATH = SPEC_DIR / "defaults.json"
RECIPES_PATH = SPEC_DIR / "recipes.json"

_LEGACY_RESOURCE_PAIR = re.compile(r"^(?P<resource>[A-Za-z]+)\s*=\s*(?P<count>\d+)$")


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    result = dict(base or {})
    for key, value in (overlay or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result.get(key, {}), value)
        else:
            result[key] = value
    return result


def _load_default_config() -> dict[str, Any]:
    if not DEFAULTS_PATH.exists():
        return {}
    import json

    cfg = json.loads(DEFAULTS_PATH.read_text(encoding="utf-8"))
    if RECIPES_PATH.exists():
        recipes = json.loads(RECIPES_PATH.read_text(encoding="utf-8"))
        if isinstance(recipes, dict) and recipes:
            generation = cfg.setdefault("generation", {})
            existing = generation.get("recipes")
            if not isinstance(existing, dict):
                existing = {}
            generation["recipes"] = _deep_merge(existing, recipes)
    return cfg


def _require_fhir_gen():
    try:
        from fhir_gen.persistence import FHIRMongoStore
    except ImportError as exc:
        raise KehrnelError(
            code="FHIR_LIBS_NOT_INSTALLED",
            status=500,
            message="fhir-gen is not installed. Install kehrnel with the [fhir] extra.",
            details={"import_error": str(exc)},
        ) from exc
    return FHIRMongoStore


def _require_fhir_mql():
    try:
        from fhir_search_to_mql import ConfigLoader
        from fhir_search_to_mql.core.config_loader import _bundled_configs_dir
        from fhir_search_to_mql.denormalizer.mongodb_handler import MongoDBHandler
        from pymongo import MongoClient
    except ImportError as exc:
        raise KehrnelError(
            code="FHIR_LIBS_NOT_INSTALLED",
            status=500,
            message="fhir-search-to-mql is not installed. Install kehrnel with the [fhir] extra.",
            details={"import_error": str(exc)},
        ) from exc
    return ConfigLoader, _bundled_configs_dir, MongoDBHandler, MongoClient


def _bundled_compartment_definitions_dir() -> str:
    _require_fhir_mql()
    from fhir_search_to_mql.compartments.compartment_loader import CompartmentLoader

    return str(CompartmentLoader().definitions_dir)


def load_pack_defaults() -> dict[str, Any]:
    """Load defaults.json merged with specification/recipes.json."""
    return _load_default_config()


def resolve_strategy_config(ctx: StrategyContext) -> dict[str, Any]:
    """Merge manifest defaults with activation config and validate required fields."""
    merged: dict[str, Any] = load_pack_defaults()
    if ctx.manifest and ctx.manifest.default_config:
        merged = _deep_merge(merged, ctx.manifest.default_config)
    merged = _deep_merge(merged, ctx.config or {})

    database = merged.get("database")
    schema_version = merged.get("schema_version")
    collections = merged.get("collections") or {}
    mode = collections.get("mode") if isinstance(collections, dict) else None
    search = merged.get("search")

    errors: list[str] = []
    if not database:
        errors.append("database is required")
    if not schema_version:
        errors.append("schema_version is required")
    if mode != "per_resource_type":
        errors.append("collections.mode must be 'per_resource_type'")
    if not isinstance(search, dict):
        errors.append("search configuration must be an object")

    if errors:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Invalid FHIR strategy configuration",
            details={"errors": errors, "config_keys": sorted(merged.keys())},
        )
    # These are persistence invariants, not tenant tuning switches. Coercing old
    # stored activations keeps their read-only catalog usable while every runtime
    # write path still enforces the current contract. New activations reject false
    # values through the JSON Schema and validate_config().
    merged["search"] = {
        **(search if isinstance(search, dict) else {}),
        "enabled": True,
        "denormalize_on_generate": True,
        "auto_index": True,
    }
    return merged


def resolve_mongo(ctx: StrategyContext) -> tuple[str, str, str]:
    """
    Resolve MongoDB connection from bindings (preferred) or dev/test env vars.

    Returns (uri, database, collection_prefix).
    """
    cfg = resolve_strategy_config(ctx)
    prefix = str(cfg.get("collection_prefix") or "")

    bindings = ctx.bindings if isinstance(ctx.bindings, dict) else {}
    db_binding = bindings.get("db") if isinstance(bindings.get("db"), dict) else {}

    uri = db_binding.get("uri") or os.getenv("MONGODB_URI")
    database = (
        db_binding.get("database")
        or db_binding.get("name")
        or os.getenv("MONGODB_DB")
        or cfg.get("database")
    )

    if not uri or not database:
        raise KehrnelError(
            code="BINDINGS_NOT_RESOLVED",
            status=400,
            message="MongoDB bindings are not resolved (need bindings.db.uri and database, or MONGODB_URI / MONGODB_DB)",
            details={
                "has_uri": bool(uri),
                "has_database": bool(database),
                "bindings_keys": sorted(bindings.keys()) if bindings else [],
            },
        )
    return str(uri), str(database), prefix


def collection_name(prefix: str, resource_type: str) -> str:
    """MongoDB collection name for a FHIR resource type (matches FHIRMongoStore)."""
    return f"{prefix or ''}{resource_type}"


def build_fhir_gen_store(uri: str, database: str, prefix: str = ""):
    """Construct fhir_gen FHIRMongoStore for the given Mongo target."""
    FHIRMongoStore = _require_fhir_gen()
    return FHIRMongoStore(uri=uri, db_name=database, collection_prefix=prefix or None)


_mongo_client_cache: dict[str, Any] = {}
_config_loader_cache: dict[tuple[str, ...], Any] = {}


def _config_loader_cache_key(loader_dirs: list[str] | None) -> tuple[str, ...]:
    if not loader_dirs:
        return ("__bundled__",)
    return tuple(str(d) for d in loader_dirs)


def get_mongo_client(uri: str):
    """Return a pooled MongoClient for ``uri`` (reused across search/execute)."""
    _, _, _, MongoClient = _require_fhir_mql()
    client = _mongo_client_cache.get(uri)
    if client is None:
        client = MongoClient(uri)
        _mongo_client_cache[uri] = client
    return client


@dataclass
class MqlContext:
    """Shared fhir-mql runtime: config loader, Mongo client, and collection helpers."""

    uri: str
    database: str
    collection_prefix: str
    config_loader: Any
    client: Any
    mongo_handler: Any
    compartment_definitions_dir: str | None = None
    owns_client: bool = False

    @property
    def db(self):
        return self.client[self.database]

    def collection(self, resource_type: str):
        return self.db[collection_name(self.collection_prefix, resource_type)]


def build_mql_context(
    uri: str,
    database: str,
    prefix: str = "",
    config_dir: str | list[str] | None = None,
    compartment_dir: str | None = None,
    *,
    client: Any | None = None,
) -> MqlContext:
    """Build fhir-mql ConfigLoader + Mongo client (bundled YAML paths when dirs are null)."""
    ConfigLoader, bundled_configs_dir, MongoDBHandler, MongoClient = _require_fhir_mql()

    loader_dirs: list[str] | None = None
    if config_dir is None:
        loader_dirs = None
    elif isinstance(config_dir, str):
        loader_dirs = [config_dir]
    else:
        loader_dirs = list(config_dir)

    loader_key = _config_loader_cache_key(loader_dirs)
    config_loader = _config_loader_cache.get(loader_key)
    if config_loader is None:
        config_loader = ConfigLoader(config_dir=loader_dirs)
        _config_loader_cache[loader_key] = config_loader

    owns_client = False
    if client is not None:
        mongo_client = client
    else:
        mongo_client = get_mongo_client(uri)
    resolved_compartment = compartment_dir or _bundled_compartment_definitions_dir()

    return MqlContext(
        uri=uri,
        database=database,
        collection_prefix=prefix or "",
        config_loader=config_loader,
        client=mongo_client,
        mongo_handler=MongoDBHandler,
        compartment_definitions_dir=resolved_compartment,
        owns_client=owns_client,
    )


def close_mql_context(mql_ctx: MqlContext) -> None:
    """Close the Mongo client only when this context owns it (not the shared pool)."""
    if getattr(mql_ctx, "owns_client", False) and getattr(mql_ctx, "client", None) is not None:
        mql_ctx.client.close()


def supported_search_resource_types(loader: Any) -> list[str]:
    """Resource types with fhir-mql search YAML configs."""
    return list(loader.list_resources())


def known_generation_resource_types() -> set[str]:
    """Resource types defined in fhir-gen schema registry."""
    return _known_generation_resource_types()


def configured_cdr_resource_types(cfg: dict[str, Any]) -> set[str]:
    """Resource types explicitly present in this strategy's configured recipes.

    The vendored libraries contain broader reusable catalogs shared by multiple
    FHIR strategies. Recipe membership is the clinical CDR's explicit resource
    scope and therefore the safe boundary for its import/write APIs.
    """
    recipes = (cfg.get("generation") or {}).get("recipes") or {}
    configured: set[str] = set()
    if isinstance(recipes, dict):
        for recipe in recipes.values():
            resources = recipe.get("resources") if isinstance(recipe, dict) else None
            if isinstance(resources, dict):
                configured.update(str(name) for name in resources if str(name))
    return configured


def _known_generation_resource_types() -> set[str]:
    """Load fhir-gen schema resource names without reading kehrnel root ``.env``."""
    import importlib

    prev_cwd = os.getcwd()
    try:
        # fhir_gen Settings loads cwd `.env`; strategy pack dir has no kehrnel `.env`.
        os.chdir(Path(__file__).resolve().parent)
        registry_mod = importlib.import_module("fhir_gen.schema.registry")
        return set(registry_mod.registry.all_resources())
    finally:
        os.chdir(prev_cwd)


def resolve_generation_payload(cfg: dict[str, Any], payload: dict[str, Any] | None) -> dict[str, Any]:
    """
    Merge a named ``recipe`` from ``generation.recipes`` (defaults + recipes.json).

    Payload ``resources`` / ``resource_counts`` override recipe counts per type.
    """
    effective = dict(payload or {})
    recipe_name = effective.get("recipe") or effective.get("generation_recipe")
    if not recipe_name:
        return effective

    recipes = (cfg.get("generation") or {}).get("recipes") or {}
    if not isinstance(recipes, dict):
        recipes = {}
    recipe = recipes.get(str(recipe_name))
    if not isinstance(recipe, dict):
        known = sorted(recipes.keys())
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message=f"Unknown generation recipe: {recipe_name!r}",
            details={"recipe": recipe_name, "known_recipes": known},
        )

    recipe_resources = recipe.get("resources")
    if isinstance(recipe_resources, dict):
        merged_resources = {str(k): int(v) for k, v in recipe_resources.items()}
        payload_resources = effective.get("resources") or effective.get("resource_counts")
        if isinstance(payload_resources, dict):
            merged_resources.update({str(k): int(v) for k, v in payload_resources.items()})
        effective["resources"] = merged_resources
        effective.pop("resource_counts", None)

    if effective.get("scenarios") is None and recipe.get("scenarios"):
        effective["scenarios"] = list(recipe["scenarios"])

    if effective.get("seed") is None and recipe.get("seed") is not None:
        effective["seed"] = recipe["seed"]

    return effective


def parse_resources_payload(payload: dict[str, Any] | None) -> dict[str, int]:
    """
    Normalize synthetic generation resource counts.

    Accepts:
    - ``resources``: {ResourceType: count}
    - ``resource_counts``: alias for ``resources``
    - ``resource_list`` / legacy list: ``["Patient=10", ...]``
    """
    payload = payload or {}
    if not isinstance(payload, dict):
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Generation payload must be an object",
        )

    counts: dict[str, int] = {}

    raw = payload.get("resources")
    if raw is None:
        raw = payload.get("resource_counts")
    if isinstance(raw, dict):
        for resource_type, count in raw.items():
            counts[str(resource_type)] = int(count)

    legacy = payload.get("resource_list")
    if legacy is None:
        legacy = payload.get("resources_list")
    if isinstance(legacy, list):
        for item in legacy:
            if isinstance(item, str):
                match = _LEGACY_RESOURCE_PAIR.match(item.strip())
                if not match:
                    raise KehrnelError(
                        code="INVALID_INPUT",
                        status=400,
                        message=f"Invalid legacy resource entry: {item!r} (expected ResourceType=count)",
                    )
                counts[match.group("resource")] = int(match.group("count"))
            elif isinstance(item, dict):
                resource_type = item.get("resource_type") or item.get("resourceType") or item.get("type")
                count = item.get("count")
                if not resource_type or count is None:
                    raise KehrnelError(
                        code="INVALID_INPUT",
                        status=400,
                        message=f"Invalid legacy resource object: {item!r}",
                    )
                counts[str(resource_type)] = int(count)

    if not counts:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Generation payload must include resources, resource_counts, or recipe",
            details={
                "hint": "Use resources: {Patient: 10}, recipe: 'clinical_dev', or resource_list: ['Patient=10']",
            },
        )

    for resource_type, count in counts.items():
        if count < 0:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message=f"Resource count must be non-negative: {resource_type}={count}",
            )

    known_gen = _known_generation_resource_types()
    unknown = sorted(rt for rt in counts if rt not in known_gen)
    if unknown:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Unknown FHIR resource type(s) for generation",
            details={"unknown": unknown, "examples": sorted(list(known_gen))[:12]},
        )

    return counts

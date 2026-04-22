import logging
import os
import json
import secrets
import time
from typing import Optional
from pathlib import Path

import certifi
from fastapi import HTTPException, Request
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import ConnectionFailure

from kehrnel.api.bridge.app.core.config import settings
from kehrnel.api.bridge.app.core.config_manager import initialize_config_manager, close_config_manager
from kehrnel.engine.core.bindings_resolver import resolve_bindings as resolve_bindings_ref
from kehrnel.engine.strategies.openehr.rps_dual.config import (
    build_coding_opts,
    build_flattener_config,
    normalize_config,
)
from kehrnel.engine.strategies.openehr.rps_dual.config_resolver import resolve_uri_async
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener
from kehrnel.engine.strategies.openehr.rps_dual.ingest.unflattener import CompositionUnflattener

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Database:
    client: AsyncIOMotorClient = None
    clients_by_uri: dict[str, AsyncIOMotorClient] = {}

db = Database()

_DEFAULT_COMPOSITIONS_COLL_NAME = settings.COMPOSITIONS_COLL_NAME
_DEFAULT_FLAT_COMPOSITIONS_COLL_NAME = settings.FLAT_COMPOSITIONS_COLL_NAME
_DEFAULT_SEARCH_COMPOSITIONS_COLL_NAME = settings.SEARCH_COMPOSITIONS_COLL_NAME
_DEFAULT_EHR_COLL_NAME = settings.EHR_COLL_NAME
_DEFAULT_EHR_CONTRIBUTIONS_COLL = settings.EHR_CONTRIBUTIONS_COLL
_DEFAULT_CODES_COLLECTION = settings.search_config.codes_collection
_DEFAULT_SHORTCUTS_COLLECTION = settings.search_config.shortcuts_collection
_DEFAULT_SEARCH_INDEX_NAME = settings.search_config.search_index_name
_DEFAULT_SEARCH_COLLECTION = settings.search_config.search_collection
_DEFAULT_FLATTEN_COLLECTION = settings.search_config.flatten_collection
_DEFAULT_SEARCH_COMPOSITIONS_MERGE = settings.search_config.search_compositions_merge
_DEFAULT_MAPPINGS_PATH = (
    Path(__file__).resolve().parents[4]
    / "engine"
    / "strategies"
    / "openehr"
    / "rps_dual"
    / "ingest"
    / "config"
    / "flattener_mappings_f.jsonc"
)
_RPS_DUAL_BASE_DIR = _DEFAULT_MAPPINGS_PATH.resolve().parents[2]


def _dictionary_ready_cache_ttl_seconds() -> float:
    raw = (os.getenv("KEHRNEL_DICTIONARY_READY_CACHE_TTL_SECONDS") or "30").strip()
    try:
        return max(float(raw), 0.0)
    except ValueError:
        logger.warning(
            "Invalid KEHRNEL_DICTIONARY_READY_CACHE_TTL_SECONDS=%r; using 30 seconds.",
            raw,
        )
        return 30.0


def _dictionary_ready_cache(request: Request) -> dict[tuple, float]:
    cache = getattr(request.app.state, "_openehr_dictionary_ready_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        request.app.state._openehr_dictionary_ready_cache = cache
    return cache


def _prune_dictionary_ready_cache(cache: dict[tuple, float], *, now: float) -> None:
    expired = [signature for signature, expires_at in cache.items() if expires_at <= now]
    for signature in expired:
        cache.pop(signature, None)


def _dictionary_readiness_signature(context: dict, strategy_cfg, modes: dict[str, str]) -> tuple:
    activation = context.get("activation")
    return (
        context.get("env_id"),
        context.get("domain") or "openehr",
        getattr(activation, "activation_id", None),
        getattr(activation, "config_hash", None),
        context.get("database_name"),
        strategy_cfg.collections.codes.name,
        strategy_cfg.collections.shortcuts.name,
        modes.get("codes") or "none",
        modes.get("shortcuts") or "none",
    )


def _is_dictionary_ready_cached(request: Request, signature: tuple) -> bool:
    ttl_seconds = _dictionary_ready_cache_ttl_seconds()
    if ttl_seconds <= 0:
        return False

    now = time.monotonic()
    cache = _dictionary_ready_cache(request)
    _prune_dictionary_ready_cache(cache, now=now)
    expires_at = cache.get(signature)
    return expires_at is not None and expires_at > now


def _mark_dictionary_ready_cached(request: Request, signature: tuple) -> None:
    ttl_seconds = _dictionary_ready_cache_ttl_seconds()
    if ttl_seconds <= 0:
        return

    now = time.monotonic()
    cache = _dictionary_ready_cache(request)
    _prune_dictionary_ready_cache(cache, now=now)
    cache[signature] = now + ttl_seconds


def _invalidate_dictionary_ready_cache(request: Request, signature: tuple | None = None) -> None:
    cache = _dictionary_ready_cache(request)
    if signature is None:
        cache.clear()
        return
    cache.pop(signature, None)

def _extract_env_id(request: Request) -> Optional[str]:
    env_id = (
        request.headers.get("x-active-env")
        or request.headers.get("x-env-id")
        or request.headers.get("x-environment-id")
        or request.query_params.get("env_id")
        or request.query_params.get("environment")
        or request.path_params.get("env_id")
    )
    return (env_id or "").strip() or None


def _default_env_id() -> Optional[str]:
    return (
        os.getenv("KEHRNEL_DEFAULT_ENV_ID")
        or os.getenv("DEFAULT_ENV_ID")
        or os.getenv("ENV_ID")
        or None
    )


def _parse_api_key_env_scopes() -> dict[str, object]:
    """
    Parse KEHRNEL_API_KEY_ENV_SCOPES JSON mapping:
      {"api-key-1": ["env-a","env-b"], "api-key-2": "*"}
    """
    raw = (os.getenv("KEHRNEL_API_KEY_ENV_SCOPES") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        logger.warning("Invalid KEHRNEL_API_KEY_ENV_SCOPES JSON; ignoring")
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _is_env_access_allowed(request: Request, env_id: str) -> bool:
    scopes = _parse_api_key_env_scopes()
    api_key = (request.headers.get("x-api-key") or "").strip()
    if scopes:
        if not api_key:
            return False
        matched_scope = None
        for key, scope in scopes.items():
            if secrets.compare_digest(api_key, str(key)):
                matched_scope = scope
                break
        if matched_scope is None:
            return False
        if matched_scope == "*":
            return True
        if isinstance(matched_scope, list):
            return env_id in {str(v).strip() for v in matched_scope if str(v).strip()}
        return False

    # Fallback safety: if no per-key scope configured, constrain to default env when set.
    default_env = _default_env_id()
    if default_env:
        return env_id == default_env

    # Fail closed when no explicit scope policy is configured.
    logger.warning(
        "Environment authz scope is not configured (KEHRNEL_API_KEY_ENV_SCOPES/KEHRNEL_DEFAULT_ENV_ID). "
        "Denying env_id=%s by default.",
        env_id,
    )
    return False


def _get_client_for_uri(uri: str) -> AsyncIOMotorClient:
    client = db.clients_by_uri.get(uri)
    if client is None:
        kwargs = {}
        if uri.startswith("mongodb+srv://") or "tls=true" in uri or "ssl=true" in uri:
            kwargs["tlsCAFile"] = certifi.where()
        client = AsyncIOMotorClient(uri, **kwargs)
        db.clients_by_uri[uri] = client
    return client


def _get_activation(request: Request, env_id: str, domain: str):
    runtime = getattr(request.app.state, "strategy_runtime", None)
    if runtime is None:
        raise HTTPException(
            status_code=503,
            detail="Strategy runtime is not available.",
        )

    activation = runtime.registry.get_activation(env_id, domain) or runtime.registry.get_activation(env_id)
    if activation is None:
        raise HTTPException(
            status_code=404,
            detail=f"No activation found for env_id={env_id} and domain={domain}.",
        )
    return runtime, activation


def _is_rps_dual_activation(activation) -> bool:
    strategy_id = (getattr(activation, "strategy_id", None) or "").strip().lower()
    domain = (getattr(activation, "domain", None) or "").strip().lower()
    return domain == "openehr" and strategy_id == "openehr.rps_dual"


def _dictionary_bootstrap_payload(activation) -> dict[str, str]:
    cfg = activation.config if isinstance(getattr(activation, "config", None), dict) else {}
    transform_cfg = cfg.get("transform") if isinstance(cfg, dict) else {}
    transform_cfg = transform_cfg if isinstance(transform_cfg, dict) else {}
    bootstrap_cfg = ((cfg.get("bootstrap") or {}).get("dictionariesOnActivate") if isinstance(cfg, dict) else {}) or {}
    bootstrap_cfg = bootstrap_cfg if isinstance(bootstrap_cfg, dict) else {}
    apply_shortcuts = bool(transform_cfg.get("apply_shortcuts", True))
    return {
        "codes": str(bootstrap_cfg.get("codes") or "ensure").strip().lower() or "ensure",
        "shortcuts": str(
            bootstrap_cfg.get("shortcuts") or ("seed" if apply_shortcuts else "none")
        ).strip().lower() or ("seed" if apply_shortcuts else "none"),
    }


async def _needs_dictionary_bootstrap(target_db: AsyncIOMotorDatabase, strategy_cfg, modes: dict[str, str]) -> bool:
    if target_db is None:
        return False

    collection_names = set(await target_db.list_collection_names())
    codes_name = strategy_cfg.collections.codes.name
    shortcuts_name = strategy_cfg.collections.shortcuts.name

    if modes.get("codes") == "ensure" and codes_name and codes_name not in collection_names:
        return True
    if modes.get("codes") == "seed" and codes_name:
        if codes_name not in collection_names:
            return True
        codes_doc = await target_db[codes_name].find_one({"_id": "ar_code"}, {"_id": 1})
        if not codes_doc:
            return True

    if modes.get("shortcuts") == "ensure" and shortcuts_name and shortcuts_name not in collection_names:
        return True
    if modes.get("shortcuts") == "seed" and shortcuts_name:
        if shortcuts_name not in collection_names:
            return True
        shortcuts_doc = await target_db[shortcuts_name].find_one({"_id": "shortcuts"}, {"_id": 1})
        if not shortcuts_doc:
            return True

    return False


def _sync_legacy_openehr_settings(activation, database_name: str | None = None) -> dict[str, str | bool | None]:
    cfg = activation.config if isinstance(getattr(activation, "config", None), dict) else {}
    collections = cfg.get("collections") if isinstance(cfg, dict) else {}
    collections = collections if isinstance(collections, dict) else {}

    compositions_cfg = collections.get("compositions") if isinstance(collections.get("compositions"), dict) else {}
    search_cfg = collections.get("search") if isinstance(collections.get("search"), dict) else {}
    codes_cfg = collections.get("codes") if isinstance(collections.get("codes"), dict) else {}
    shortcuts_cfg = collections.get("shortcuts") if isinstance(collections.get("shortcuts"), dict) else {}
    ehr_cfg = collections.get("ehr") if isinstance(collections.get("ehr"), dict) else {}
    contrib_cfg = collections.get("contributions") if isinstance(collections.get("contributions"), dict) else {}

    canonical_collection = (
        cfg.get("canonical_collection")
        or ((collections.get("canonical") or {}).get("name") if isinstance(collections.get("canonical"), dict) else None)
        or _DEFAULT_COMPOSITIONS_COLL_NAME
    )
    flat_collection = compositions_cfg.get("name") or _DEFAULT_FLAT_COMPOSITIONS_COLL_NAME
    search_collection = search_cfg.get("name") or _DEFAULT_SEARCH_COMPOSITIONS_COLL_NAME
    search_enabled = bool(search_cfg.get("enabled", True))
    codes_collection = codes_cfg.get("name") or _DEFAULT_CODES_COLLECTION
    shortcuts_collection = shortcuts_cfg.get("name") or _DEFAULT_SHORTCUTS_COLLECTION
    search_index_name = (
        ((search_cfg.get("atlasIndex") or {}).get("name") if isinstance(search_cfg.get("atlasIndex"), dict) else None)
        or _DEFAULT_SEARCH_INDEX_NAME
    )
    ehr_collection = ehr_cfg.get("name") or _DEFAULT_EHR_COLL_NAME
    contributions_collection = contrib_cfg.get("name") or _DEFAULT_EHR_CONTRIBUTIONS_COLL

    settings.COMPOSITIONS_COLL_NAME = canonical_collection
    settings.FLAT_COMPOSITIONS_COLL_NAME = flat_collection
    settings.SEARCH_COMPOSITIONS_COLL_NAME = search_collection
    settings.EHR_COLL_NAME = ehr_collection
    settings.EHR_CONTRIBUTIONS_COLL = contributions_collection
    if database_name:
        settings.MONGODB_DB = database_name

    settings.search_config.flatten_collection = flat_collection or _DEFAULT_FLATTEN_COLLECTION
    settings.search_config.search_collection = search_collection or _DEFAULT_SEARCH_COLLECTION
    settings.search_config.codes_collection = codes_collection or _DEFAULT_CODES_COLLECTION
    settings.search_config.shortcuts_collection = shortcuts_collection or _DEFAULT_SHORTCUTS_COLLECTION
    settings.search_config.search_index_name = search_index_name or _DEFAULT_SEARCH_INDEX_NAME
    settings.search_config.search_compositions_merge = _DEFAULT_SEARCH_COMPOSITIONS_MERGE

    return {
        "canonical_collection": canonical_collection,
        "flat_collection": flat_collection,
        "search_collection": search_collection,
        "codes_collection": codes_collection,
        "shortcuts_collection": shortcuts_collection,
        "search_index_name": search_index_name,
        "search_enabled": search_enabled,
        "ehr_collection": ehr_collection,
        "contributions_collection": contributions_collection,
        "database_name": database_name or settings.MONGODB_DB,
    }


async def _ensure_legacy_openehr_ingestion_runtime(
    request: Request,
    *,
    activation,
    target_db: AsyncIOMotorDatabase,
    database_name: str,
    synced: dict[str, str | bool | None],
) -> None:
    if not _is_rps_dual_activation(activation):
        return

    runtime_state = getattr(request.app.state, "legacy_openehr_runtime", {}) or {}
    signature = {
        "env_id": getattr(activation, "env_id", None),
        "activation_id": getattr(activation, "activation_id", None),
        "config_hash": getattr(activation, "config_hash", None),
        "database_name": database_name,
        "canonical_collection": synced.get("canonical_collection"),
        "flat_collection": synced.get("flat_collection"),
        "search_collection": synced.get("search_collection"),
    }

    if (
        getattr(request.app.state, "flattener", None) is not None
        and getattr(request.app.state, "unflattener", None) is not None
        and runtime_state == signature
    ):
        return

    strategy_cfg = normalize_config(activation.config or {})
    mappings_content = None
    mappings_ref = strategy_cfg.transform.mappings
    if mappings_ref:
        mappings_content = await resolve_uri_async(mappings_ref, target_db, _RPS_DUAL_BASE_DIR)

    flattener_config = build_flattener_config(strategy_cfg)
    coding_opts = build_coding_opts(strategy_cfg)
    flattener = await CompositionFlattener.create(
        db=target_db,
        config=flattener_config,
        mappings_path=str(_DEFAULT_MAPPINGS_PATH),
        mappings_content=mappings_content,
        field_map=None,
        coding_opts=coding_opts,
    )

    request.app.state.flattener = flattener
    request.app.state.unflattener = CompositionUnflattener.from_flattener(flattener)
    request.app.state.db = target_db
    request.app.state.target_db = target_db
    request.app.state.source_db = target_db
    request.app.state.config = {
        "apply_shortcuts": strategy_cfg.transform.apply_shortcuts,
        "source": {
            "canonical_compositions_collection": synced.get("canonical_collection"),
            "database_name": database_name,
        },
        "target": {
            "compositions_collection": synced.get("flat_collection"),
            "search_collection": synced.get("search_collection"),
            "codes_collection": synced.get("codes_collection"),
            "shortcuts_collection": synced.get("shortcuts_collection") or strategy_cfg.collections.shortcuts.name,
            "rebuilt_collection": synced.get("flat_collection"),
            "database_name": database_name,
            "search_compositions_merge": settings.search_config.search_compositions_merge,
        },
    }
    request.app.state.mappings_path = str(_DEFAULT_MAPPINGS_PATH)
    request.app.state.ingest_options = {
        "store_canonical": bool((activation.config or {}).get("store_canonical", False)),
        "canonical_collection": synced.get("canonical_collection"),
        "search_enabled": bool(synced.get("search_enabled", True)),
        "atlas_index_name": synced.get("search_index_name"),
    }
    request.app.state.strategy_raw = activation.config or {}
    request.app.state.legacy_openehr_runtime = signature

    logger.info(
        "Initialized legacy openEHR ingestion runtime for env_id=%s activation_id=%s",
        signature.get("env_id"),
        signature.get("activation_id"),
    )


async def ensure_active_openehr_dictionaries(
    request: Request,
    *,
    context: dict | None = None,
) -> bool:
    context = context or await resolve_active_openehr_context(
        request,
        ensure_ingestion=False,
    )
    activation = context.get("activation")
    if activation is None or not _is_rps_dual_activation(activation):
        return False

    strategy_cfg = normalize_config(activation.config or {})
    bootstrap_payload = _dictionary_bootstrap_payload(activation)
    if all(mode == "none" for mode in bootstrap_payload.values()):
        return False
    readiness_signature = _dictionary_readiness_signature(context, strategy_cfg, bootstrap_payload)
    if _is_dictionary_ready_cached(request, readiness_signature):
        return False

    target_db = context.get("db")
    if not await _needs_dictionary_bootstrap(target_db, strategy_cfg, bootstrap_payload):
        _mark_dictionary_ready_cached(request, readiness_signature)
        return False

    runtime = context.get("runtime")
    env_id = context.get("env_id")
    domain = context.get("domain") or "openehr"
    if runtime is None or not env_id:
        raise HTTPException(
            status_code=503,
            detail="Strategy runtime is unavailable. Cannot bootstrap configured dictionaries before ingest.",
        )

    try:
        result = await runtime.dispatch(
            env_id,
            "op",
            {"domain": domain, "op": "ensure_dictionaries", "payload": bootstrap_payload},
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Configured dictionaries are missing and automatic bootstrap failed: {exc}",
        ) from exc

    if isinstance(result, dict) and result.get("ok") is False:
        _invalidate_dictionary_ready_cache(request, readiness_signature)
        warnings = result.get("warnings") or []
        warning_text = f" Warnings: {'; '.join(str(w) for w in warnings)}" if warnings else ""
        raise HTTPException(
            status_code=503,
            detail=f"Configured dictionaries are missing and automatic bootstrap failed.{warning_text}",
        )

    if await _needs_dictionary_bootstrap(target_db, strategy_cfg, bootstrap_payload):
        _invalidate_dictionary_ready_cache(request, readiness_signature)
        raise HTTPException(
            status_code=409,
            detail=(
                "The strategy requires dictionary collections that are still missing in the tenant database. "
                "Re-activate the strategy or run ensure_dictionaries before ingesting."
            ),
        )
    _mark_dictionary_ready_cached(request, readiness_signature)

    # Force the request-scoped flattener/unflattener to reload freshly bootstrapped dictionaries.
    request.app.state.flattener = None
    request.app.state.unflattener = None
    request.app.state.legacy_openehr_runtime = {}

    if context.get("ingestion_ready"):
        await _ensure_legacy_openehr_ingestion_runtime(
            request,
            activation=activation,
            target_db=target_db,
            database_name=context["database_name"],
            synced=context["synced"],
        )
        context["ingestion_ready"] = True

    return True


async def resolve_active_openehr_context(
    request: Request,
    *,
    domain: str = "openehr",
    ensure_ingestion: bool = False,
) -> dict:
    cached = getattr(request.state, "_openehr_context", None)
    if cached and cached.get("domain") == domain:
        if ensure_ingestion and not cached.get("ingestion_ready"):
            await _ensure_legacy_openehr_ingestion_runtime(
                request,
                activation=cached["activation"],
                target_db=cached["db"],
                database_name=cached["database_name"],
                synced=cached["synced"],
            )
            cached["ingestion_ready"] = True
        return cached

    env_id = _extract_env_id(request)
    if not env_id:
        raise HTTPException(
            status_code=400,
            detail="Missing active environment. Provide x-active-env (or env_id query param).",
        )
    if not _is_env_access_allowed(request, env_id):
        raise HTTPException(
            status_code=403,
            detail=f"Access to env_id={env_id} is not permitted for this API key.",
        )

    runtime, activation = _get_activation(request, env_id, domain)
    resolved = None
    if activation.bindings_ref:
        if runtime.bindings_resolver is None:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Bindings resolver is not configured. Set KEHRNEL_BINDINGS_RESOLVER "
                    "or HDL resolver environment variables."
                ),
            )

        resolved = await resolve_bindings_ref(
            runtime.bindings_resolver,
            bindings_ref=activation.bindings_ref,
            env_id=env_id,
            domain=domain,
            strategy_id=activation.strategy_id,
            op=f"http:{request.method.lower()}:{request.url.path}",
            context={"activation_config": activation.config or {}},
        )
        db_cfg = (resolved or {}).get("db") if isinstance(resolved, dict) else None
    elif getattr(activation, "bindings", None):
        logger.debug("Using plaintext bindings for env_id=%s (no bindings_ref).", env_id)
        db_cfg = activation.bindings.get("db") if isinstance(activation.bindings, dict) else None
        resolved = {"db": db_cfg} if isinstance(db_cfg, dict) else None
    elif getattr(activation, "bindings_meta", None):
        logger.debug("Using bindings_meta for env_id=%s (no bindings_ref).", env_id)
        db_cfg = activation.bindings_meta.get("db") if isinstance(activation.bindings_meta, dict) else None
        resolved = {"db": db_cfg} if isinstance(db_cfg, dict) else None
    else:
        raise HTTPException(
            status_code=409,
            detail=(
                "Activation has no bindings_ref or plaintext bindings. "
                "Re-activate environment with bindings_ref or --allow-plaintext-bindings."
            ),
        )
    provider = (db_cfg or {}).get("provider")
    uri = (db_cfg or {}).get("uri")
    database_name = (db_cfg or {}).get("database")
    if provider != "mongodb" or not uri or not database_name:
        raise HTTPException(
            status_code=502,
            detail="Bindings resolver returned invalid MongoDB binding (provider/uri/database required).",
        )

    client = _get_client_for_uri(str(uri))
    target_db = client[str(database_name)]
    synced = _sync_legacy_openehr_settings(activation, str(database_name))

    context = {
        "domain": domain,
        "env_id": env_id,
        "runtime": runtime,
        "activation": activation,
        "bindings": resolved,
        "database_name": str(database_name),
        "db": target_db,
        "synced": synced,
        "ingestion_ready": False,
    }

    if ensure_ingestion:
        await _ensure_legacy_openehr_ingestion_runtime(
            request,
            activation=activation,
            target_db=target_db,
            database_name=str(database_name),
            synced=synced,
        )
        context["ingestion_ready"] = True

    request.state._openehr_context = context
    return context


async def _resolve_customer_db_from_activation(
    request: Request,
    domain: str = "openehr",
) -> AsyncIOMotorDatabase:
    context = await resolve_active_openehr_context(request, domain=domain, ensure_ingestion=False)
    return context["db"]


async def get_mongodb_ehr_db(request: Request = None) -> AsyncIOMotorDatabase:
    # Request-aware path: resolve customer DB from active env binding contract.
    if request is not None:
        return await _resolve_customer_db_from_activation(request, domain="openehr")

    # Fallback for non-request internal utility calls.
    if db.client is None:
        db.client = AsyncIOMotorClient(settings.MONGODB_URI, tlsCAFile=certifi.where())
    return db.client[settings.MONGODB_DB]


async def get_mongodb_database(database_name: str, request: Request = None) -> AsyncIOMotorDatabase:
    """Get a specific database by name."""
    if request is not None:
        base_db = await _resolve_customer_db_from_activation(request, domain="openehr")
        return base_db.client[database_name]
    if db.client is None:
        db.client = AsyncIOMotorClient(settings.MONGODB_URI, tlsCAFile=certifi.where())
    return db.client[database_name]

async def connect_to_mongo(tls_allow_invalid_certificates: bool = False):
    logger.info("Connecting to MongoDB")
    try:
        db.client = AsyncIOMotorClient(
            settings.MONGODB_URI,
            serverSelectionTimeoutMS = 5000,
            tlsAllowInvalidCertificates=tls_allow_invalid_certificates
        )

        # It sends an ismaster command which let us know if the connections was established correctly
        await db.client.admin.command('ismaster')
        logger.info("Successfully connected to MongoDB!")
        
        # Initialize configuration manager if dynamic config is enabled
        if settings.USE_DYNAMIC_CONFIG:
            logger.info("Initializing configuration manager...")
            await initialize_config_manager(
                config_db_uri=settings.config_db_uri,
                config_db_name=settings.CONFIG_DB_NAME,
                config_collection_name=settings.CONFIG_COLLECTION_NAME,
                cache_ttl_minutes=settings.CONFIG_CACHE_TTL_MINUTES
            )
            logger.info("Configuration manager initialized successfully")
        else:
            logger.info("Dynamic configuration disabled, using static configuration")
            
    except ConnectionFailure as e:
        logger.critical(f"Fatal error: Could not connect to MongoDB. Application will shut down. Error: {e}")

async def close_mongo_connection():
    if db.client:
        logger.info("Closing MongoDB connection")
        db.client.close()
        logger.info("MongoDB connection closed")
    for uri, client in list(db.clients_by_uri.items()):
        try:
            client.close()
        except Exception:
            pass
        finally:
            db.clients_by_uri.pop(uri, None)
    
    # Close configuration manager
    if settings.USE_DYNAMIC_CONFIG:
        await close_config_manager()
        logger.info("Configuration manager connection closed")

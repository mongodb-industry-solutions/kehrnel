"""FastAPI assembly for the new kehrnel runtime."""
from __future__ import annotations

import json
import os
import logging
import secrets
import hashlib
import time
import re
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path
from typing import Optional
from dotenv import find_dotenv, load_dotenv
from fastapi import FastAPI, Request, HTTPException, Security
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

# Load environment before importing modules that instantiate settings at import time.
load_dotenv(find_dotenv(".env.local", usecwd=True), override=False)

from kehrnel.api.core.admin.routes import router as admin_router
from kehrnel.api.core.admin.ops_by_domain import router as ops_domain_router
from kehrnel.api.core.admin.activation_routes import router as activation_router
from kehrnel.api.domains.fhir.routes import router as fhir_domain_router
from kehrnel.api.domains.openehr.routes import router as openehr_domain_router
from kehrnel.api.strategies.openehr.rps_dual.routes import router as openehr_rps_dual_router
from kehrnel.engine.core.runtime import StrategyRuntime
from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.registry import FileActivationRegistry
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.bundle_store import BundleStore
from kehrnel.engine.core.pack_validator import StrategyPackValidator
from kehrnel.engine.core.bindings_resolver import load_bindings_resolver_from_env
from kehrnel.engine.core.synthetic_jobs import SyntheticJobManager
from kehrnel.engine.core.synthetic_jobs_store import MongoSyntheticJobStore
from kehrnel.engine.core.redaction import redact_sensitive


# ============================================================================
# Security Configuration
# ============================================================================

# API Key Authentication
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

# Paths that don't require authentication
PUBLIC_PATHS = {"/health", "/openapi.json", "/favicon.ico"}
PUBLIC_PATH_PREFIXES = ("/docs", "/redoc", "/openapi/", "/guide")
PUBLIC_PATH_PATTERNS = (
    # Strategy static assets referenced from docs/specs.
    re.compile(r"^/strategies/[^/]+/assets/.+"),
)


def _is_public_path(path: str) -> bool:
    return (
        path in PUBLIC_PATHS
        or any(path.startswith(prefix) for prefix in PUBLIC_PATH_PREFIXES)
        or any(pattern.match(path) for pattern in PUBLIC_PATH_PATTERNS)
    )


def _get_api_keys() -> set[str]:
    """Get valid API keys from environment."""
    keys_str = os.getenv("KEHRNEL_API_KEYS", "")
    if not keys_str:
        return set()
    return {k.strip() for k in keys_str.split(",") if k.strip()}


def _is_auth_enabled() -> bool:
    """Check if API key authentication is enabled."""
    return os.getenv("KEHRNEL_AUTH_ENABLED", "true").lower() in ("true", "1", "yes")


class APIKeyAuthMiddleware(BaseHTTPMiddleware):
    """Middleware to validate API keys on requests."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path or ""
        # Skip auth for public paths
        if _is_public_path(path):
            return await call_next(request)

        # Skip if auth is disabled
        if not _is_auth_enabled():
            return await call_next(request)

        # Get API key from header
        api_key = request.headers.get("X-API-Key")
        valid_keys = _get_api_keys()

        if not valid_keys:
            # Fail closed: auth is enabled but no keys are configured.
            logging.getLogger("kehrnel.security").warning(
                "API auth enabled but no keys configured (KEHRNEL_API_KEYS)"
            )
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "AUTH_MISCONFIGURED", "message": "Authentication is enabled but not configured"}},
            )

        if not api_key:
            return JSONResponse(
                status_code=401,
                content={"error": {"code": "UNAUTHORIZED", "message": "Invalid or missing API key"}},
            )
        # Check if key is valid using constant-time comparison
        key_valid = any(secrets.compare_digest(api_key, k) for k in valid_keys)
        if not key_valid:
            return JSONResponse(
                status_code=401,
                content={"error": {"code": "UNAUTHORIZED", "message": "Invalid or missing API key"}},
            )
        # Avoid using attacker-controlled headers directly for other controls.
        key_fingerprint = hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:20]
        request.state.authenticated_api_key = key_fingerprint
        request.state.rate_limit_key = f"key:{key_fingerprint}"

        return await call_next(request)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Simple in-memory rate limiting middleware."""

    def __init__(self, app, requests_per_minute: int = 60, max_clients: int = 5000):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.max_clients = max_clients
        self._request_counts: dict[str, list[float]] = {}

    async def dispatch(self, request: Request, call_next):
        # Skip rate limiting for public/doc/static paths.
        if _is_public_path(request.url.path or ""):
            return await call_next(request)

        # Prefer authenticated identity from auth middleware; fallback to IP.
        client_id = getattr(request.state, "rate_limit_key", None)
        if not client_id:
            ip = request.client.host if request.client else "unknown"
            client_id = f"ip:{ip}"

        current_time = time.time()
        window_start = current_time - 60  # 1 minute window

        # Clean old requests and get current count
        if client_id not in self._request_counts:
            self._request_counts[client_id] = []

        self._request_counts[client_id] = [
            t for t in self._request_counts[client_id] if t > window_start
        ]
        # Bound memory growth from spoofed identities / high cardinality traffic.
        if len(self._request_counts) > self.max_clients:
            oldest_key = None
            oldest_ts = float("inf")
            for key, ts_list in self._request_counts.items():
                ts = ts_list[-1] if ts_list else 0.0
                if ts < oldest_ts:
                    oldest_ts = ts
                    oldest_key = key
            if oldest_key:
                self._request_counts.pop(oldest_key, None)

        if len(self._request_counts[client_id]) >= self.requests_per_minute:
            return JSONResponse(
                status_code=429,
                content={"error": {"code": "RATE_LIMITED", "message": "Too many requests. Please try again later."}},
                headers={"Retry-After": "60"}
            )

        self._request_counts[client_id].append(current_time)
        return await call_next(request)


def _get_cors_origins() -> list[str]:
    """Get allowed CORS origins from environment."""
    origins_str = os.getenv("KEHRNEL_CORS_ORIGINS", "")
    if not origins_str:
        # Default: no CORS (same-origin only) in production
        # Set KEHRNEL_CORS_ORIGINS=* for development
        return []
    if origins_str == "*":
        return ["*"]
    return [o.strip() for o in origins_str.split(",") if o.strip()]


def _get_rate_limit() -> int:
    """Get rate limit from environment (requests per minute)."""
    try:
        return int(os.getenv("KEHRNEL_RATE_LIMIT", "60"))
    except ValueError:
        return 60


def _get_rate_limit_clients() -> int:
    """Max identities tracked by in-memory rate limiter."""
    try:
        return max(1000, int(os.getenv("KEHRNEL_RATE_LIMIT_MAX_CLIENTS", "5000")))
    except ValueError:
        return 5000


# ============================================================================
# Strategy Discovery
# ============================================================================

def _strategy_paths() -> list[Path]:
    paths = [Path(__file__).resolve().parents[1] / "engine" / "strategies"]
    extra = os.getenv("KEHRNEL_STRATEGY_PATHS")
    if extra:
        for sep in (":", ","):
            if sep in extra:
                extra_paths = [p for p in extra.split(sep) if p]
                break
        else:
            extra_paths = [extra] if extra else []
        for part in extra_paths:
            if part:
                paths.append(Path(part))
    return paths


def validate_strategy_pack(manifest_data: dict, base_path: Path) -> list[str]:
    """Validate that a strategy pack has required assets and fields."""
    return StrategyPackValidator(manifest_data, base_path).validate()


def _seed_default_bundles(store: BundleStore):
    log = logging.getLogger("kehrnel.bundle.seed")
    try:
        bundles_dir = Path(__file__).resolve().parents[1] / "engine" / "strategies" / "openehr" / "rps_dual" / "bundles"
        if bundles_dir.exists():
            for path in bundles_dir.glob("*.json"):
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                    store.save_bundle(data, mode="upsert")
                except Exception as exc:
                    log.warning("Skipping invalid default bundle %s: %s", path, exc)
                    continue
    except Exception as exc:
        log.warning("Default bundle seeding failed: %s", exc)
        return


def _load_manifests() -> tuple[list[StrategyManifest], list[dict[str, object]], dict[str, str]]:
    manifests: list[StrategyManifest] = []
    diagnostics: list[dict[str, object]] = []
    log = logging.getLogger("kehrnel.strategy.discovery")
    manifest_records: list[dict[str, object]] = []
    for base in _strategy_paths():
        if not base.exists():
            continue
        for manifest_path in base.glob("**/manifest.json"):
            # Skip disabled strategy folders staged outside active discovery.
            if any(part.startswith("_disabled") for part in manifest_path.parts):
                continue
            try:
                data = json.loads(manifest_path.read_text(encoding="utf-8"))
                base_dir = manifest_path.parent
                defaults_path = base_dir / "defaults.json"
                schema_path = base_dir / "schema.json"
                spec_field = data.get("spec")
                if isinstance(spec_field, dict):
                    spec_path = base_dir / (spec_field.get("path") or "spec.json")
                elif isinstance(spec_field, str):
                    spec_path = base_dir / spec_field
                else:
                    spec_path = base_dir / "spec.json"
                if defaults_path.exists():
                    data["default_config"] = json.loads(defaults_path.read_text(encoding="utf-8"))
                if schema_path.exists():
                    data["config_schema"] = json.loads(schema_path.read_text(encoding="utf-8"))
                if data.get("pack_format") == "strategy-pack/v1" and spec_path.exists():
                    try:
                        data["pack_spec"] = json.loads(spec_path.read_text(encoding="utf-8"))
                    except Exception:
                        pass
                manifest_records.append({"data": data, "manifest_path": manifest_path})
            except Exception as exc:
                log.error("Failed to load manifest %s: %s", manifest_path, exc)
                diagnostics.append(
                    {
                        "id": None,
                        "domain": None,
                        "version": None,
                        "entrypoint": None,
                        "is_valid": False,
                        "validation_errors": [f"{manifest_path}: {exc}"],
                        "paths": {"manifest_path": str(manifest_path), "base_dir": str(manifest_path.parent)},
                    }
                )
    # validate and collect diagnostics
    for rec in manifest_records:
        data = rec["data"]
        manifest_path: Path = rec["manifest_path"]
        errors = validate_strategy_pack(data, manifest_path.parent)
        rec["errors"] = errors
    # enforce unique IDs
    seen = set()
    unique: list[StrategyManifest] = []
    asset_dirs: dict[str, str] = {}
    for rec in manifest_records:
        data = rec["data"]
        manifest_path: Path = rec["manifest_path"]
        errors = list(rec.get("errors") or [])
        mid = data.get("id")
        if not errors and mid in seen:
            errors.append(f"Duplicate strategy id detected during discovery: {mid}")
        if not errors:
            seen.add(mid)
            unique.append(StrategyManifest(**data))
            if mid:
                asset_dirs[mid] = str(manifest_path.parent)
        diagnostics.append(
            {
                "id": data.get("id"),
                "domain": data.get("domain"),
                "version": data.get("version"),
                "entrypoint": data.get("entrypoint"),
                "is_valid": len(errors) == 0,
                "validation_errors": errors,
                "paths": {"manifest_path": str(manifest_path), "base_dir": str(manifest_path.parent)},
            }
        )
    return unique, diagnostics, asset_dirs


def create_app(registry_path: str | None = None, bundle_path: str | None = None) -> FastAPI:
    load_dotenv(find_dotenv(".env.local", usecwd=True), override=False)

    @asynccontextmanager
    async def _lifespan(app_instance: FastAPI):
        """
        Best-effort bootstrap for domain composition/synthetic endpoints that
        depend on app.state.flattener / app.state.unflattener.
        """
        if os.getenv("KEHRNEL_INIT_INGESTION_RUNTIME", "true").lower() in ("1", "true", "yes"):
            if (
                getattr(app_instance.state, "flattener", None) is None
                or getattr(app_instance.state, "unflattener", None) is None
            ):
                try:
                    from kehrnel.api.bridge.app.core.config import settings as bridge_settings
                    from kehrnel.api.bridge.app.utils.config_runtime import apply_ingestion_config

                    config_internal = {
                        "apply_shortcuts": True,
                        "source": {
                            "canonical_compositions_collection": bridge_settings.COMPOSITIONS_COLL_NAME,
                            "database_name": bridge_settings.MONGODB_DB,
                        },
                        "target": {
                            "compositions_collection": bridge_settings.FLAT_COMPOSITIONS_COLL_NAME,
                            "search_collection": bridge_settings.SEARCH_COMPOSITIONS_COLL_NAME,
                            "codes_collection": "_codes",
                            "shortcuts_collection": "_shortcuts",
                            "rebuilt_collection": bridge_settings.FLAT_COMPOSITIONS_COLL_NAME,
                            "database_name": bridge_settings.MONGODB_DB,
                        },
                    }
                    await apply_ingestion_config(
                        app=app_instance,
                        config=config_internal,
                        mappings_inline=None,
                        use_mappings_file=True,
                        mappings_path=None,
                    )
                    logging.getLogger("kehrnel.bootstrap").info("Initialized default ingestion runtime")
                except Exception as exc:
                    logging.getLogger("kehrnel.bootstrap").warning(
                        "Could not initialize default ingestion runtime: %s", exc
                    )
        yield

    # Disable built-in docs handlers so we can control the HTML (favicon, titles, etc.)
    # and keep behavior consistent for all generated docs pages.
    app = FastAPI(
        title="Kehrnel Runtime",
        version="0.0.0",
        docs_url=None,
        redoc_url=None,
        lifespan=_lifespan,
        swagger_ui_parameters={"favicon_url": "/favicon.ico"},
    )

    # ========================================================================
    # Security Middleware (order matters - first added = last executed)
    # ========================================================================

    # 1. Rate Limiting (applied first to incoming requests)
    # 1. API Key Authentication
    if _is_auth_enabled():
        app.add_middleware(APIKeyAuthMiddleware)

    # 2. Rate Limiting
    rate_limit = _get_rate_limit()
    if rate_limit > 0:
        app.add_middleware(
            RateLimitMiddleware,
            requests_per_minute=rate_limit,
            max_clients=_get_rate_limit_clients(),
        )

    # 3. CORS Configuration
    cors_origins = _get_cors_origins()
    if cors_origins:
        allow_credentials = "*" not in cors_origins
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=allow_credentials,
            allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
            allow_headers=["*"],
        )

    # ========================================================================
    # Runtime Setup
    # ========================================================================

    reg_path = registry_path or os.getenv("KEHRNEL_REGISTRY_PATH") or str(Path(".kehrnel_registry.json").resolve())
    bundle_dir = Path(bundle_path) if bundle_path else Path(".kehrnel/bundles")
    bundle_store = BundleStore(bundle_dir)
    _seed_default_bundles(bundle_store)
    bindings_resolver = load_bindings_resolver_from_env()
    runtime = StrategyRuntime(
        FileActivationRegistry(Path(reg_path)),
        bundle_store=bundle_store,
        bindings_resolver=bindings_resolver,
    )
    manifests, diagnostics, asset_dirs = _load_manifests()
    # Clear stale manifests from registry before registering fresh ones from disk
    runtime.registry.clear_manifests()
    for manifest in manifests:
        runtime.register_manifest(manifest)
    app.state.strategy_runtime = runtime

    # Environment scoping note (ENGSEC-729 class): kehrnel does not have a user session model,
    # so multi-tenant isolation relies on API-key scoping (KEHRNEL_API_KEY_ENV_SCOPES) and/or
    # a single shared env id (KEHRNEL_DEFAULT_ENV_ID).
    if _is_auth_enabled():
        keys = _get_api_keys()
        env_scopes = (os.getenv("KEHRNEL_API_KEY_ENV_SCOPES") or "").strip()
        default_env = (
            os.getenv("KEHRNEL_DEFAULT_ENV_ID")
            or os.getenv("DEFAULT_ENV_ID")
            or os.getenv("ENV_ID")
            or ""
        ).strip()
        if not env_scopes and default_env and len(keys) > 1:
            logging.getLogger("kehrnel.security").warning(
                "Multiple API keys configured but no KEHRNEL_API_KEY_ENV_SCOPES set; "
                "all keys will share env_id=%s (no per-key environment isolation).",
                default_env,
            )

    jobs_store = None
    try:
        jobs_uri = (os.getenv("CORE_MONGODB_URL") or "").strip()
        jobs_db = (os.getenv("CORE_DATABASE_NAME") or "hdl_core").strip()
        jobs_collection = (os.getenv("KEHRNEL_JOBS_COLLECTION") or "synthetic_data_jobs").strip()
        if jobs_uri and jobs_db and jobs_collection:
            jobs_store = MongoSyntheticJobStore(
                uri=jobs_uri,
                database=jobs_db,
                collection=jobs_collection,
            )
    except Exception as exc:
        logging.getLogger("kehrnel.jobs").warning(
            "Store initialization failed; running with reduced persistence features: %s",
            exc,
        )
        jobs_store = None
    app.state.synthetic_job_manager = SyntheticJobManager(runtime, store=jobs_store)
    app.state.strategy_diagnostics = diagnostics
    app.state.bundle_store = bundle_store
    app.state.strategy_asset_dirs = asset_dirs
    # Store allowed strategy paths for validation
    app.state.allowed_strategy_paths = [p.resolve() for p in _strategy_paths()]

    def _strategy_openapi(domain: str, strategy: str) -> dict:
        domain_norm = (domain or "").strip().lower()
        strategy_norm = (strategy or "").strip().lower()
        if not domain_norm or not strategy_norm:
            raise HTTPException(status_code=400, detail="domain and strategy are required")
        full_strategy_id = f"{domain_norm}.{strategy_norm}"
        manifest = runtime.registry.get_manifest(full_strategy_id)
        if not manifest:
            raise HTTPException(status_code=404, detail=f"Unknown strategy: {full_strategy_id}")

        prefix = f"/api/strategies/{domain_norm}/{strategy_norm}/"
        schema = deepcopy(app.openapi())
        paths = schema.get("paths", {}) or {}
        strategy_paths = {path: spec for path, spec in paths.items() if path.startswith(prefix)}
        if not strategy_paths:
            raise HTTPException(status_code=404, detail=f"No API paths found for strategy: {full_strategy_id}")
        schema["paths"] = strategy_paths
        schema["info"] = {
            **(schema.get("info") or {}),
            "title": f"Kehrnel Strategy API - {full_strategy_id}",
            "description": f"Strategy-specific API documentation for {full_strategy_id}.",
        }
        return schema

    def _core_openapi() -> dict:
        schema = deepcopy(app.openapi())
        paths = schema.get("paths", {}) or {}
        core_paths = {
            path: spec
            for path, spec in paths.items()
            if not path.startswith("/api/strategies/")
        }
        schema["paths"] = core_paths
        schema["info"] = {
            **(schema.get("info") or {}),
            "title": "Kehrnel Core API",
            "description": "Core/runtime API documentation (non-domain, non-strategy).",
        }
        return schema

    @app.get("/docs", include_in_schema=False)
    async def docs_root():
        return get_swagger_ui_html(
            openapi_url="/openapi.json",
            title="Kehrnel Docs",
            swagger_favicon_url="/favicon.ico",
        )

    @app.get("/redoc", include_in_schema=False)
    async def redoc_root():
        return get_redoc_html(
            openapi_url="/openapi.json",
            title="Kehrnel ReDoc",
            redoc_favicon_url="/favicon.ico",
        )

    def _domain_openapi(domain: str) -> dict:
        domain_norm = (domain or "").strip().lower()
        if not domain_norm:
            raise HTTPException(status_code=400, detail="domain is required")
        prefix = f"/api/domains/{domain_norm}/"
        schema = deepcopy(app.openapi())
        paths = schema.get("paths", {}) or {}
        domain_paths = {path: spec for path, spec in paths.items() if path.startswith(prefix)}
        if not domain_paths:
            raise HTTPException(status_code=404, detail=f"No API paths found for domain: {domain_norm}")
        schema["paths"] = domain_paths
        schema["info"] = {
            **(schema.get("info") or {}),
            "title": f"Kehrnel Domain API - {domain_norm}",
            "description": f"Domain API documentation for {domain_norm}.",
        }
        return schema

    @app.get("/openapi/strategies/{domain}/{strategy}.json", include_in_schema=False)
    async def strategy_openapi(domain: str, strategy: str):
        return JSONResponse(content=_strategy_openapi(domain, strategy))

    @app.get("/openapi/strategies/{strategy_id}.json", include_in_schema=False)
    async def strategy_openapi_by_id(strategy_id: str):
        strategy_id_norm = (strategy_id or "").strip().lower()
        if "." not in strategy_id_norm:
            raise HTTPException(
                status_code=400,
                detail="strategy_id must be in '<domain>.<strategy>' format",
            )
        domain_norm, strategy_norm = strategy_id_norm.split(".", 1)
        return JSONResponse(content=_strategy_openapi(domain_norm, strategy_norm))

    @app.get("/docs/strategies/{domain}/{strategy}", include_in_schema=False)
    async def strategy_docs(domain: str, strategy: str):
        domain_norm = (domain or "").strip().lower()
        strategy_norm = (strategy or "").strip().lower()
        _strategy_openapi(domain_norm, strategy_norm)
        full_strategy_id = f"{domain_norm}.{strategy_norm}"
        return get_swagger_ui_html(
            openapi_url=f"/openapi/strategies/{domain_norm}/{strategy_norm}.json",
            title=f"Kehrnel Docs - {full_strategy_id}",
            swagger_favicon_url="/favicon.ico",
        )

    @app.get("/openapi/core.json", include_in_schema=False)
    async def core_openapi():
        return JSONResponse(content=_core_openapi())

    @app.get("/openapi/domains/{domain}.json", include_in_schema=False)
    async def domain_openapi(domain: str):
        return JSONResponse(content=_domain_openapi(domain))

    @app.get("/docs/domains/{domain}", include_in_schema=False)
    async def domain_docs(domain: str):
        domain_norm = (domain or "").strip().lower()
        _domain_openapi(domain_norm)
        return get_swagger_ui_html(
            openapi_url=f"/openapi/domains/{domain_norm}.json",
            title=f"Kehrnel Docs - domain {domain_norm}",
            swagger_favicon_url="/favicon.ico",
        )

    @app.get("/docs/core", include_in_schema=False)
    async def core_docs():
        _core_openapi()
        return get_swagger_ui_html(
            openapi_url="/openapi/core.json",
            title="Kehrnel Docs - Core",
            swagger_favicon_url="/favicon.ico",
        )

    @app.get("/redoc/core", include_in_schema=False)
    async def core_redoc():
        _core_openapi()
        return get_redoc_html(
            openapi_url="/openapi/core.json",
            title="Kehrnel ReDoc - Core",
            redoc_favicon_url="/favicon.ico",
        )

    @app.get("/redoc/strategies/{domain}/{strategy}", include_in_schema=False)
    async def strategy_redoc(domain: str, strategy: str):
        domain_norm = (domain or "").strip().lower()
        strategy_norm = (strategy or "").strip().lower()
        _strategy_openapi(domain_norm, strategy_norm)
        full_strategy_id = f"{domain_norm}.{strategy_norm}"
        return get_redoc_html(
            openapi_url=f"/openapi/strategies/{domain_norm}/{strategy_norm}.json",
            title=f"Kehrnel ReDoc - {full_strategy_id}",
            redoc_favicon_url="/favicon.ico",
        )

    @app.get("/redoc/domains/{domain}", include_in_schema=False)
    async def domain_redoc(domain: str):
        domain_norm = (domain or "").strip().lower()
        _domain_openapi(domain_norm)
        return get_redoc_html(
            openapi_url=f"/openapi/domains/{domain_norm}.json",
            title=f"Kehrnel ReDoc - domain {domain_norm}",
            redoc_favicon_url="/favicon.ico",
        )

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        debug_enabled = os.getenv("KEHRNEL_DEBUG", "false").lower() in ("1", "true", "yes")
        code = "INTERNAL_ERROR"
        status = 500
        message = str(exc) if debug_enabled else "Internal server error"
        details = {}
        if isinstance(exc, KehrnelError):
            code = exc.code
            status = exc.status
            message = str(exc)
            details = getattr(exc, "details", {}) or {}
        elif isinstance(exc, ValueError):
            code = "INVALID_INPUT"
            status = 400
        elif isinstance(exc, KeyError):
            code = "NOT_FOUND"
            status = 404
        # Defense-in-depth: avoid leaking credentials / filesystem internals in exception messages.
        message = redact_sensitive(message) or message
        return JSONResponse(status_code=status, content={"error": {"code": code, "message": message, "details": details}})
    runtime_routers = [
        admin_router,
        ops_domain_router,
        activation_router,
        fhir_domain_router,
        openehr_domain_router,
        openehr_rps_dual_router,
    ]

    for router in runtime_routers:
        app.include_router(router)

    # Mount Docusaurus documentation if build exists
    docs_build_path = Path(__file__).resolve().parents[3] / "docs" / "website" / "build"
    if docs_build_path.exists():
        app.mount("/guide", StaticFiles(directory=str(docs_build_path), html=True), name="documentation")
        logging.getLogger("kehrnel.docs").info("Documentation mounted at /guide from %s", docs_build_path)
    else:
        @app.get("/guide", include_in_schema=False)
        @app.get("/guide/", include_in_schema=False)
        async def guide_not_built():
            # Keep this public and explicit: it avoids confusion when running the API without building docs.
            return HTMLResponse(
                status_code=200,
                content=(
                    "<html><head><title>kehrnel docs</title></head><body>"
                    "<h2>Documentation is not built</h2>"
                    "<p>The kehrnel runtime serves the static Docusaurus site from <code>docs/website/build</code>.</p>"
                    "<p>Build it with:</p>"
                    "<pre><code>cd docs/website\nnpm install\nnpm run build</code></pre>"
                    "<p>Then restart the API and open <code>/guide</code> again.</p>"
                    "</body></html>"
                ),
            )

    # Serve favicon for API docs (Swagger/ReDoc) and the mounted docs site.
    # Prefer the built asset, fall back to the Docusaurus static asset during dev.
    favicon_candidates = [
        docs_build_path / "img" / "favicon.png",
        Path(__file__).resolve().parents[3] / "docs" / "website" / "static" / "img" / "favicon.png",
    ]
    favicon_path = next((p for p in favicon_candidates if p.exists()), None)
    if favicon_path:
        @app.get("/favicon.ico", include_in_schema=False)
        async def favicon():
            return FileResponse(str(favicon_path), media_type="image/png")

    return app


app = create_app()


def main():
    import argparse
    import uvicorn

    default_host = os.getenv("KEHRNEL_API_HOST", "0.0.0.0")
    default_port = int(os.getenv("KEHRNEL_API_PORT", os.getenv("API_PORT", "8000")))
    default_reload = os.getenv("KEHRNEL_API_RELOAD", "false").lower() in ("1", "true", "yes")

    parser = argparse.ArgumentParser(description="Run kehrnel API server.")
    parser.add_argument("--host", default=default_host, help=f"Bind host (default: {default_host})")
    parser.add_argument("--port", type=int, default=default_port, help=f"Bind port (default: {default_port})")
    parser.add_argument("--reload", action="store_true", default=default_reload, help="Enable auto-reload")
    args = parser.parse_args()

    uvicorn.run("kehrnel.api.app:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()

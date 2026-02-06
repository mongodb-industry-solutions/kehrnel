"""FastAPI assembly for the new kehrnel runtime."""
from __future__ import annotations

import json
import os
import logging
import secrets
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, Request, HTTPException, Security
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from kehrnel.api.admin.routes import router as admin_router
from kehrnel.api.admin.ops_by_domain import router as ops_domain_router
from kehrnel.api.admin.activation_routes import router as activation_router
from kehrnel.api.portal.routes import router as portal_router
from kehrnel.core.runtime import StrategyRuntime
from kehrnel.core.manifest import StrategyManifest
from kehrnel.core.registry import FileActivationRegistry
from kehrnel.core.errors import KehrnelError
from kehrnel.core.bundle_store import BundleStore
from kehrnel.core.pack_validator import StrategyPackValidator


# ============================================================================
# Security Configuration
# ============================================================================

# API Key Authentication
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

# Paths that don't require authentication
PUBLIC_PATHS = {"/health", "/docs", "/openapi.json", "/redoc"}


def _get_api_keys() -> set[str]:
    """Get valid API keys from environment."""
    keys_str = os.getenv("KEHRNEL_API_KEYS", "")
    if not keys_str:
        return set()
    return {k.strip() for k in keys_str.split(",") if k.strip()}


def _is_auth_enabled() -> bool:
    """Check if API key authentication is enabled."""
    return os.getenv("KEHRNEL_AUTH_ENABLED", "false").lower() in ("true", "1", "yes")


class APIKeyAuthMiddleware(BaseHTTPMiddleware):
    """Middleware to validate API keys on requests."""

    async def dispatch(self, request: Request, call_next):
        # Skip auth for public paths
        if request.url.path in PUBLIC_PATHS:
            return await call_next(request)

        # Skip if auth is disabled
        if not _is_auth_enabled():
            return await call_next(request)

        # Get API key from header
        api_key = request.headers.get("X-API-Key")
        valid_keys = _get_api_keys()

        if not valid_keys:
            # No keys configured but auth enabled - log warning and allow (for dev)
            logging.getLogger("kehrnel.security").warning(
                "API auth enabled but no keys configured (KEHRNEL_API_KEYS)"
            )
            return await call_next(request)

        if not api_key or not secrets.compare_digest(api_key, api_key):
            # Check if key is valid using constant-time comparison
            key_valid = any(secrets.compare_digest(api_key or "", k) for k in valid_keys)
            if not key_valid:
                return JSONResponse(
                    status_code=401,
                    content={"error": {"code": "UNAUTHORIZED", "message": "Invalid or missing API key"}}
                )

        return await call_next(request)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Simple in-memory rate limiting middleware."""

    def __init__(self, app, requests_per_minute: int = 60):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self._request_counts: dict[str, list[float]] = {}

    async def dispatch(self, request: Request, call_next):
        import time

        # Skip rate limiting for health checks
        if request.url.path == "/health":
            return await call_next(request)

        # Get client identifier (IP or API key)
        client_id = request.headers.get("X-API-Key") or request.client.host if request.client else "unknown"

        current_time = time.time()
        window_start = current_time - 60  # 1 minute window

        # Clean old requests and get current count
        if client_id not in self._request_counts:
            self._request_counts[client_id] = []

        self._request_counts[client_id] = [
            t for t in self._request_counts[client_id] if t > window_start
        ]

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


# ============================================================================
# Strategy Discovery
# ============================================================================

def _strategy_paths() -> list[Path]:
    paths = [Path(__file__).resolve().parents[1] / "strategies"]
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
    try:
        bundles_dir = Path(__file__).resolve().parents[1] / "strategies" / "openehr" / "rps_dual" / "bundles"
        if bundles_dir.exists():
            for path in bundles_dir.glob("*.json"):
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                    store.save_bundle(data, mode="upsert")
                except Exception:
                    continue
    except Exception:
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
    app = FastAPI(title="Kehrnel Runtime", version="0.0.0")

    # ========================================================================
    # Security Middleware (order matters - first added = last executed)
    # ========================================================================

    # 1. Rate Limiting (applied first to incoming requests)
    rate_limit = _get_rate_limit()
    if rate_limit > 0:
        app.add_middleware(RateLimitMiddleware, requests_per_minute=rate_limit)

    # 2. API Key Authentication
    if _is_auth_enabled():
        app.add_middleware(APIKeyAuthMiddleware)

    # 3. CORS Configuration
    cors_origins = _get_cors_origins()
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
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
    runtime = StrategyRuntime(FileActivationRegistry(Path(reg_path)), bundle_store=bundle_store)
    manifests, diagnostics, asset_dirs = _load_manifests()
    # Clear stale manifests from registry before registering fresh ones from disk
    runtime.registry.clear_manifests()
    for manifest in manifests:
        runtime.register_manifest(manifest)
    app.state.strategy_runtime = runtime
    app.state.strategy_diagnostics = diagnostics
    app.state.bundle_store = bundle_store
    app.state.strategy_asset_dirs = asset_dirs
    # Store allowed strategy paths for validation
    app.state.allowed_strategy_paths = [p.resolve() for p in _strategy_paths()]

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        code = "INTERNAL_ERROR"
        status = 500
        message = str(exc)
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
        return JSONResponse(status_code=status, content={"error": {"code": code, "message": message, "details": details}})
    app.include_router(admin_router)
    app.include_router(ops_domain_router)
    app.include_router(activation_router)
    app.include_router(portal_router)
    return app


app = create_app()

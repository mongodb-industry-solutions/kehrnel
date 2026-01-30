"""
Admin/runtime API for strategy discovery and environment operations.
"""
from pathlib import Path
from fastapi import APIRouter, Request, Body
from fastapi.responses import JSONResponse, FileResponse
from typing import Any, Dict, List

from kehrnel.core.manifest import StrategyManifest
from kehrnel.core.errors import KehrnelError
from kehrnel.core.bundle_store import BundleStore
from kehrnel.core.pack_loader import load_strategy

router = APIRouter()


def _error_response(exc: Exception) -> JSONResponse:
    code = "INTERNAL_ERROR"
    status = 500
    details = {}
    if hasattr(exc, "code"):
        code = getattr(exc, "code")
        status = getattr(exc, "status", status)
        details = getattr(exc, "details", {}) or {}
    elif isinstance(exc, KeyError):
        status = 404
        code = "NOT_FOUND"
    elif isinstance(exc, ValueError):
        status = 400
        code = "INVALID_INPUT"
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": str(exc), "details": details}})


def _history_summary(rt, env_id: str, domain: str):
    history_entries = rt.registry.list_history(env_id, domain) if hasattr(rt.registry, "list_history") else []
    recent = history_entries[-3:] if history_entries else []

    def _safe_entry(entry):
        activation = (entry or {}).get("activation") or {}
        return {
            "activation_id": activation.get("activation_id"),
            "strategy_id": activation.get("strategy_id"),
            "version": activation.get("version"),
            "manifest_digest": activation.get("manifest_digest"),
            "config_hash": activation.get("config_hash"),
            "timestamp": (entry or {}).get("timestamp"),
            "reason": (entry or {}).get("reason"),
        }

    return {"count": len(history_entries or []), "recent": [_safe_entry(e) for e in recent]}


@router.get("/v1/strategies", response_model=Dict[str, List[StrategyManifest]])
async def list_strategies(request: Request):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            return {"strategies": []}
        manifests = []
        for m in rt.list_strategies():
            m_dict = m.model_dump()
            # expose pack spec meta (but not full spec body) for clients
            if hasattr(m, "pack_spec") and m.pack_spec:
                m_dict["pack_spec_meta"] = {
                    "has_spec": True,
                    "encoding_profiles": [p.get("id") for p in (m.pack_spec or {}).get("encodingProfiles", []) if isinstance(p, dict)],
                    "stores": [(s or {}).get("role") or (s or {}).get("destinationType") for s in (m.pack_spec or {}).get("storage", {}).get("stores", []) if isinstance(s, dict)],
                }
                m_dict.pop("pack_spec", None)
            manifests.append(m_dict)
        return {"strategies": manifests}
    except Exception as exc:
        return _error_response(exc)


@router.get("/health", include_in_schema=False)
async def health():
    return {"ok": True}


def _validate_strategy_path(pack_path: str, request: Request) -> Path:
    """Validate that pack_path is within allowed strategy directories."""
    import os
    resolved = Path(pack_path).resolve()
    # Get allowed strategy paths from app state or environment
    allowed_paths = getattr(request.app.state, "allowed_strategy_paths", None)
    if not allowed_paths:
        # Default: only allow paths under the strategies directory
        base_strategies = Path(__file__).resolve().parents[2] / "strategies"
        allowed_paths = [base_strategies.resolve()]
        # Add paths from environment variable
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
                    allowed_paths.append(Path(part).resolve())
    # Check if resolved path is under any allowed path
    for allowed in allowed_paths:
        try:
            resolved.relative_to(allowed)
            return resolved
        except ValueError:
            continue
    raise KehrnelError(
        code="PATH_NOT_ALLOWED",
        status=403,
        message="Strategy path is outside allowed directories"
    )


@router.post("/v1/strategies/load", include_in_schema=False)
async def load_strategy_pack(request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise KehrnelError(code="RUNTIME_NOT_INITIALIZED", status=503, message="Strategy runtime not initialized")
        pack_path = body.get("path") or body.get("packPath")
        strategy_id = body.get("strategy_id") or body.get("strategyId")
        if not pack_path:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="path is required")
        # Validate path is within allowed directories
        validated_path = _validate_strategy_path(pack_path, request)
        manifest = load_strategy(strategy_id, str(validated_path))
        rt.register_manifest(manifest)
        return {"ok": True, "strategy": manifest}
    except Exception as exc:
        return _error_response(exc)


@router.get("/v1/endpoints", include_in_schema=False)
async def endpoints_registry(request: Request):
    base = ""
    endpoints = {
        "health": f"{base}/health",
        "strategies": f"{base}/v1/strategies",
        "strategy_detail": f"{base}/v1/strategies/{{strategy_id}}",
        "strategy_spec": f"{base}/v1/strategies/{{strategy_id}}/spec",
        "bundles": f"{base}/v1/bundles",
        "ops": f"{base}/v1/ops",
        "activations": f"{base}/v1/environments/{{env_id}}/activate",
    }
    return {"endpoints": endpoints}


@router.get("/v1/strategies/{strategy_id}/endpoints", include_in_schema=False)
async def strategy_endpoints(strategy_id: str, request: Request):
    base = ""
    return {
        "strategy_id": strategy_id,
        "endpoints": {
            "activate": f"{base}/v1/environments/{{env_id}}/activate",
            "compile_query": f"{base}/v1/environments/{{env_id}}/compile_query",
            "execute_query": f"{base}/v1/environments/{{env_id}}/execute_query",
            "ops": f"{base}/v1/environments/{{env_id}}/activations/{{domain}}/ops/{{op}}",
        },
    }


@router.get("/v1/strategies/diagnostics", include_in_schema=False)
async def get_strategy_diagnostics(request: Request):
    try:
        diagnostics = getattr(request.app.state, "strategy_diagnostics", None) or []
        return {"strategies": diagnostics}
    except Exception as exc:
        return _error_response(exc)


@router.get("/v1/ops", include_in_schema=False)
async def list_ops(request: Request):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        ops = []
        for manifest in rt.list_strategies():
            for op in manifest.ops:
                ops.append({"strategy_id": manifest.id, "domain": manifest.domain, "name": op.name, "kind": op.kind, "summary": op.summary})
        return {"ops": ops}
    except Exception as exc:
        return _error_response(exc)


@router.post("/v1/ops", include_in_schema=False)
async def run_op(request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    try:
        env_id = body.get("environment") or body.get("env_id")
        domain = body.get("domain")
        op = body.get("op")
        payload = body.get("payload") or {}
        if not env_id or not domain or not op:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="environment, domain, and op are required")
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        result = await rt.dispatch(env_id, "op", {"op": op, "payload": payload, "domain": domain})
        return {"ok": True, "result": result}
    except Exception as exc:
        return _error_response(exc)


@router.get("/v1/bundles", include_in_schema=False)
async def list_bundles(request: Request):
    try:
        store: BundleStore = getattr(request.app.state, "bundle_store", None)
        bundles = store.list_bundles() if store else []
        return {"bundles": bundles}
    except Exception as exc:
        return _error_response(exc)


@router.get("/v1/bundles/{bundle_id}", include_in_schema=False)
async def get_bundle(bundle_id: str, request: Request):
    try:
        store: BundleStore = getattr(request.app.state, "bundle_store", None)
        if not store:
            raise KehrnelError(code="BUNDLE_NOT_AVAILABLE", status=503, message="Bundle store not configured")
        bundle = store.get_bundle(bundle_id)
        return {"bundle": bundle}
    except Exception as exc:
        return _error_response(exc)


@router.post("/v1/bundles", include_in_schema=False)
async def import_bundle(request: Request, body: Dict[str, Any] = Body(default_factory=dict), mode: str = "error"):
    try:
        store: BundleStore = getattr(request.app.state, "bundle_store", None)
        if not store:
            raise KehrnelError(code="BUNDLE_NOT_AVAILABLE", status=503, message="Bundle store not configured")
        res = store.save_bundle(body, mode=mode)
        return {"bundle": res}
    except Exception as exc:
        return _error_response(exc)


@router.delete("/v1/bundles/{bundle_id}", include_in_schema=False)
async def delete_bundle(bundle_id: str, request: Request):
    try:
        store: BundleStore = getattr(request.app.state, "bundle_store", None)
        if not store:
            raise KehrnelError(code="BUNDLE_NOT_AVAILABLE", status=503, message="Bundle store not configured")
        store.delete_bundle(bundle_id)
        return {"ok": True}
    except Exception as exc:
        return _error_response(exc)


@router.get("/v1/strategies/{strategy_id}", response_model=StrategyManifest)
async def get_strategy(strategy_id: str, request: Request):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        manifest = rt.registry.get_manifest(strategy_id)
        if not manifest:
            raise KeyError("Strategy not found")
        data = manifest.model_dump()
        if hasattr(manifest, "pack_spec") and manifest.pack_spec:
            data["pack_spec"] = manifest.pack_spec
        return data
    except Exception as exc:
        return _error_response(exc)


@router.get("/v1/strategies/{strategy_id}/assets/{asset_path:path}")
async def get_strategy_asset(strategy_id: str, asset_path: str, request: Request):
    try:
        asset_dirs = getattr(request.app.state, "strategy_asset_dirs", None) or {}
        base_dir = asset_dirs.get(strategy_id)
        if not base_dir:
            raise KehrnelError(code="STRATEGY_NOT_FOUND", status=404, message=f"Strategy {strategy_id} not found")
        # Explicit path traversal protection
        if ".." in asset_path or asset_path.startswith("/"):
            raise KehrnelError(code="INVALID_PATH", status=400, message="Path traversal not allowed")
        base_path = Path(base_dir).resolve()
        safe_asset = asset_path.lstrip("/")
        # Additional check for null bytes and other dangerous characters
        if "\x00" in safe_asset or "\\" in safe_asset:
            raise KehrnelError(code="INVALID_PATH", status=400, message="Invalid characters in path")
        candidate = (base_path / safe_asset).resolve()
        if base_path not in candidate.parents and candidate != base_path:
            raise KehrnelError(code="ASSET_OUT_OF_BOUNDS", status=400, message="Invalid asset path")
        if not candidate.exists() or not candidate.is_file():
            raise KehrnelError(code="ASSET_NOT_FOUND", status=404, message="Asset not found")
        return FileResponse(str(candidate))
    except Exception as exc:
        return _error_response(exc)


@router.get("/v1/strategies/{strategy_id}/spec")
async def get_strategy_spec(strategy_id: str, request: Request):
    """Return the full spec.json for a strategy pack."""
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        manifest = rt.registry.get_manifest(strategy_id)
        if not manifest:
            raise KeyError(f"Strategy {strategy_id} not found")
        pack_spec = getattr(manifest, "pack_spec", None)
        if not pack_spec:
            raise KehrnelError(
                code="SPEC_NOT_AVAILABLE",
                status=404,
                message=f"Strategy {strategy_id} does not have a spec.json (pack_format must be strategy-pack/v1)",
            )
        return {"strategy_id": strategy_id, "spec": pack_spec}
    except Exception as exc:
        return _error_response(exc)


@router.post("/v1/environments/{env_id}/extensions/{strategy_id}/{op}", include_in_schema=False)
async def run_extension(env_id: str, strategy_id: str, op: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        activation = rt.registry.get_activation_by_strategy(env_id, strategy_id)
        if not activation:
            raise KeyError(f"No activation for env {env_id}")
        if activation.strategy_id != strategy_id:
            raise ValueError(f"Environment {env_id} active with {activation.strategy_id}, not {strategy_id}")
        result = await rt.dispatch(env_id, "op", {"op": op, "payload": payload or {}, "strategy_id": strategy_id})
        return {"ok": True, "result": result}
    except Exception as exc:
        return _error_response(exc)


@router.post("/v1/environments/{env_id}/activate", include_in_schema=False)
async def activate_env(env_id: str, request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        strategy_id = body.get("strategy_id") or body.get("strategyId")
        version = body.get("version") or "latest"
        config = body.get("config") or {}
        bindings = body.get("bindings") or {}
        domain = (body.get("domain") or "").lower()
        allow_plain = body.get("allow_plaintext_bindings", False)
        force = bool(body.get("force"))
        replace_reason = body.get("reason")
        if not strategy_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="strategy_id is required")
        if not domain:
            raise KehrnelError(code="DOMAIN_REQUIRED", status=400, message="domain is required")
        from kehrnel.strategy_sdk import StrategyBindings
        activation = await rt.activate(
            env_id,
            strategy_id,
            version,
            config,
            StrategyBindings(**bindings),
            allow_plaintext_bindings=allow_plain,
            domain=domain,
            force=force,
            replace_reason=replace_reason,
        )
        return {
            "ok": True,
            "activation": {
                "env_id": activation.env_id,
                "strategy_id": activation.strategy_id,
                "version": activation.version,
                "strategy_version": activation.version,
                "domain": activation.domain,
                "config": activation.config,
                "bindings_meta": activation.bindings_meta,
                "config_hash": activation.config_hash,
                "manifest_digest": activation.manifest_digest,
                "activation_id": activation.activation_id,
                "replaced": activation.replaced,
                "previous_activation_id": activation.previous_activation_id,
                "replaced_from": activation.replaced_from,
                "already_active": activation.already_active,
            },
        }
    except Exception as exc:
        return _error_response(exc)


@router.put("/v1/environments/strategy", include_in_schema=False)
async def activate_strategy_compat(request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    """Compatibility activation endpoint for HDL proxy."""
    env_id = body.get("envId") or body.get("environment") or body.get("env_id")
    if not env_id:
        return _error_response(KehrnelError(code="INVALID_INPUT", status=400, message="envId is required"))
    mapped = {
        "strategy_id": body.get("strategyId") or body.get("strategy_id"),
        "version": body.get("version") or body.get("strategyVersion") or "latest",
        "config": body.get("configOverrides") or body.get("config") or {},
        "bindings": body.get("bindings") or {},
        "allow_plaintext_bindings": True,
        "domain": (body.get("domain") or "").lower() or body.get("domain"),
        "force": body.get("force") or False,
        "reason": body.get("reason"),
    }
    return await activate_env(env_id, request, mapped)


@router.post("/v1/strategies/activate", include_in_schema=False)
async def activate_env_v2(request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    env_id = body.get("environment")
    if not env_id:
        return _error_response(KehrnelError(code="INVALID_INPUT", status=400, message="environment is required"))
    return await activate_env(env_id, request, body)


@router.get("/v1/environments/{env_id}", include_in_schema=False)
async def get_env(env_id: str, request: Request):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        activations = rt.registry.list_activations(env_id)
        if not activations:
            raise KeyError("No activation for env")
        summary = {}
        history_summary = {}
        for proto, activation in activations.items():
            manifest = rt.registry.get_manifest(activation.strategy_id)
            summary[proto] = {
                "strategy_id": activation.strategy_id,
                "strategy_version": activation.version,
                "version": activation.version,
                "manifest_digest": activation.manifest_digest,
                "domain": getattr(manifest, "domain", proto),
                "config": activation.config,
                "bindings_meta": activation.bindings_meta,
                "activation_id": activation.activation_id,
                "activated_at": activation.activated_at,
                "updated_at": activation.updated_at,
                "config_hash": activation.config_hash,
            }
            history_summary[proto] = _history_summary(rt, env_id, proto)
        return {"env_id": env_id, "activations": summary, "history": history_summary}
    except Exception as exc:
        return _error_response(exc)


@router.get("/v1/environments/{env_id}/activations", include_in_schema=False)
async def list_env_activations(env_id: str, request: Request):
    return await get_env(env_id, request)


@router.post("/v1/environments/{env_id}/activations/{domain}/upgrade", include_in_schema=False)
async def upgrade_activation(env_id: str, domain: str, request: Request):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        new_act = await rt.upgrade_activation(env_id, domain)
        return {
            "ok": True,
            "activation": {
                "activation_id": new_act.activation_id,
                "strategy_id": new_act.strategy_id,
                "version": new_act.version,
                "domain": new_act.domain,
                "manifest_digest": new_act.manifest_digest,
                "config": new_act.config,
                "bindings_meta": new_act.bindings_meta,
            },
        }
    except Exception as exc:
        return _error_response(exc)


@router.post("/v1/environments/{env_id}/activations/{domain}/rollback", include_in_schema=False)
async def rollback_activation(env_id: str, domain: str, request: Request):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        act = await rt.rollback_activation(env_id, domain)
        return {
            "ok": True,
            "activation": {
                "activation_id": act.activation_id,
                "strategy_id": act.strategy_id,
                "version": act.version,
                "domain": act.domain,
                "manifest_digest": act.manifest_digest,
                "config": act.config,
                "bindings_meta": act.bindings_meta,
                "config_hash": act.config_hash,
            },
        }
    except Exception as exc:
        return _error_response(exc)


@router.delete("/v1/environments/{env_id}/activations/{domain}", include_in_schema=False)
async def delete_activation(env_id: str, domain: str, request: Request):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        act = rt.delete_activation(env_id, domain)
        return {"ok": True, "activation": {"activation_id": act.activation_id, "domain": act.domain}}
    except Exception as exc:
        return _error_response(exc)


@router.post("/v1/environments/{env_id}/plan", include_in_schema=False)
async def plan_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        res = await rt.dispatch(env_id, "plan", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:
        return _error_response(exc)


@router.post("/v1/environments/{env_id}/apply", include_in_schema=False)
async def apply_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        res = await rt.dispatch(env_id, "apply", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:
        return _error_response(exc)


@router.post("/v1/environments/{env_id}/transform", include_in_schema=False)
async def transform_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        res = await rt.dispatch(env_id, "transform", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:
        return _error_response(exc)


@router.post("/v1/environments/{env_id}/ingest", include_in_schema=False)
async def ingest_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        res = await rt.dispatch(env_id, "ingest", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:
        return _error_response(exc)


@router.post("/v1/environments/{env_id}/query", include_in_schema=False)
async def query_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        domain = (payload or {}).get("domain")
        if not domain:
            raise KehrnelError(code="DOMAIN_REQUIRED", status=400, message="domain is required")
        payload["domain"] = domain
        res = await rt.dispatch(env_id, "query", payload or {})
        # when dispatch returns QueryResult dict or simple, wrap as ok/result if needed
        if isinstance(res, dict) and "ok" in res:
            return res
        return {"ok": True, "result": res}
    except Exception as exc:
        return _error_response(exc)


@router.post("/v1/environments/{env_id}/compile_query", include_in_schema=False)
async def compile_query_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict), debug: bool = False):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        domain = (payload or {}).get("domain")
        if not domain:
            raise KehrnelError(code="DOMAIN_REQUIRED", status=400, message="domain is required")
        payload["domain"] = domain
        if debug and isinstance(payload, dict):
            payload["debug"] = True
        res = await rt.dispatch(env_id, "compile_query", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:
        return _error_response(exc)


@router.get("/v1/environments/{env_id}/endpoints", include_in_schema=False)
async def list_env_endpoints(env_id: str, request: Request):
    try:
        base = str(request.base_url).rstrip("/")
        rt = getattr(request.app.state, "strategy_runtime", None)
        activations = rt.registry.list_activations(env_id) if rt else {}
        domains = []
        for domain_key, activation in activations.items():
            domains.append(
                {
                    "domain": domain_key,
                    "activation_id": activation.activation_id,
                    "strategy_id": activation.strategy_id,
                    "strategy_version": activation.version,
                }
            )
        sample_domain = domains[0]["domain"] if domains else "domain"
        return {
            "env_id": env_id,
            "domains": domains,
            "endpoints": {
                "compile_query": {
                    "url": f"{base}/v1/environments/{env_id}/compile_query",
                    "method": "POST",
                    "required_params": ["domain"],
                    "payload_example": {"domain": sample_domain, "query": {"scope": "patient", "predicates": [], "select": []}},
                },
                "query": {
                    "url": f"{base}/v1/environments/{env_id}/query",
                    "method": "POST",
                    "required_params": ["domain"],
                    "payload_example": {"domain": sample_domain, "query": {"scope": "patient", "predicates": [], "select": []}},
                },
                "activations": {"url": f"{base}/v1/environments/{env_id}/activations", "method": "GET"},
                "ops": {
                    "url": f"{base}/v1/environments/{env_id}/extensions/{{strategy_id}}/{{op}}",
                    "method": "POST",
                    "required_params": ["strategy_id", "op"],
                    "payload_example": {"payload": {}, "domain": sample_domain},
                },
            },
        }
    except Exception as exc:
        return _error_response(exc)

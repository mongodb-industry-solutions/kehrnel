"""StrategyRuntime scaffold (to be implemented in later steps)."""
from __future__ import annotations

import importlib
from dataclasses import dataclass, field, asdict, is_dataclass
from typing import Dict, Optional, Any

from .manifest import StrategyManifest
from .activation import EnvironmentActivation
from .registry import ActivationRegistry, FileActivationRegistry
from strategy_sdk import StrategyHandle, StrategyBindings
from .types import QueryPlan, QueryResult, ApplyResult, TransformResult, StrategyContext
from kehrnel.adapters.mongodb.connection import get_database as _mongo_get_db
from kehrnel.adapters.mongodb.storage import MongoStorageAdapter
from kehrnel.adapters.mongodb.index_admin import MongoIndexAdminAdapter
from kehrnel.adapters.mongodb.atlas_search import MongoAtlasSearchAdapter


class StrategyRuntime:
    def __init__(self, registry: ActivationRegistry):
        self.registry = registry
        self.env_manifests: Dict[str, StrategyManifest] = {}
        # per-env cache: {"adapters": {...}, "dict_cache": {...}}
        self._env_cache: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def from_file(cls, path) -> "StrategyRuntime":
        reg = FileActivationRegistry(path)
        return cls(registry=reg)

    def register_manifest(self, manifest: StrategyManifest):
        self.registry.register_manifest(manifest)

    def list_strategies(self):
        return self.registry.list_manifests()

    async def activate(self, env_id: str, strategy_id: str, version: str, config: dict, bindings: StrategyBindings, allow_plaintext_bindings: bool = False):
        manifest = self.registry.get_manifest(strategy_id)
        if not manifest:
            raise ValueError(f"Strategy {strategy_id} not registered")
        # invalidate cache for this env
        self.invalidate_env_cache(env_id)
        # redact uri
        bdict = bindings.__dict__
        uri = (bdict.get("db") or {}).get("uri")
        redacted_uri = None
        if uri:
            redacted_uri = self._redact_uri(uri)
        bindings_meta = {
            "db": {
                "provider": (bdict.get("db") or {}).get("provider"),
                "database": (bdict.get("db") or {}).get("database"),
                "uri": redacted_uri,
            }
        }
        ctx = StrategyContext(environment_id=env_id, config=config, bindings=bdict if allow_plaintext_bindings else None, adapters=None, manifest=manifest)
        # load plugin
        mod_path, cls_name = manifest.entrypoint.split(":")
        mod = importlib.import_module(mod_path)
        plugin_cls = getattr(mod, cls_name)
        handle = StrategyHandle(manifest, plugin_cls(manifest))
        await _maybe_await(handle.activate, config=config, bindings=bindings, context=ctx)
        activation = EnvironmentActivation(
            env_id=env_id,
            strategy_id=strategy_id,
            version=version,
            config=config,
            bindings_meta=bindings_meta,
            activation_id=None,
            bindings=bdict if allow_plaintext_bindings else None,
        )
        self.registry.activate(activation)
        return activation

    async def dispatch(self, env_id: str, op: str, payload: dict):
        activation = self.registry.get_activation(env_id)
        if not activation:
            raise ValueError(f"No activation for env {env_id}")

        manifest = self.registry.get_manifest(activation.strategy_id)
        if not manifest:
            raise ValueError(f"Strategy {activation.strategy_id} not registered")

        bindings_payload = (payload or {}).get("bindings") if isinstance(payload, dict) else None
        effective_bindings = activation.bindings or bindings_payload
        if not effective_bindings:
            raise ValueError("Bindings not stored; re-activate with allow_plaintext_bindings=true or provide bindings in request.")

        cache = self._env_cache.setdefault(env_id, {}).setdefault("dict_cache", {})
        ctx = StrategyContext(
            environment_id=env_id,
            config=activation.config,
            bindings=effective_bindings,
            adapters=self._build_adapters(env_id, effective_bindings),
            manifest=manifest,
            meta={"dict_cache": cache},
        )
        mod_path, cls_name = manifest.entrypoint.split(":")
        mod = importlib.import_module(mod_path)
        plugin_cls = getattr(mod, cls_name)
        handle = StrategyHandle(manifest, plugin_cls(manifest))
        # rebuild bindings from stored meta if possible
        bindings = StrategyBindings(**(effective_bindings or {}))
        await _maybe_await(handle.activate, config=activation.config, bindings=bindings, context=ctx)

        # Route ops
        op_lower = op.lower()
        if op_lower == "search":
            op_lower = "query"
        try:
            if op_lower == "validate":
                return await handle.plugin.validate_config(ctx)
            if op_lower == "plan":
                return _to_dict(await handle.plugin.plan(ctx))
            if op_lower == "apply":
                plan = payload.get("plan") if payload else None
                return _to_dict(await handle.plugin.apply(ctx, plan))
            if op_lower == "transform":
                return _to_dict(await handle.plugin.transform(ctx, payload or {}))
            if op_lower == "reverse_transform":
                return _to_dict(await handle.plugin.reverse_transform(ctx, payload or {}))
            if op_lower == "ingest":
                return await handle.plugin.ingest(ctx, payload or {})
            if op_lower == "compile_query":
                protocol = payload.get("protocol")
                query = payload.get("query")
                return _to_dict(await handle.plugin.compile_query(ctx, protocol=protocol, query=query))
            if op_lower == "execute_query":
                plan = payload.get("plan")
                return _to_dict(await handle.plugin.execute_query(ctx, plan))
            if op_lower == "query":
                protocol = payload.get("protocol")
                query = payload.get("query")
                plan = await handle.plugin.compile_query(ctx, protocol=protocol, query=query)
                res = await handle.plugin.execute_query(ctx, plan)
                return _to_dict(res)
            if op_lower in ("op", "extensions"):
                op_name = payload.get("op") if payload else None
                op_payload = payload.get("payload") if payload else {}
                if not op_name:
                    raise ValueError("op name required")
                res = _to_dict(await handle.plugin.run_op(ctx, op_name, op_payload))
                if self._is_maintenance_op(manifest, op_name):
                    self.invalidate_env_cache(env_id, dict_only=True)
                return res
        except Exception as exc:
            raise
        raise ValueError(f"Operation {op} not supported")

    def _build_adapters(self, env_id: str, bindings: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        env_cache = self._env_cache.setdefault(env_id, {})
        if "adapters" in env_cache:
            return env_cache["adapters"]
        adapters: Dict[str, Any] = {}
        db_cfg = (bindings or {}).get("db") if isinstance(bindings, dict) else None
        if db_cfg and db_cfg.get("provider") == "mongodb":
            db = _mongo_get_db(bindings)
            adapters["storage"] = MongoStorageAdapter(db)
            adapters["index_admin"] = MongoIndexAdminAdapter(db)
            adapters["atlas_search"] = MongoAtlasSearchAdapter(db)
            # backward-compatible alias (deprecated)
            adapters["text_search"] = adapters["atlas_search"]
        env_cache["adapters"] = adapters
        return adapters

    def _redact_uri(self, uri: str) -> str:
        if "@" in uri:
            parts = uri.split("://", 1)
            scheme = parts[0] if len(parts) > 1 else "mongodb"
            rest = parts[1] if len(parts) > 1 else parts[0]
            if "@" in rest:
                host_part = rest.split("@", 1)[1]
                return f"{scheme}://***:***@{host_part}"
        return uri

    def invalidate_env_cache(self, env_id: str, dict_only: bool = False):
        if dict_only:
            if env_id in self._env_cache and "dict_cache" in self._env_cache[env_id]:
                self._env_cache[env_id]["dict_cache"] = {}
            return
        self._env_cache.pop(env_id, None)

    def _is_maintenance_op(self, manifest: StrategyManifest, op_name: str) -> bool:
        for op in manifest.ops:
            if op.name == op_name and op.kind.lower() == "maintenance":
                return True
        return False


def _to_dict(obj: Any) -> Any:
    if is_dataclass(obj):
        return _to_dict(asdict(obj))
    if hasattr(obj, "model_dump"):
        return _to_dict(obj.model_dump())
    if hasattr(obj, "__dict__") and not isinstance(obj, (str, bytes, int, float, bool)):
        return {k: _to_dict(v) for k, v in obj.__dict__.items()}
    if isinstance(obj, list):
        return [_to_dict(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _to_dict(v) for k, v in obj.items()}
    return obj


async def _maybe_await(fn, *args, **kwargs):
    res = fn(*args, **kwargs)
    if hasattr(res, "__await__"):
        return await res
    return res

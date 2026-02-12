"""StrategyRuntime scaffold (to be implemented in later steps)."""
from __future__ import annotations

import importlib
import importlib.util
import json
import hashlib
from datetime import datetime
from typing import Dict, Optional, Any
from dataclasses import is_dataclass, asdict

from .manifest import StrategyManifest
from .config import validate_config
from .activation import EnvironmentActivation
from .registry import ActivationRegistry, FileActivationRegistry
from kehrnel.core.errors import KehrnelError
from kehrnel.core.bundle_store import BundleStore
from kehrnel.core.bundles import compute_bundle_digest
from kehrnel.strategy_sdk import StrategyHandle, StrategyBindings
from .types import QueryPlan, QueryResult, ApplyResult, TransformResult, StrategyContext
from .bindings_resolver import resolve_bindings as _resolve_bindings_ref
from kehrnel.persistence.mongodb.connection import get_database as _mongo_get_db
from kehrnel.persistence.mongodb.storage import MongoStorageAdapter
from kehrnel.persistence.mongodb.index_admin import MongoIndexAdminAdapter
from kehrnel.persistence.mongodb.atlas_search import MongoAtlasSearchAdapter
from pathlib import Path


class StrategyRuntime:
    def __init__(self, registry: ActivationRegistry, bundle_store=None, bindings_resolver=None):
        self.registry = registry
        self.bundle_store = bundle_store
        self.bindings_resolver = bindings_resolver
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

    @staticmethod
    def _has_meaningful_bindings(bindings: StrategyBindings | None) -> bool:
        """
        Treat only real inline adapter values as plaintext bindings.
        Dataclass defaults (None / empty dict) are not considered meaningful.
        """
        if not bindings:
            return False
        raw = getattr(bindings, "__dict__", {}) or {}
        if not raw:
            return False
        for value in raw.values():
            if value is None:
                continue
            if isinstance(value, dict) and len(value) == 0:
                continue
            return True
        return False

    async def activate(
        self,
        env_id: str,
        strategy_id: str,
        version: str,
        config: dict,
        bindings: StrategyBindings,
        allow_plaintext_bindings: bool = False,
        domain: str | None = None,
        reason: str = "activate",
        manifest_digest_override: str | None = None,
        force: bool = False,
        replace_reason: str | None = None,
        bindings_ref: str | None = None,
    ):
        manifest = self.registry.get_manifest(strategy_id)
        if not manifest:
            raise ValueError(f"Strategy {strategy_id} not registered")
        domain = domain or getattr(manifest, "domain", None)
        chosen_domain = domain
        if not chosen_domain:
            raise ValueError("Domain required for activation")
        chosen_domain = str(chosen_domain).strip().lower()
        existing = self.registry.get_activation(env_id, chosen_domain)
        incoming_bindings = self._has_meaningful_bindings(bindings)
        if incoming_bindings or allow_plaintext_bindings:
            raise KehrnelError(
                code="PLAINTEXT_BINDINGS_FORBIDDEN",
                status=400,
                message="Plaintext bindings are not allowed. Use bindings_ref with a configured resolver.",
            )
        if not bindings_ref:
            raise KehrnelError(
                code="BINDINGS_REF_REQUIRED",
                status=400,
                message="bindings_ref is required for activation.",
            )
        existing_hash = existing.config_hash if existing else None
        # idempotent: if same strategy and config hash matches, return existing
        merged_config: dict = {}
        defaults = manifest.default_config or self._load_defaults_from_entrypoint(manifest)
        if defaults:
            merged_config = self._deep_merge(merged_config, defaults)
        merged_config = self._deep_merge(merged_config, config or {})
        # simple required validation from schema if present
        schema = getattr(manifest, "config_schema", None) or self._load_schema_from_entrypoint(manifest) or {}
        validate_config(schema, merged_config)
        self._validate_pack_config(manifest, merged_config)
        store_profiles = self._build_store_profiles(manifest, merged_config)
        bundle_refs = self._validate_bundle_refs(merged_config, manifest)
        new_config_hash = self._config_hash(merged_config)
        if existing and not force:
            if existing.strategy_id == strategy_id and existing_hash == new_config_hash:
                existing.already_active = True
                return existing
            # switch strategy or config changed: replace
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
        ctx = StrategyContext(environment_id=env_id, config=merged_config, bindings=None, adapters=None, manifest=manifest)
        # load plugin
        mod_path, cls_name = manifest.entrypoint.split(":")
        mod = importlib.import_module(mod_path)
        plugin_cls = getattr(mod, cls_name)
        handle = StrategyHandle(manifest, plugin_cls(manifest))
        await _maybe_await(handle.activate, config=merged_config, bindings=bindings, context=ctx)
        now = datetime.utcnow().isoformat()
        manifest_digest = manifest_digest_override or self._manifest_digest(manifest)
        activation = EnvironmentActivation(
            env_id=env_id,
            domain=chosen_domain,
            strategy_id=strategy_id,
            version=version,
            manifest_digest=manifest_digest,
            config=merged_config,
            bindings_meta=bindings_meta,
            activation_id=None,
            config_hash=new_config_hash,
            activated_at=now,
            updated_at=now,
            bindings=None,
            bindings_ref=bindings_ref,
            replaced=bool(existing),
            previous_activation_id=getattr(existing, "activation_id", None) if existing else None,
            replaced_from={"activation_id": existing.activation_id, "strategy_id": existing.strategy_id, "version": existing.version} if existing else None,
            bundle_refs=bundle_refs,
            store_profiles=store_profiles,
        )
        hist_reason = replace_reason or (reason if existing else reason) or ("replace" if existing else "activate")
        if existing and not hist_reason:
            hist_reason = "replace"
        self.registry.activate(activation, reason=hist_reason or "activate")
        return activation

    async def upgrade_activation(self, env_id: str, domain: str) -> EnvironmentActivation:
        activation = self.registry.get_activation(env_id, domain)
        if not activation:
            raise KehrnelError(code="ACTIVATION_NOT_FOUND", status=404, message=f"No activation for env {env_id} (domain={domain})")
        if not activation.bindings and not activation.bindings_ref:
            raise KehrnelError(
                code="BINDINGS_NOT_STORED",
                status=400,
                message="Bindings not stored; provide bindings_ref or re-activate with allow_plaintext_bindings=true.",
            )
        from kehrnel.strategy_sdk import StrategyBindings
        rebind = StrategyBindings(**(activation.bindings or {}))
        return await self.activate(
            env_id,
            activation.strategy_id,
            activation.version,
            activation.config,
            rebind,
            allow_plaintext_bindings=False,
            domain=activation.domain or domain,
            reason="upgrade",
            force=True,
            replace_reason="upgrade",
            bindings_ref=activation.bindings_ref,
        )

    async def rollback_activation(self, env_id: str, domain: str) -> EnvironmentActivation:
        history_entry = self.registry.pop_history(env_id, domain)
        if not history_entry:
            raise KehrnelError(code="ROLLBACK_NOT_AVAILABLE", status=409, message="No rollback history for activation")
        snapshot = history_entry.get("activation") or {}
        prev_activation = EnvironmentActivation(**snapshot)
        if not prev_activation.bindings and not prev_activation.bindings_ref:
            raise KehrnelError(code="BINDINGS_NOT_STORED", status=400, message="Bindings not stored; cannot rollback.")
        from kehrnel.strategy_sdk import StrategyBindings
        rebind = StrategyBindings(**(prev_activation.bindings or {}))
        return await self.activate(
            env_id,
            prev_activation.strategy_id,
            prev_activation.version,
            prev_activation.config,
            rebind,
            allow_plaintext_bindings=False,
            domain=prev_activation.domain or domain,
            reason="rollback",
            manifest_digest_override=prev_activation.manifest_digest,
            force=True,
            replace_reason="rollback",
            bindings_ref=prev_activation.bindings_ref,
        )

    def delete_activation(self, env_id: str, domain: str) -> EnvironmentActivation:
        removed = self.registry.deactivate(env_id, domain, reason="deactivate")
        if not removed:
            raise KehrnelError(code="ACTIVATION_NOT_FOUND", status=404, message=f"No activation for env {env_id} (domain={domain})")
        self.invalidate_env_cache(env_id)
        return removed

    async def dispatch(self, env_id: str, op: str, payload: dict):
        activation = None
        requested_domain = None
        allow_mismatch = False
        if isinstance(payload, dict):
            dom_val = payload.get("domain")
            if dom_val:
                requested_domain = str(dom_val).lower()
            allow_mismatch = bool(payload.get("allow_mismatch"))

        # compile/query must explicitly provide domain
        if op.lower() in ("compile_query", "query") and not requested_domain:
            raise KehrnelError(code="DOMAIN_REQUIRED", status=400, message="domain is required")

        if requested_domain:
            activation = self.registry.get_activation(env_id, requested_domain)
        if not activation:
            activation = self.registry.get_activation(env_id)
        if not activation:
            raise KehrnelError(code="ACTIVATION_NOT_FOUND", status=404, message=f"No activation for env {env_id} (domain={requested_domain})", details={"env_id": env_id, "domain": requested_domain})

        manifest = self.registry.get_manifest(activation.strategy_id)
        if not manifest:
            raise ValueError(f"Strategy {activation.strategy_id} not registered")
        current_digest = self._manifest_digest(manifest)
        activation_version = (activation.version or "").strip()
        is_latest_alias = activation_version.lower() in ("latest", "current")
        digest_mismatch = bool(activation.manifest_digest and activation.manifest_digest != current_digest)
        # If digest matches, treat activation as compatible even if version label is "latest".
        version_mismatch = bool(
            activation_version
            and not is_latest_alias
            and activation_version != manifest.version
        )
        if digest_mismatch or version_mismatch:
            details = {
                "expected_version": activation.version,
                "actual_version": manifest.version,
                "expected_digest": activation.manifest_digest,
                "actual_digest": current_digest,
            }
            if not allow_mismatch:
                raise KehrnelError(code="ACTIVATION_STRATEGY_MISMATCH", status=409, message="Active strategy differs from current manifest", details=details)

        bindings_payload = (payload or {}).get("bindings") if isinstance(payload, dict) else None
        if bindings_payload:
            raise KehrnelError(
                code="PLAINTEXT_BINDINGS_FORBIDDEN",
                status=400,
                message="Per-request plaintext bindings are not allowed. Use bindings_ref activation.",
            )
        effective_bindings = activation.bindings
        if not effective_bindings and activation.bindings_ref:
            if not effective_bindings and self.bindings_resolver is None:
                raise KehrnelError(
                    code="BINDINGS_RESOLVER_NOT_CONFIGURED",
                    status=500,
                    message=(
                        "Activation uses bindings_ref but no bindings resolver is configured. "
                        "Set KEHRNEL_BINDINGS_RESOLVER or configure HDL resolver env vars."
                    ),
                    details={
                        "env_id": env_id,
                        "domain": activation.domain,
                        "strategy_id": activation.strategy_id,
                        "activation_id": activation.activation_id,
                        "bindings_ref": activation.bindings_ref,
                        "required_env": [
                            "KEHRNEL_BINDINGS_RESOLVER",
                            "ENV_SECRETS_KEY",
                            "CORE_MONGODB_URL",
                            "CORE_DATABASE_NAME",
                        ],
                    },
                )
            try:
                effective_bindings = await _resolve_bindings_ref(
                    self.bindings_resolver,
                    bindings_ref=activation.bindings_ref,
                    env_id=env_id,
                    domain=activation.domain,
                    strategy_id=activation.strategy_id,
                    op=op,
                    context={"payload": payload or {}, "activation_config": activation.config or {}},
                )
            except Exception as exc:
                raise KehrnelError(
                    code="BINDINGS_REF_RESOLUTION_FAILED",
                    status=502,
                    message=str(exc),
                    details={"bindings_ref": activation.bindings_ref, "env_id": env_id, "domain": activation.domain},
                )
        if not effective_bindings:
            raise KehrnelError(
                code="BINDINGS_UNAVAILABLE",
                status=500,
                message=(
                    "Bindings not available; activate with bindings_ref and configure a bindings resolver."
                ),
                details={
                    "env_id": env_id,
                    "domain": activation.domain,
                    "strategy_id": activation.strategy_id,
                    "activation_id": activation.activation_id,
                    "has_plaintext_bindings": bool(activation.bindings),
                    "bindings_ref": activation.bindings_ref,
                    "bindings_in_payload": False,
                },
            )

        cache = self._env_cache.setdefault(env_id, {}).setdefault("dict_cache", {})
        config_hash = activation.config_hash or self._config_hash(activation.config)
        manifest_digest = activation.manifest_digest or self._manifest_digest(manifest)
        meta = {
            "dict_cache": cache,
            "activation_id": activation.activation_id,
            "config_hash": config_hash,
            "manifest_digest": manifest_digest,
            "bundle_refs": activation.bundle_refs or {},
            "bundle_store": self.bundle_store,
            "store_profiles": activation.store_profiles or {},
        }
        if isinstance(payload, dict):
            if payload.get("__progress_cb"):
                meta["progress_cb"] = payload.get("__progress_cb")
            if payload.get("__should_cancel"):
                meta["should_cancel"] = payload.get("__should_cancel")
        ctx = StrategyContext(
            environment_id=env_id,
            config=activation.config,
            bindings=effective_bindings,
            adapters=self._build_adapters(env_id, effective_bindings),
            manifest=manifest,
            meta=meta,
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
                domain = payload.get("domain")
                query = payload.get("query")
                if query is None and isinstance(payload, dict) and isinstance(payload.get("aql"), str):
                    query = {
                        "aql": payload.get("aql"),
                        "scope": payload.get("scope"),
                        "debug": bool(payload.get("debug")),
                    }
                plan = await handle.plugin.compile_query(ctx, domain=domain, query=query)
                plan_dict = _to_dict(plan)
                # unwrap nested plan if present
                if isinstance(plan_dict, dict) and "plan" in plan_dict and isinstance(plan_dict["plan"], dict) and "pipeline" in plan_dict["plan"]:
                    inner = plan_dict["plan"]
                    inner.setdefault("engine", plan_dict.get("engine"))
                    inner.setdefault("explain", plan_dict.get("explain", inner.get("explain")))
                    plan_dict = inner
                explain = plan_dict.get("explain") or {}
                explain.setdefault("activation_id", activation.activation_id)
                explain.setdefault("strategy_id", activation.strategy_id)
                explain.setdefault("strategy_version", activation.version)
                explain.setdefault("domain", activation.domain)
                explain.setdefault("config_hash", self._config_hash(activation.config))
                explain.setdefault("manifest_digest", activation.manifest_digest or self._manifest_digest(manifest))
                explain.setdefault("engine", plan_dict.get("engine"))
                explain.setdefault("scope", plan_dict.get("scope") or explain.get("scope"))
                if allow_mismatch:
                    explain.setdefault("warnings", []).append("activation_manifest_mismatch_allowed")
                plan_dict["explain"] = explain
                return {"engine": plan_dict.get("engine") or getattr(plan, "engine", None), "plan": plan_dict}
            if op_lower == "execute_query":
                plan = payload.get("plan")
                return _to_dict(await handle.plugin.execute_query(ctx, plan))
            if op_lower == "query":
                domain = payload.get("domain")
                query = payload.get("query")
                if query is None and isinstance(payload, dict) and isinstance(payload.get("aql"), str):
                    query = {
                        "aql": payload.get("aql"),
                        "scope": payload.get("scope"),
                        "debug": bool(payload.get("debug")),
                    }
                plan = await handle.plugin.compile_query(ctx, domain=domain, query=query)
                res = await handle.plugin.execute_query(ctx, plan)
                return _to_dict(res)
            if op_lower in ("op", "extensions"):
                op_name = payload.get("op") if payload else None
                op_payload = payload.get("payload") if payload else {}
                if not op_name:
                    raise ValueError("op name required")
                # optional op input validation against manifest.ops[].input_schema
                op_def = next((o for o in manifest.ops or [] if o.name == op_name), None)
                if op_def and op_def.input_schema:
                    from kehrnel.core.config import validate_config as validate_op
                    validate_op(op_def.input_schema, op_payload or {})
                res = _to_dict(await handle.plugin.run_op(ctx, op_name, op_payload))
                if self._is_maintenance_op(manifest, op_name):
                    self.invalidate_env_cache(env_id, dict_only=True)
                return res
        except Exception as exc:
            raise
        raise ValueError(f"Operation {op} not supported")

    def _config_hash(self, config: Dict[str, Any]) -> str:
        try:
            blob = json.dumps(config or {}, sort_keys=True).encode("utf-8")
            return hashlib.sha256(blob).hexdigest()
        except Exception:
            return ""

    def _manifest_digest(self, manifest: StrategyManifest) -> str:
        try:
            payload = manifest.model_dump()
            blob = json.dumps(payload, sort_keys=True).encode("utf-8")
            return hashlib.sha256(blob).hexdigest()
        except Exception:
            return ""

    def _deep_merge(self, base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(base or {})
        for k, v in (overlay or {}).items():
            if isinstance(v, dict) and isinstance(result.get(k), dict):
                result[k] = self._deep_merge(result.get(k, {}), v)
            else:
                result[k] = v
        return result

    def _load_defaults_from_entrypoint(self, manifest: StrategyManifest) -> Dict[str, Any]:
        try:
            mod_path, _ = manifest.entrypoint.split(":")
            spec = importlib.util.find_spec(mod_path)
            if not spec or not spec.origin:
                return {}
            base = Path(spec.origin).parent
            defaults_path = base / "defaults.json"
            if defaults_path.exists():
                import json as _json

                return _json.loads(defaults_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return {}

    def _load_schema_from_entrypoint(self, manifest: StrategyManifest) -> Dict[str, Any]:
        try:
            mod_path, _ = manifest.entrypoint.split(":")
            spec = importlib.util.find_spec(mod_path)
            if not spec or not spec.origin:
                return {}
            base = Path(spec.origin).parent
            schema_path = base / "schema.json"
            if schema_path.exists():
                import json as _json

                return _json.loads(schema_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return {}

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

    def _validate_pack_config(self, manifest: StrategyManifest, config: Dict[str, Any]) -> None:
        """Validate encoding profile selections against pack_spec when present."""
        spec = getattr(manifest, "pack_spec", None)
        if not spec or not isinstance(spec, dict):
            return
        profiles = {p.get("id") for p in spec.get("encodingProfiles", []) if isinstance(p, dict) and p.get("id")}
        stores = (spec.get("storage") or {}).get("stores") or []

        def _get_from_path(obj: Dict[str, Any], path: str):
            parts = (path or "").split(".")
            current = obj
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return None
            return current

        errors = []
        for store in stores:
            if not isinstance(store, dict):
                continue
            ep_path = store.get("encodingProfileFromConfig")
            if not ep_path:
                continue
            val = _get_from_path(config or {}, ep_path)
            role = store.get("role") or store.get("destinationType")
            if val is None:
                errors.append(f"missing encodingProfile at {ep_path} for store {role}")
            elif profiles and val not in profiles:
                errors.append(f"encodingProfile '{val}' not declared in spec for store {role}")
        if errors:
            raise KehrnelError(code="PACK_CONFIG_INVALID", status=400, message="; ".join(errors), details={"errors": errors})

    def _build_store_profiles(self, manifest: StrategyManifest, config: Dict[str, Any]) -> Dict[str, Any]:
        """Materialize store roles with selected collection names and encoding profiles."""
        spec = getattr(manifest, "pack_spec", None)
        if not spec or not isinstance(spec, dict):
            return {}
        stores = (spec.get("storage") or {}).get("stores") or []

        def _get(obj: Dict[str, Any], path: str):
            parts = (path or "").split(".")
            cur = obj
            for part in parts:
                if isinstance(cur, dict) and part in cur:
                    cur = cur[part]
                else:
                    return None
            return cur

        profiles: Dict[str, Any] = {}
        for store in stores:
            if not isinstance(store, dict):
                continue
            role = store.get("role") or store.get("destinationType")
            col = _get(config or {}, store.get("collectionNameFromConfig") or "")
            enc = _get(config or {}, store.get("encodingProfileFromConfig") or "")
            atlas_cfg = store.get("atlasSearch") or {}
            atlas_idx = _get(config or {}, atlas_cfg.get("indexNameFromConfig") or "")
            profiles[role] = {
                "destinationType": store.get("destinationType"),
                "collection": col,
                "encodingProfile": enc,
                "atlasIndexName": atlas_idx,
                "indexes": store.get("indexes") or [],
            }
        return profiles

    def _validate_bundle_refs(self, config: Dict[str, Any], manifest: StrategyManifest) -> Dict[str, str]:
        refs: Dict[str, str] = {}
        if not self.bundle_store:
            return refs
        slim_cfg = (config.get("slim_search") or {}) if isinstance(config, dict) else {}
        bundle_id = slim_cfg.get("bundle_id")
        enabled = slim_cfg.get("enabled", False)
        if enabled and bundle_id:
            try:
                b = self.bundle_store.get_bundle(bundle_id)
                refs[bundle_id] = b.get("_digest") or compute_bundle_digest(b)
            except KehrnelError as exc:
                if exc.code == "BUNDLE_NOT_FOUND":
                    if self._seed_bundle_from_disk(bundle_id):
                        b = self.bundle_store.get_bundle(bundle_id)
                        refs[bundle_id] = b.get("_digest") or compute_bundle_digest(b)
                    else:
                        raise
                else:
                    raise
        return refs

    def _seed_bundle_from_disk(self, bundle_id: str) -> bool:
        bundles_root = Path(__file__).resolve().parents[1] / "strategies"
        candidate = bundles_root / "openehr" / "rps_dual" / "bundles" / f"{bundle_id}.json"
        if candidate.exists():
            try:
                data = json.loads(candidate.read_text(encoding="utf-8"))
                if self.bundle_store:
                    self.bundle_store.save_bundle(data, mode="upsert")
                    return True
            except Exception:
                return False
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

"""StrategyRuntime scaffold (to be implemented in later steps)."""
from __future__ import annotations

import asyncio
import importlib
import importlib.util
import json
import hashlib
import weakref
from copy import deepcopy
from datetime import datetime
from typing import Dict, Optional, Any
from dataclasses import is_dataclass, asdict, replace

from .manifest import StrategyManifest
from .manifest_digest import activation_version_compatible, compute_manifest_digest
from .config import validate_config
from .activation import EnvironmentActivation
from .environment import EnvironmentRecord
from .registry import ActivationRegistry, FileActivationRegistry
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.bundle_store import BundleStore
from kehrnel.engine.core.bundles import compute_bundle_digest
from kehrnel.strategy_sdk import StrategyHandle, StrategyBindings
from .types import QueryPlan, QueryResult, ApplyResult, TransformResult, StrategyContext
from .bindings_resolver import resolve_bindings as _resolve_bindings_ref
from .data_target import resolved_data_target_digest
from kehrnel.persistence.mongodb.connection import get_client as _mongo_get_client
from kehrnel.persistence.mongodb.connection import get_database as _mongo_get_db
from kehrnel.persistence.mongodb.storage import MongoStorageAdapter
from kehrnel.persistence.mongodb.index_admin import MongoIndexAdminAdapter
from kehrnel.persistence.mongodb.atlas_search import MongoAtlasSearchAdapter
from kehrnel.persistence.artifacts import AzureBlobArtifactStore, FileSystemArtifactStore, S3ArtifactStore
from kehrnel.persistence.validation import CommandValidationEngine
from kehrnel.persistence.embeddings import HttpEmbeddingAdapter
from kehrnel.persistence.llm import HttpLlmAdapter, HttpRerankAdapter
from kehrnel.engine.terminology import RuntimeTerminologyAdapter
from pathlib import Path


class StrategyRuntime:
    def __init__(self, registry: ActivationRegistry, bundle_store=None, bindings_resolver=None):
        self.registry = registry
        self.bundle_store = bundle_store
        self.bindings_resolver = bindings_resolver
        self.env_manifests: Dict[str, StrategyManifest] = {}
        # per-env cache: {"adapters_by_loop": {loop_key: {...}}, "dict_cache": {...}}
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

    @staticmethod
    def _bindings_payload(bindings: StrategyBindings | Dict[str, Any] | None) -> Dict[str, Any]:
        if not bindings:
            return {}
        if is_dataclass(bindings):
            raw = asdict(bindings)
        elif isinstance(bindings, dict):
            raw = dict(bindings)
        else:
            raw = getattr(bindings, "__dict__", {}) or {}
        return raw if isinstance(raw, dict) else {}

    def _bindings_hash(self, bindings: StrategyBindings | Dict[str, Any] | None, bindings_ref: str | None = None) -> str:
        payload = {
            "bindings_ref": (bindings_ref or "").strip() or None,
            "bindings": self._bindings_payload(bindings),
        }
        if not payload["bindings_ref"] and not payload["bindings"]:
            return ""
        try:
            blob = json.dumps(payload, sort_keys=True).encode("utf-8")
            return hashlib.sha256(blob).hexdigest()
        except Exception:
            return ""

    @staticmethod
    def _uses_mongodb(manifest: StrategyManifest | None) -> bool:
        storage = getattr(getattr(manifest, "adapters", None), "storage", []) or []
        return any(str(provider).strip().lower() in {"mongo", "mongodb"} for provider in storage)

    @classmethod
    def _strategy_database(cls, manifest: StrategyManifest | None, config: Dict[str, Any] | None) -> str | None:
        """Return the strategy-owned MongoDB database declared at activation.

        ``database`` is the canonical key. ``database_name`` remains accepted for
        older strategy packs while they migrate to the shared activation contract.
        """
        if not cls._uses_mongodb(manifest):
            return None
        raw = config or {}
        value = raw.get("database") or raw.get("database_name")
        if not isinstance(value, str) or not value.strip():
            raise KehrnelError(
                code="STRATEGY_DATABASE_REQUIRED",
                status=400,
                message="A MongoDB-backed strategy requires an explicit database in its activation configuration.",
                details={"strategy_id": getattr(manifest, "id", None), "field": "database"},
            )
        database = value.strip()
        if len(database.encode("utf-8")) > 63 or any(char in database for char in '/\\.\"$*<>:|?\0'):
            raise KehrnelError(
                code="INVALID_STRATEGY_DATABASE",
                status=400,
                message="The strategy database name is not valid for MongoDB.",
                details={"strategy_id": getattr(manifest, "id", None), "database": database},
            )
        return database

    @classmethod
    def _bind_strategy_database(
        cls,
        manifest: StrategyManifest | None,
        config: Dict[str, Any] | None,
        bindings: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        """Make the reviewed strategy database authoritative over URI/env defaults."""
        database = cls._strategy_database(manifest, config)
        resolved = dict(bindings or {})
        if database is None:
            return resolved
        current_db_binding = resolved.get("db")
        if not isinstance(current_db_binding, dict):
            return resolved
        db_binding = dict(current_db_binding)
        db_binding["database"] = database
        if not db_binding.get("provider"):
            db_binding["provider"] = "mongodb"
        resolved["db"] = db_binding
        return resolved

    @staticmethod
    def _acceptance_records(
        manifest: StrategyManifest,
        existing: Optional[EnvironmentActivation],
        incoming: list[Dict[str, Any]] | None,
        now: str,
    ) -> list[Dict[str, Any]]:
        """Resolve manifest activation conditions into recorded acceptances.

        Required attestations are runtime authority: a missing acceptance is a
        typed 422. Prior acceptances stay valid while their statement version
        matches the manifest; a bumped statement version re-requires acceptance.
        """
        activation_block = getattr(manifest, "activation", None)
        declared: Dict[str, Any] = {}
        required: list[Any] = []
        if activation_block is not None:
            for condition in activation_block.conditions or []:
                declared[condition.id] = condition
                if condition.required and condition.kind == "attestation":
                    required.append(condition)
        records: Dict[str, Dict[str, Any]] = {}
        if existing is not None:
            for record in getattr(existing, "condition_acceptances", None) or []:
                if not isinstance(record, dict):
                    continue
                condition = declared.get(record.get("id"))
                if condition is not None and record.get("statement_version") == condition.statement_version:
                    records[str(record["id"])] = dict(record)
        for item in incoming or []:
            if not isinstance(item, dict):
                raise KehrnelError(
                    code="INVALID_CONDITION_ACCEPTANCE",
                    status=400,
                    message="Condition acceptances must be objects with an id.",
                    details={"strategy_id": manifest.id},
                )
            condition_id = str(item.get("id") or "").strip()
            condition = declared.get(condition_id)
            if condition is None:
                raise KehrnelError(
                    code="INVALID_CONDITION_ACCEPTANCE",
                    status=400,
                    message=f"Condition '{condition_id}' is not declared by this strategy's manifest.",
                    details={"strategy_id": manifest.id, "condition_id": condition_id},
                )
            accepted = item.get("accepted", True)
            if not accepted:
                records.pop(condition_id, None)
                continue
            if condition.kind != "attestation":
                continue
            records[condition_id] = {
                "id": condition_id,
                "kind": condition.kind,
                "statement_version": condition.statement_version,
                "accepted": True,
                "accepted_by": item.get("accepted_by") or item.get("acceptedBy"),
                "accepted_at": now,
            }
        missing = [condition.id for condition in required if condition.id not in records]
        if missing:
            condition = next(item for item in required if item.id == missing[0])
            references = [
                reference.model_dump() if hasattr(reference, "model_dump") else dict(reference)
                for reference in (condition.references or [])
            ]
            raise KehrnelError(
                code=condition.code or "STRATEGY_ATTESTATION_REQUIRED",
                status=422,
                message=condition.message
                or f"Activation requires accepting the '{condition.title or condition.id}' attestation.",
                details={
                    "strategy_id": manifest.id,
                    "condition_id": condition.id,
                    "missing_condition_ids": missing,
                    "statement_version": condition.statement_version,
                    "references": references,
                },
            )
        return list(records.values())

    @staticmethod
    def _acceptances_equal(left: list[Dict[str, Any]], right: list[Dict[str, Any]]) -> bool:
        def key(records: list[Dict[str, Any]]) -> list[Any]:
            return sorted(
                (
                    record.get("id"),
                    record.get("statement_version"),
                    record.get("accepted"),
                    record.get("accepted_by"),
                    record.get("accepted_at"),
                )
                for record in records or []
            )

        return key(left) == key(right)

    async def _effective_bindings(
        self,
        env_id: str,
        activation: EnvironmentActivation,
        *,
        op: str,
        payload: Any = None,
    ) -> Optional[Dict[str, Any]]:
        """Resolve an activation's bindings (plaintext or via the configured resolver)."""
        effective_bindings = activation.bindings
        if not effective_bindings and activation.bindings_ref:
            if self.bindings_resolver is None:
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
        return effective_bindings

    @staticmethod
    def _seed_target_collection(manifest: StrategyManifest, config: Dict[str, Any], declared_name: str) -> str:
        """Map a manifest-declared seed collection to its configured tenant name."""
        spec = getattr(manifest, "pack_spec", None)
        if not isinstance(spec, dict):
            return declared_name
        defaults = getattr(manifest, "default_config", None) or {}

        def _get(obj: Dict[str, Any], path: str):
            current: Any = obj
            for part in (path or "").split("."):
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return None
            return current

        for store in (spec.get("storage") or {}).get("stores") or []:
            if not isinstance(store, dict):
                continue
            path = store.get("collectionNameFromConfig")
            if not path:
                continue
            if _get(defaults, path) == declared_name:
                configured = _get(config or {}, path)
                if isinstance(configured, str) and configured.strip():
                    return configured.strip()
        return declared_name

    @staticmethod
    async def _copy_seed_collection(
        source_db: Any,
        target_db: Any,
        source_name: str,
        target_name: str,
        *,
        replace: bool = False,
        batch_size: int = 2000,
    ) -> tuple[str, int]:
        """Copy one seed collection and return (copy_path, copied_count).

        Prefers a server-side ``$out`` copy (no document round-trips, the right
        shape for a SaaS activation of a ~150k-document demo subset) and falls
        back to a bounded client-side batched copy when the deployment forbids
        it (e.g. restricted roles). Indexes are created afterwards by the
        activation's plan/apply step.
        """
        if source_db.name == target_db.name and source_name == target_name:
            raise KehrnelError(
                code="SEED_SOURCE_EQUALS_TARGET",
                status=400,
                message=(
                    "The seed source and target resolve to the same collection; refusing "
                    "to copy a collection onto itself."
                ),
                details={"database": source_db.name, "collection": source_name},
            )
        target = target_db[target_name]
        if not replace:
            try:
                await source_db[source_name].aggregate(
                    [{"$match": {}}, {"$out": {"db": target_db.name, "coll": target_name}}],
                    allowDiskUse=True,
                ).to_list(length=None)
                return "server_side", int(await target.estimated_document_count())
            except Exception:
                # Fall through to the portable batched copy.
                pass
        if replace:
            await target.delete_many({})
        copied = 0
        batch: list[Dict[str, Any]] = []
        async for document in source_db[source_name].find({}):
            batch.append(document)
            if len(batch) >= batch_size:
                await target.insert_many(batch, ordered=False)
                copied += len(batch)
                batch = []
        if batch:
            await target.insert_many(batch, ordered=False)
            copied += len(batch)
        return "batched", copied

    async def provision_activation_seed(
        self,
        env_id: str,
        activation: EnvironmentActivation,
        *,
        force: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Copy manifest-declared seed collections into the activation-owned database.

        Sample-class content only: the seed source is a redistributable demo
        subset declared in the pack manifest. Runs only after every required
        attestation is recorded on the activation.
        """
        manifest = self.registry.get_manifest(activation.strategy_id)
        if manifest is None:
            raise ValueError(f"Strategy {activation.strategy_id} not registered")
        activation_block = getattr(manifest, "activation", None)
        seed = getattr(activation_block, "seed", None)
        if seed is None:
            return None
        declared = {
            condition.id: condition for condition in (activation_block.conditions or [])
        }
        recorded = {
            str(record.get("id"))
            for record in (activation.condition_acceptances or [])
            if isinstance(record, dict)
        }
        missing = [cid for cid in activation_block.required_attestation_ids if cid not in recorded]
        if missing:
            condition = declared.get(missing[0])
            references = [
                reference.model_dump() if hasattr(reference, "model_dump") else dict(reference)
                for reference in (getattr(condition, "references", None) or [])
            ]
            raise KehrnelError(
                code=getattr(condition, "code", None) or "STRATEGY_ATTESTATION_REQUIRED",
                status=422,
                message=(
                    "Seed provisioning requires an accepted license attestation. "
                    + (getattr(condition, "message", None) or "")
                ).strip(),
                details={
                    "strategy_id": manifest.id,
                    "condition_id": missing[0],
                    "missing_condition_ids": missing,
                    "references": references,
                },
            )

        effective_bindings = await self._effective_bindings(
            env_id, activation, op="provision_seed"
        )
        effective_bindings = self._bind_strategy_database(manifest, activation.config, effective_bindings)
        db_cfg = (effective_bindings or {}).get("db") or {}
        if db_cfg.get("provider") != "mongodb":
            raise KehrnelError(
                code="SEED_BINDING_REQUIRED",
                status=400,
                message="Seed provisioning requires a MongoDB storage binding.",
                details={"strategy_id": manifest.id},
            )
        target_database = self._strategy_database(manifest, activation.config)
        source_database = str(seed.source_database or "").strip()
        if not source_database:
            raise KehrnelError(
                code="SEED_SOURCE_MISSING",
                status=400,
                message="The manifest's activation.seed does not declare a source_database.",
                details={"strategy_id": manifest.id},
            )
        client = _mongo_get_client(effective_bindings)
        source_db = client[source_database]
        target_db = client[target_database]
        results: list[Dict[str, Any]] = []
        started_at = datetime.utcnow().isoformat()
        for entry in seed.collections or []:
            target_name = self._seed_target_collection(manifest, activation.config, entry.name)
            source_name = entry.source or entry.name
            if entry.mode == "empty":
                if target_name not in await target_db.list_collection_names():
                    await target_db.create_collection(target_name)
                results.append(
                    {
                        "name": entry.name,
                        "collection": target_name,
                        "mode": "empty",
                        "copied": 0,
                        "skipped": True,
                    }
                )
                continue
            target = target_db[target_name]
            existing_count = await target.estimated_document_count()
            if existing_count and not force:
                results.append(
                    {
                        "name": entry.name,
                        "collection": target_name,
                        "mode": "copy",
                        "copied": 0,
                        "existing": existing_count,
                        "skipped": True,
                        "reason": "target_not_empty",
                    }
                )
                continue
            copy_mode, copied = await self._copy_seed_collection(
                source_db,
                target_db,
                source_name,
                target_name,
                replace=bool(force and existing_count),
            )
            entry_result: Dict[str, Any] = {
                "name": entry.name,
                "collection": target_name,
                "mode": "copy",
                "copy_path": copy_mode,
                "copied": copied,
                "skipped": False,
            }
            if entry.expected_documents is not None and copied != entry.expected_documents:
                entry_result["warning"] = (
                    f"expected {entry.expected_documents} documents but copied {copied}"
                )
            results.append(entry_result)
        observed_digest = "sha256:" + hashlib.sha256(
            "|".join(
                f"{entry['collection']}:{entry['copied']}" for entry in sorted(results, key=lambda item: item["collection"])
            ).encode("utf-8")
        ).hexdigest()
        warnings = [
            f"{entry['collection']}: {entry['warning']}"
            for entry in results
            if entry.get("warning")
        ]
        if seed.digest and seed.digest != observed_digest:
            warnings.append(
                f"seed digest mismatch: manifest declares {seed.digest} but observed {observed_digest}"
            )
        result = {
            "ok": not warnings,
            "kind": seed.kind,
            "source_database": source_database,
            "database": target_database,
            "license_class": seed.license_class,
            "scope": seed.scope,
            "manifest_digest": seed.digest,
            "observed_digest": observed_digest,
            "collections": results,
            "warnings": warnings,
            "started_at": started_at,
            "completed_at": datetime.utcnow().isoformat(),
        }
        self.registry.update_activation_fields(env_id, activation.domain, seed=result)
        return result

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
        config_hash_override: str | None = None,
        force: bool = False,
        replace_reason: str | None = None,
        bindings_ref: str | None = None,
        condition_acceptances: list[Dict[str, Any]] | None = None,
    ):
        manifest = self.registry.get_manifest(strategy_id)
        if not manifest:
            raise ValueError(f"Strategy {strategy_id} not registered")
        domain = domain or getattr(manifest, "domain", None)
        chosen_domain = domain
        if not chosen_domain:
            raise ValueError("Domain required for activation")
        chosen_domain = str(chosen_domain).strip().lower()
        env_record = self.registry.get_environment(env_id)
        if env_record and not bindings_ref and getattr(env_record, "bindings_ref", None):
            bindings_ref = env_record.bindings_ref
        existing = self.registry.get_activation(env_id, chosen_domain)
        accepted_at = datetime.utcnow().isoformat()
        acceptances = self._acceptance_records(manifest, existing, condition_acceptances, accepted_at)
        incoming_bindings = self._has_meaningful_bindings(bindings)
        if incoming_bindings and not allow_plaintext_bindings:
            raise KehrnelError(
                code="PLAINTEXT_BINDINGS_FORBIDDEN",
                status=400,
                message="Plaintext bindings are not allowed. Use bindings_ref with a configured resolver.",
            )
        if not bindings_ref and not allow_plaintext_bindings:
            raise KehrnelError(
                code="BINDINGS_REF_REQUIRED",
                status=400,
                message="bindings_ref is required for activation.",
            )
        existing_hash = existing.config_hash if existing else None
        current_manifest_digest = manifest_digest_override or self._manifest_digest(manifest)
        # A manifest default is a UI/CLI suggestion, not approval to place data
        # in that database. Mongo-backed activations must carry the reviewed
        # database explicitly in their submitted configuration.
        strategy_database = self._strategy_database(manifest, config)
        # idempotent: if same strategy and config hash matches, return existing
        restore_snapshot = replace_reason == "rollback" and bool(config)
        if restore_snapshot:
            merged_config = dict(config or {})
        else:
            merged_config = {}
            defaults = manifest.default_config or self._load_defaults_from_entrypoint(manifest)
            if defaults:
                merged_config = self._deep_merge(merged_config, defaults)
            merged_config = self._deep_merge(merged_config, config or {})
        # simple required validation from schema if present
        schema = getattr(manifest, "config_schema", None) or self._load_schema_from_entrypoint(manifest) or {}
        if strategy_database:
            schema_properties = schema.get("properties") if isinstance(schema, dict) else {}
            database_field = "database" if "database" in (schema_properties or {}) else "database_name"
            merged_config[database_field] = strategy_database
        validate_config(schema, merged_config)
        self._validate_pack_config(manifest, merged_config)
        store_profiles = self._build_store_profiles(manifest, merged_config)
        bundle_refs = self._validate_bundle_refs(merged_config, manifest)
        new_config_hash = config_hash_override or self._config_hash(merged_config)
        bdict = self._bind_strategy_database(manifest, merged_config, self._bindings_payload(bindings))
        bindings = StrategyBindings(**bdict)
        uri = (bdict.get("db") or {}).get("uri")
        redacted_uri = self._redact_uri(uri) if uri else None
        bindings_meta = {
            "db": {
                "provider": (bdict.get("db") or {}).get("provider") or ("mongodb" if strategy_database else None),
                "database": strategy_database or (bdict.get("db") or {}).get("database"),
                "uri": redacted_uri,
            }
        }
        new_bindings_hash = self._bindings_hash(bindings, bindings_ref)
        if existing and not force:
            existing_bindings_hash = (getattr(existing, "bindings_hash", None) or "").strip()
            if existing_bindings_hash and new_bindings_hash:
                bindings_match = existing_bindings_hash == new_bindings_hash
            else:
                bindings_match = (
                    (getattr(existing, "bindings_ref", None) or None) == ((bindings_ref or "").strip() or None)
                    and (getattr(existing, "bindings_meta", None) or {}) == bindings_meta
                )
            digest_matches = bool(existing.manifest_digest and existing.manifest_digest == current_manifest_digest)
            if (
                existing.strategy_id == strategy_id
                and existing_hash == new_config_hash
                and bindings_match
                and activation_version_compatible(existing, manifest)
            ):
                if not self._acceptances_equal(acceptances, existing.condition_acceptances or []):
                    existing = self.registry.update_activation_fields(
                        env_id,
                        chosen_domain,
                        condition_acceptances=acceptances,
                    ) or existing
                if digest_matches:
                    return replace(
                        existing,
                        already_active=True,
                        replaced=False,
                        previous_activation_id=None,
                        replaced_from=None,
                    )
                synced = self._sync_activation_digest(existing, current_manifest_digest)
                return replace(
                    synced,
                    already_active=True,
                    replaced=False,
                    previous_activation_id=None,
                    replaced_from=None,
                )
            # switch strategy or config changed: replace
        # invalidate cache for this env
        self.invalidate_env_cache(env_id)
        ctx = StrategyContext(environment_id=env_id, config=merged_config, bindings=None, adapters=None, manifest=manifest)
        # load plugin
        mod_path, cls_name = manifest.entrypoint.split(":")
        mod = importlib.import_module(mod_path)
        plugin_cls = getattr(mod, cls_name)
        handle = StrategyHandle(manifest, plugin_cls(manifest))
        await _maybe_await(handle.activate, config=merged_config, bindings=bindings, context=ctx)
        now = datetime.utcnow().isoformat()
        activation = EnvironmentActivation(
            env_id=env_id,
            domain=chosen_domain,
            strategy_id=strategy_id,
            version=version,
            manifest_digest=current_manifest_digest,
            config=merged_config,
            bindings_meta=bindings_meta,
            activation_id=None,
            config_hash=new_config_hash,
            bindings_hash=new_bindings_hash or None,
            activated_at=now,
            updated_at=now,
            bindings=(bdict if allow_plaintext_bindings else None),
            bindings_ref=bindings_ref,
            replaced=bool(existing),
            previous_activation_id=getattr(existing, "activation_id", None) if existing else None,
            replaced_from={"activation_id": existing.activation_id, "strategy_id": existing.strategy_id, "version": existing.version} if existing else None,
            bundle_refs=bundle_refs,
            store_profiles=store_profiles,
            condition_acceptances=acceptances,
            seed=getattr(existing, "seed", None) if existing else None,
        )
        hist_reason = replace_reason or (reason if existing else reason) or ("replace" if existing else "activate")
        if existing and not hist_reason:
            hist_reason = "replace"
        self.registry.activate(activation, reason=hist_reason or "activate")
        if bindings_ref and (not env_record or getattr(env_record, "bindings_ref", None) != bindings_ref):
            self.registry.upsert_environment(
                EnvironmentRecord(
                    env_id=env_id,
                    name=getattr(env_record, "name", None) if env_record else None,
                    description=getattr(env_record, "description", None) if env_record else None,
                    metadata=dict(getattr(env_record, "metadata", {}) or {}) if env_record else {},
                    bindings_ref=bindings_ref,
                    created_at=getattr(env_record, "created_at", None) if env_record else None,
                    updated_at=now,
                )
            )
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
            allow_plaintext_bindings=bool(activation.bindings) and not bool(activation.bindings_ref),
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
            allow_plaintext_bindings=bool(prev_activation.bindings) and not bool(prev_activation.bindings_ref),
            domain=prev_activation.domain or domain,
            reason="rollback",
            manifest_digest_override=prev_activation.manifest_digest,
            config_hash_override=prev_activation.config_hash,
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

    def list_environments(self) -> Dict[str, EnvironmentRecord]:
        return self.registry.list_environments()

    def get_environment(self, env_id: str) -> Optional[EnvironmentRecord]:
        return self.registry.get_environment(env_id)

    def upsert_environment(
        self,
        env_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        metadata: dict | None = None,
        bindings_ref: str | None = None,
    ) -> EnvironmentRecord:
        existing = self.registry.get_environment(env_id)
        return self.registry.upsert_environment(
            EnvironmentRecord(
                env_id=env_id,
                name=name if name is not None else getattr(existing, "name", None),
                description=description if description is not None else getattr(existing, "description", None),
                metadata=dict(metadata or {}) if metadata is not None else dict(getattr(existing, "metadata", {}) or {}),
                bindings_ref=bindings_ref if bindings_ref is not None else getattr(existing, "bindings_ref", None),
                created_at=getattr(existing, "created_at", None),
                updated_at=datetime.utcnow().isoformat(),
            )
        )

    def delete_environment(self, env_id: str, remove_activations: bool = False) -> EnvironmentRecord:
        try:
            removed = self.registry.delete_environment(env_id, remove_activations=remove_activations)
        except ValueError as exc:
            raise KehrnelError(code="ENVIRONMENT_NOT_EMPTY", status=409, message=str(exc)) from exc
        if not removed:
            raise KehrnelError(code="ENVIRONMENT_NOT_FOUND", status=404, message=f"Environment {env_id} not found")
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
            # An explicit domain is a routing boundary. Never fall back to the
            # environment's sole activation: that could execute a FHIR request
            # through openEHR (or vice versa) when only the other domain exists.
            activation = self.registry.get_activation(env_id, requested_domain)
        else:
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
        # Prefer manifest_digest for compatibility checks; versions are user labels and may drift.
        # Only enforce version mismatch when no digest is available (legacy activations).
        version_mismatch = bool(
            not activation.manifest_digest
            and activation_version
            and not is_latest_alias
            and activation_version != manifest.version
        )
        if digest_mismatch or version_mismatch:
            if not allow_mismatch and activation_version_compatible(activation, manifest):
                activation = self._sync_activation_digest(activation, current_digest)
            else:
                details = {
                    "expected_version": activation.version,
                    "actual_version": manifest.version,
                    "expected_digest": activation.manifest_digest,
                    "actual_digest": current_digest,
                }
                if not allow_mismatch:
                    raise KehrnelError(
                        code="ACTIVATION_STRATEGY_MISMATCH",
                        status=409,
                        message="Active strategy differs from current manifest",
                        details=details,
                    )

        # Server-side orchestration may pin a request to the activation it
        # authorized before performing slower work (for example terminology
        # expansion).  Re-check that snapshot at the dispatch boundary so an
        # activation switch cannot execute the reviewed plan against a
        # different strategy configuration or binding.
        activation_precondition = (
            payload.get("__activation_precondition")
            if isinstance(payload, dict)
            else None
        )
        if activation_precondition is not None:
            if not isinstance(activation_precondition, dict):
                raise KehrnelError(
                    code="ACTIVATION_PRECONDITION_INVALID",
                    status=400,
                    message="The activation precondition must be an object.",
                )
            actual_activation = {
                "activationId": activation.activation_id,
                "strategyId": activation.strategy_id,
                "configHash": activation.config_hash
                or self._config_hash(activation.config),
                "manifestDigest": activation.manifest_digest
                or current_digest,
                "bindingsHash": activation.bindings_hash,
            }
            mismatches = {
                field: {
                    "expected": expected,
                    "actual": actual_activation.get(field),
                }
                for field, expected in activation_precondition.items()
                if field in actual_activation
                and expected is not None
                and actual_activation.get(field) != expected
            }
            if mismatches:
                raise KehrnelError(
                    code="ACTIVATION_PRECONDITION_FAILED",
                    status=409,
                    message=(
                        "The active strategy changed after the request was "
                        "authorized; create a new plan against the current activation."
                    ),
                    details={
                        "env_id": env_id,
                        "domain": requested_domain or activation.domain,
                        "mismatches": mismatches,
                    },
                )

        bindings_payload = (payload or {}).get("bindings") if isinstance(payload, dict) else None
        if bindings_payload:
            raise KehrnelError(
                code="PLAINTEXT_BINDINGS_FORBIDDEN",
                status=400,
                message="Per-request plaintext bindings are not allowed. Use bindings_ref activation.",
            )
        effective_bindings = await self._effective_bindings(
            env_id,
            activation,
            op=op,
            payload=payload,
        )

        # The environment binding owns connectivity and credentials. The
        # activation owns the logical strategy database. Never allow a database
        # embedded in the URI, environment metadata, or a generic resolver to
        # redirect strategy collections into the tenant core database.
        effective_bindings = self._bind_strategy_database(manifest, activation.config, effective_bindings)
        resolved_target_digest = resolved_data_target_digest(effective_bindings)
        expected_target_digest = (
            activation_precondition.get("dataTargetDigest")
            if isinstance(activation_precondition, dict)
            else None
        )
        if (
            expected_target_digest is not None
            and resolved_target_digest != expected_target_digest
        ):
            raise KehrnelError(
                code="DATA_TARGET_PRECONDITION_FAILED",
                status=409,
                message=(
                    "The resolved persistence target changed after the request "
                    "was authorized; retry against the current target."
                ),
                details={
                    "env_id": env_id,
                    "domain": requested_domain or activation.domain,
                    "expectedDataTargetDigest": expected_target_digest,
                    "actualDataTargetDigest": resolved_target_digest,
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
            "data_target_digest": resolved_target_digest,
        }
        if isinstance(payload, dict):
            if payload.get("__job_id"):
                meta["job_id"] = str(payload.get("__job_id"))
            if payload.get("__progress_cb"):
                meta["progress_cb"] = payload.get("__progress_cb")
            if payload.get("__should_cancel"):
                meta["should_cancel"] = payload.get("__should_cancel")
            # Privileged diagnostics are enabled ONLY via this server-set internal
            # marker (the HTTP boundary sets it after a server-side policy check);
            # never from a client-facing payload field.
            if payload.get("__privileged"):
                meta["privileged"] = True
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
            if op_lower == "authoritative_write":
                return _to_dict(
                    await handle.plugin.execute_authoritative_write(
                        ctx,
                        payload or {},
                    )
                )
            if op_lower == "compile_query":
                domain = payload.get("domain")
                query = payload.get("query")
                if isinstance(query, dict) and isinstance(query.get("aql"), str):
                    query = {
                        "raw_aql": query.get("aql"),
                        "params": query.get("parameters") or query.get("params") or {},
                        "debug": bool(query.get("debug") or payload.get("debug")),
                    }
                if query is None and isinstance(payload, dict) and isinstance(payload.get("aql"), str):
                    # Accept raw AQL payloads at the runtime layer.
                    # openEHR strategies may choose to compile these through a domain-specific
                    # compatibility path rather than the normalized IR compiler.
                    aql_text = payload.get("aql")

                    # Extra guard: the current handwritten validator is permissive enough to accept
                    # inputs like "SELECT FROM". Reject obviously malformed queries early.
                    import re
                    if not isinstance(aql_text, str) or not re.search(r"\bselect\b\s+\S[\s\S]*\bfrom\b", aql_text, flags=re.IGNORECASE):
                        raise KehrnelError(
                            code="INVALID_AQL",
                            status=400,
                            message="AQL syntax error: query must include SELECT <expr> FROM ...",
                            details={"aql": aql_text},
                        )
                    if (domain or "").lower() not in ("openehr", "open_ehr"):
                        raise KehrnelError(
                            code="RAW_AQL_UNSUPPORTED",
                            status=400,
                            message="Raw AQL is only supported for the openEHR domain",
                            details={"domain": domain},
                        )
                    query = {
                        "raw_aql": aql_text,
                        "params": payload.get("parameters") or payload.get("params") or {},
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
                if isinstance(query, dict) and isinstance(query.get("aql"), str):
                    query = {
                        "raw_aql": query.get("aql"),
                        "params": query.get("parameters") or query.get("params") or {},
                        "debug": bool(query.get("debug") or payload.get("debug")),
                    }
                if query is None and isinstance(payload, dict) and isinstance(payload.get("aql"), str):
                    # Accept raw AQL payloads and defer domain-specific compilation to the strategy.
                    aql_text = payload.get("aql")
                    if not isinstance(aql_text, str) or not aql_text.strip():
                        raise KehrnelError(code="INVALID_AQL", status=400, message="aql must be a non-empty string")
                    if (domain or "").lower() not in ("openehr", "open_ehr"):
                        raise KehrnelError(
                            code="RAW_AQL_UNSUPPORTED",
                            status=400,
                            message="Raw AQL is only supported for the openEHR domain",
                            details={"domain": domain},
                        )
                    query = {
                        "raw_aql": aql_text,
                        "params": payload.get("parameters") or payload.get("params") or {},
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
                    from kehrnel.engine.core.config import validate_config as validate_op
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
        return compute_manifest_digest(manifest)

    def _sync_activation_digest(self, activation: EnvironmentActivation, manifest_digest: str) -> EnvironmentActivation:
        now = datetime.utcnow().isoformat()
        synced = self.registry.update_activation_fields(
            activation.env_id,
            activation.domain,
            manifest_digest=manifest_digest,
            updated_at=now,
        )
        return synced or replace(activation, manifest_digest=manifest_digest, updated_at=now)

    def _deep_merge(self, base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        # Activation must never hand a plugin references into the manifest's
        # shared default_config. Several strategies enrich nested defaults at
        # runtime; a shallow copy made a second identical activation hash a
        # different configuration.
        result = deepcopy(base or {})
        for k, v in (overlay or {}).items():
            if isinstance(v, dict) and isinstance(result.get(k), dict):
                result[k] = self._deep_merge(result.get(k, {}), v)
            else:
                result[k] = deepcopy(v)
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
        # Motor binds a client to the event loop that first performs an operation on it
        # and routes every later operation through that loop's executor. Synthetic jobs
        # dispatch on a throwaway worker loop, so a client shared with the API loop (or
        # with a previous job's loop) raises "got Future attached to a different loop".
        # Cache one adapter set per loop instead, and drop sets whose loop is gone.
        adapters_by_loop: Dict[Any, Dict[str, Any]] = env_cache.setdefault("adapters_by_loop", {})
        self._prune_dead_loop_adapters(adapters_by_loop)
        loop_key = self._current_loop_key()
        binding_key = self._bindings_hash(bindings)
        cache_key = (loop_key, binding_key)
        cached = adapters_by_loop.get(cache_key)
        if cached is not None:
            return cached["adapters"]
        adapters: Dict[str, Any] = {
            "terminology": RuntimeTerminologyAdapter(self, env_id),
        }
        db_cfg = (bindings or {}).get("db") if isinstance(bindings, dict) else None
        if db_cfg and db_cfg.get("provider") == "mongodb":
            db = _mongo_get_db(bindings)
            adapters["storage"] = MongoStorageAdapter(db)
            adapters["index_admin"] = MongoIndexAdminAdapter(db)
            adapters["atlas_search"] = MongoAtlasSearchAdapter(db)
            # backward-compatible alias (deprecated)
            adapters["text_search"] = adapters["atlas_search"]
        artifact_cfg = (bindings or {}).get("artifact") if isinstance(bindings, dict) else None
        if not artifact_cfg and isinstance(bindings, dict):
            extras = bindings.get("extras")
            artifact_cfg = extras.get("artifact") if isinstance(extras, dict) else None
        if artifact_cfg and artifact_cfg.get("provider") == "filesystem":
            root = str(artifact_cfg.get("root") or "").strip()
            if not root:
                raise KehrnelError(
                    code="INVALID_ARTIFACT_BINDING",
                    status=400,
                    message="Filesystem artifact bindings require artifact.root.",
                )
            adapters["artifact_store"] = FileSystemArtifactStore(root)
        elif artifact_cfg and artifact_cfg.get("provider") == "s3":
            client_options = {
                target: artifact_cfg[source]
                for source, target in (
                    ("access_key_id", "aws_access_key_id"),
                    ("secret_access_key", "aws_secret_access_key"),
                    ("session_token", "aws_session_token"),
                )
                if artifact_cfg.get(source)
            }
            adapters["artifact_store"] = S3ArtifactStore(
                str(artifact_cfg.get("bucket") or ""),
                prefix=str(artifact_cfg.get("prefix") or ""),
                region=artifact_cfg.get("region"),
                endpoint_url=artifact_cfg.get("endpoint_url"),
                server_side_encryption=artifact_cfg.get("server_side_encryption"),
                kms_key_id=artifact_cfg.get("kms_key_id"),
                client_options=client_options,
            )
        elif artifact_cfg and artifact_cfg.get("provider") in {"azure", "azure-blob"}:
            adapters["artifact_store"] = AzureBlobArtifactStore(
                str(artifact_cfg.get("container") or ""),
                prefix=str(artifact_cfg.get("prefix") or ""),
                connection_string=artifact_cfg.get("connection_string"),
            )
        validation_cfg = (bindings or {}).get("validation") if isinstance(bindings, dict) else None
        if not validation_cfg and isinstance(bindings, dict):
            extras = bindings.get("extras")
            validation_cfg = extras.get("validation") if isinstance(extras, dict) else None
        if validation_cfg and validation_cfg.get("provider") == "command":
            adapters["validation_engine"] = CommandValidationEngine(
                list(validation_cfg.get("argv") or []),
                timeout_seconds=int(validation_cfg.get("timeout_seconds") or 900),
                max_output_bytes=int(validation_cfg.get("max_output_bytes") or 20_000_000),
                environment=validation_cfg.get("environment") or {},
            )
        embedding_cfg = (bindings or {}).get("embedding") if isinstance(bindings, dict) else None
        if not embedding_cfg and isinstance(bindings, dict):
            extras = bindings.get("extras")
            embedding_cfg = extras.get("embedding") if isinstance(extras, dict) else None
        if embedding_cfg and embedding_cfg.get("provider") == "http":
            adapters["embedding"] = HttpEmbeddingAdapter(
                str(embedding_cfg.get("endpoint") or ""),
                model=embedding_cfg.get("model"),
                api_key=embedding_cfg.get("api_key"),
                timeout_seconds=int(embedding_cfg.get("timeout_seconds") or 60),
                headers=embedding_cfg.get("headers") or {},
            )
        llm_cfg = (bindings or {}).get("llm") if isinstance(bindings, dict) else None
        if not llm_cfg and isinstance(bindings, dict):
            extras = bindings.get("extras")
            llm_cfg = extras.get("llm") if isinstance(extras, dict) else None
        if llm_cfg and llm_cfg.get("provider") == "http" and llm_cfg.get("endpoint"):
            adapters["llm"] = HttpLlmAdapter(
                str(llm_cfg.get("endpoint")),
                model=str(llm_cfg.get("model") or "gpt-5.5"),
                api_key=llm_cfg.get("api_key"),
                timeout_seconds=int(llm_cfg.get("timeout_seconds") or 60),
                max_tokens=int(llm_cfg.get("max_tokens") or 4096),
                headers=llm_cfg.get("headers") or {},
            )
        rerank_cfg = (bindings or {}).get("rerank") if isinstance(bindings, dict) else None
        if not rerank_cfg and isinstance(bindings, dict):
            extras = bindings.get("extras")
            rerank_cfg = extras.get("rerank") if isinstance(extras, dict) else None
        if rerank_cfg and rerank_cfg.get("provider") == "http" and rerank_cfg.get("endpoint"):
            adapters["rerank"] = HttpRerankAdapter(
                str(rerank_cfg.get("endpoint")),
                model=str(rerank_cfg.get("model") or "rerank-2.5"),
                api_key=rerank_cfg.get("api_key"),
                timeout_seconds=int(rerank_cfg.get("timeout_seconds") or 15),
                headers=rerank_cfg.get("headers") or {},
            )
        loop = self._current_loop()
        adapters_by_loop[cache_key] = {
            "adapters": adapters,
            "loop": weakref.ref(loop) if loop is not None else None,
        }
        return adapters

    @staticmethod
    def _current_loop() -> Optional[asyncio.AbstractEventLoop]:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return None

    def _current_loop_key(self) -> Any:
        loop = self._current_loop()
        # No running loop means a synchronous caller; keep those in their own bucket.
        return id(loop) if loop is not None else "__no_loop__"

    def _prune_dead_loop_adapters(self, adapters_by_loop: Dict[Any, Dict[str, Any]]) -> None:
        for key in list(adapters_by_loop):
            entry = adapters_by_loop.get(key) or {}
            loop_ref = entry.get("loop")
            if loop_ref is None:
                continue
            loop = loop_ref() if loop_ref is not None else None
            if loop is not None and not loop.is_closed():
                continue
            self._close_adapters(adapters_by_loop.pop(key, {}).get("adapters") or {})

    @staticmethod
    def _close_adapters(adapters: Dict[str, Any]) -> None:
        seen: set[int] = set()
        for adapter in (adapters or {}).values():
            # Motor Database objects reject truth testing, so compare with None.
            db = getattr(adapter, "db", None)
            if db is None:
                db = getattr(adapter, "_db", None)
            client = getattr(db, "client", None) if db is not None else None
            if client is None or id(client) in seen:
                continue
            seen.add(id(client))
            try:
                client.close()
            except Exception:
                pass

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
        # Only drop the reference here: an in-flight dispatch may still hold these
        # adapters. Clients for dead loops are closed by _prune_dead_loop_adapters.
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

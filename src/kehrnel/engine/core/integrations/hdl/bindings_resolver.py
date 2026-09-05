"""Resolve Kehrnel bindings from Healthcare Data Lab encrypted environment secrets.

Set:
  KEHRNEL_BINDINGS_RESOLVER=kehrnel.engine.core.integrations.hdl.bindings_resolver:resolve_hdl_bindings

Required env vars:
  ENV_SECRETS_KEY           base64-encoded 32-byte key (AES-256-GCM)
  CORE_MONGODB_URL          HDL core DB URI
  CORE_DATABASE_NAME        HDL core DB name
"""
from __future__ import annotations

import base64
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote, urlsplit

import certifi
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pymongo import MongoClient

from kehrnel.persistence.mongodb.tls import mongo_tls_enabled


@dataclass
class _CoreStore:
    client: MongoClient
    db_name: str

    @property
    def db(self):
        return self.client[self.db_name]


_STORE: Optional[_CoreStore] = None


def _artifact_binding(env_id: str) -> Optional[dict[str, Any]]:
    """Resolve an optional, environment-scoped artifact store for HDL.

    MongoDB connection secrets and artifact-store credentials have different
    lifecycles.  HDL's encrypted environment document currently owns only the
    MongoDB URI, so deployments configure the shared artifact service through
    Kehrnel environment variables.  Every returned root/prefix is scoped by the
    environment id to prevent cross-environment object reuse.
    """

    provider = (os.getenv("KEHRNEL_HDL_ARTIFACT_PROVIDER") or "").strip().lower()
    if not provider:
        return None
    safe_env_id = re.sub(r"[^A-Za-z0-9._-]+", "-", env_id).strip("-.")
    if not safe_env_id:
        raise ValueError("env_id cannot be converted to a safe artifact namespace")

    if provider == "filesystem":
        root = (os.getenv("KEHRNEL_HDL_ARTIFACT_ROOT") or "").strip()
        if not root:
            raise ValueError("KEHRNEL_HDL_ARTIFACT_ROOT is required for filesystem artifacts")
        return {"provider": "filesystem", "root": str(Path(root).expanduser() / safe_env_id)}

    if provider == "s3":
        bucket = (os.getenv("KEHRNEL_HDL_ARTIFACT_S3_BUCKET") or "").strip()
        if not bucket:
            raise ValueError("KEHRNEL_HDL_ARTIFACT_S3_BUCKET is required for S3 artifacts")
        base_prefix = (os.getenv("KEHRNEL_HDL_ARTIFACT_S3_PREFIX") or "").strip("/")
        binding: dict[str, Any] = {
            "provider": "s3",
            "bucket": bucket,
            "prefix": "/".join(part for part in (base_prefix, safe_env_id) if part),
        }
        optional = {
            "region": "KEHRNEL_HDL_ARTIFACT_S3_REGION",
            "endpoint_url": "KEHRNEL_HDL_ARTIFACT_S3_ENDPOINT_URL",
            "server_side_encryption": "KEHRNEL_HDL_ARTIFACT_S3_SERVER_SIDE_ENCRYPTION",
            "kms_key_id": "KEHRNEL_HDL_ARTIFACT_S3_KMS_KEY_ID",
            "access_key_id": "KEHRNEL_HDL_ARTIFACT_S3_ACCESS_KEY_ID",
            "secret_access_key": "KEHRNEL_HDL_ARTIFACT_S3_SECRET_ACCESS_KEY",
            "session_token": "KEHRNEL_HDL_ARTIFACT_S3_SESSION_TOKEN",
        }
        for key, variable in optional.items():
            value = (os.getenv(variable) or "").strip()
            if value:
                binding[key] = value
        return binding

    raise ValueError("KEHRNEL_HDL_ARTIFACT_PROVIDER must be 'filesystem' or 's3'")


def _client_kwargs(uri: str) -> dict:
    return {"tlsCAFile": certifi.where()} if mongo_tls_enabled(uri) else {}


def _b64_decode(value: str) -> bytes:
    return base64.b64decode((value or "").encode("utf-8"))


def _load_env_secrets_key() -> bytes:
    b64 = (os.getenv("ENV_SECRETS_KEY") or "").strip()
    if not b64:
        raise ValueError("ENV_SECRETS_KEY is required for HDL bindings resolver")
    key = _b64_decode(b64)
    if len(key) != 32:
        raise ValueError("ENV_SECRETS_KEY must decode to exactly 32 bytes")
    return key


def _core_store() -> _CoreStore:
    global _STORE
    if _STORE is not None:
        return _STORE
    uri = (os.getenv("CORE_MONGODB_URL") or "").strip()
    if not uri:
        raise ValueError("CORE_MONGODB_URL is required for HDL bindings resolver")
    db_name = (os.getenv("CORE_DATABASE_NAME") or "").strip()
    if not db_name:
        raise ValueError("CORE_DATABASE_NAME is required for HDL bindings resolver")
    client = MongoClient(uri, **_client_kwargs(uri))
    _STORE = _CoreStore(client=client, db_name=db_name)
    return _STORE


def _parse_bindings_ref(bindings_ref: str) -> tuple[str, Optional[str]]:
    """
    Supported refs:
      - hdl:env:<env_id>
      - hdl:env:<env_id>:mongo
      - hdl:env:<env_id>:mongo:<db_name>
      - env:<env_id>
    """
    ref = (bindings_ref or "").strip()
    parts = ref.split(":")
    if len(parts) >= 3 and parts[0] == "hdl" and parts[1] == "env":
        env_id = parts[2]
        db_name = parts[4] if len(parts) >= 5 and parts[3] == "mongo" and parts[4] else None
        return env_id, db_name
    if len(parts) == 2 and parts[0] == "env":
        return parts[1], None
    raise ValueError("Unsupported bindings_ref format")


def _decrypt_sealed_uri(sealed_uri: dict[str, Any]) -> str:
    if not isinstance(sealed_uri, dict):
        raise ValueError("sealedUri must be an object")
    iv = _b64_decode(sealed_uri.get("iv", ""))
    ct = _b64_decode(sealed_uri.get("ct", ""))
    tag = _b64_decode(sealed_uri.get("tag", ""))
    if not iv or not ct or not tag:
        raise ValueError("sealedUri must contain valid iv/ct/tag")
    key = _load_env_secrets_key()
    aesgcm = AESGCM(key)
    plaintext = aesgcm.decrypt(iv, ct + tag, None)
    return plaintext.decode("utf-8")


def _resolve_database_name(
    *,
    explicit_db: Optional[str],
    uri: str,
    context: dict[str, Any] | None,
    env_id: str,
) -> Optional[str]:
    """Resolve only the database reviewed in the strategy activation.

    The URI and HDL environment metadata describe connectivity and core tenant
    storage. They must never silently select the database used by a strategy.
    """
    cfg = ((context or {}).get("activation_config") or {})
    configured: Optional[str] = None
    if isinstance(cfg, dict):
        candidate = cfg.get("database") or cfg.get("database_name")
        if isinstance(candidate, str) and candidate.strip():
            configured = candidate.strip()
    if not configured:
        return None
    if explicit_db and explicit_db.strip() != configured:
        raise ValueError(
            f"bindings_ref database '{explicit_db.strip()}' does not match reviewed strategy database '{configured}'"
        )
    # HDL environment connection URIs may carry the environment's transversal
    # database as their default path. A domain strategy must never reuse it.
    try:
        environment_database = unquote(urlsplit(uri).path.lstrip("/").split("/", 1)[0]).strip()
    except Exception:
        environment_database = ""
    if environment_database and configured == environment_database and not _package_only_operation(context):
        raise ValueError(
            f"strategy database '{configured}' must be different from the environment core database"
        )
    return configured


def _package_only_operation(context: dict[str, Any] | None) -> bool:
    dispatch_payload = ((context or {}).get("payload") or {})
    requested_operation = dispatch_payload.get("op") if isinstance(dispatch_payload, dict) else None
    return requested_operation in {
        "fhir_resource_catalog",
        "fhir_capabilities",
        "fhir_list_search_params",
    }


def _environment_database_names(store: _CoreStore, env_id: str) -> set[str]:
    """Resolve every HDL database assigned to an env id across owner scopes.

    Older HDL data can contain the same environment id under a user and a team.
    Rejecting every matching database is safer than guessing the active owner.
    """
    names: set[str] = set()
    for collection_name in ("teams", "users"):
        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"environments.id": env_id},
                        {"environments.envId": env_id},
                        {"environments._id": env_id},
                    ]
                }
            },
            {"$unwind": "$environments"},
            {
                "$match": {
                    "$or": [
                        {"environments.id": env_id},
                        {"environments.envId": env_id},
                        {"environments._id": env_id},
                    ]
                }
            },
            {"$project": {"_id": 0, "database": "$environments.database"}},
        ]
        for row in store.db[collection_name].aggregate(pipeline):
            value = row.get("database")
            if isinstance(value, str) and value.strip():
                names.add(value.strip())
    for row in store.db["managed_environment_snapshots"].find(
        {"envId": env_id}, {"_id": 0, "dbName": 1}
    ):
        value = row.get("dbName")
        if isinstance(value, str) and value.strip():
            names.add(value.strip())
    return names


def resolve_hdl_bindings(
    *,
    bindings_ref: str,
    env_id: str,
    domain: str | None = None,
    strategy_id: str | None = None,
    op: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Resolver entrypoint used by Kehrnel runtime.
    """
    ref_env_id, ref_db = _parse_bindings_ref(bindings_ref)
    # Ensure reference env matches activated env by default.
    if ref_env_id != env_id:
        raise ValueError(f"bindings_ref env '{ref_env_id}' does not match activation env '{env_id}'")

    store = _core_store()
    secret_doc = store.db["environment_secrets"].find_one({"envId": ref_env_id})
    if not secret_doc:
        raise ValueError(f"No environment_secrets entry found for envId={ref_env_id}")
    uri = _decrypt_sealed_uri(secret_doc.get("sealedUri") or {})

    db_name = _resolve_database_name(explicit_db=ref_db, uri=uri, context=context, env_id=ref_env_id)
    if not db_name:
        raise ValueError(
            "Strategy activation config must define its MongoDB database"
        )
    if (
        str(domain or "").strip().lower() == "fhir"
        and not _package_only_operation(context)
        and db_name in _environment_database_names(store, env_id)
    ):
        raise ValueError(
            f"FHIR strategy database '{db_name}' must be different from the HDL environment database"
        )

    resolved = {
        "db": {
            "provider": "mongodb",
            "uri": uri,
            "database": db_name,
        }
    }
    artifact = _artifact_binding(env_id)
    if artifact:
        resolved["artifact"] = artifact
    return resolved

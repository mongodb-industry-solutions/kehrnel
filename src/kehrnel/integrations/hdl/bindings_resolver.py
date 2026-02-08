"""Resolve Kehrnel bindings from Healthcare Data Lab encrypted environment secrets.

Set:
  KEHRNEL_BINDINGS_RESOLVER=kehrnel.integrations.hdl.bindings_resolver:resolve_hdl_bindings

Required env vars:
  ENV_SECRETS_KEY           base64-encoded 32-byte key (AES-256-GCM)
  CORE_MONGODB_URL          HDL core DB URI (fallback: MONGODB_URI)
  CORE_DATABASE_NAME        HDL core DB name
"""
from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlparse

import certifi
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pymongo import MongoClient


DEFAULT_ENV_META_COLLECTIONS = ("teams", "users", "workspaces")


@dataclass
class _CoreStore:
    client: MongoClient
    db_name: str

    @property
    def db(self):
        return self.client[self.db_name]


_STORE: Optional[_CoreStore] = None


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
    uri = (os.getenv("CORE_MONGODB_URL") or os.getenv("MONGODB_URI") or "").strip()
    if not uri:
        raise ValueError("CORE_MONGODB_URL (or MONGODB_URI) is required for HDL bindings resolver")
    db_name = (os.getenv("CORE_DATABASE_NAME") or "").strip()
    if not db_name:
        raise ValueError("CORE_DATABASE_NAME is required for HDL bindings resolver")
    client = MongoClient(uri, tlsCAFile=certifi.where())
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


def _uri_database_name(uri: str) -> Optional[str]:
    try:
        parsed = urlparse(uri)
        path = (parsed.path or "").lstrip("/")
        if not path:
            return None
        return path.split("/", 1)[0] or None
    except Exception:
        return None


def _collections_for_env_metadata() -> tuple[str, ...]:
    raw = (os.getenv("KEHRNEL_HDL_ENV_METADATA_COLLECTIONS") or "").strip()
    if not raw:
        return DEFAULT_ENV_META_COLLECTIONS
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def _lookup_env_database(env_id: str) -> Optional[str]:
    store = _core_store()
    for coll_name in _collections_for_env_metadata():
        coll = store.db[coll_name]
        doc = coll.find_one({"environments.id": env_id}, {"environments.$": 1})
        if not doc:
            continue
        envs = doc.get("environments") or []
        if not envs:
            continue
        db_name = (envs[0] or {}).get("database")
        if db_name:
            return str(db_name)
    return None


def _resolve_database_name(
    *,
    explicit_db: Optional[str],
    uri: str,
    context: dict[str, Any] | None,
    env_id: str,
) -> Optional[str]:
    if explicit_db:
        return explicit_db
    from_uri = _uri_database_name(uri)
    if from_uri:
        return from_uri

    cfg = ((context or {}).get("activation_config") or {})
    if isinstance(cfg, dict):
        for path in (
            ("database",),
            ("db", "database"),
            ("target", "database_name"),
            ("source", "database_name"),
        ):
            cur = cfg
            for part in path:
                if isinstance(cur, dict) and part in cur:
                    cur = cur[part]
                else:
                    cur = None
                    break
            if isinstance(cur, str) and cur.strip():
                return cur.strip()

    from_env = _lookup_env_database(env_id)
    if from_env:
        return from_env
    return None


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
            "Could not determine database name from bindings_ref, URI, activation config, or environment metadata"
        )

    return {
        "db": {
            "provider": "mongodb",
            "uri": uri,
            "database": db_name,
        }
    }


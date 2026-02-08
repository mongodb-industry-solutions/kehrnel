"""Motor client factory."""
from __future__ import annotations

import os
from motor.motor_asyncio import AsyncIOMotorClient


def get_client(bindings: dict) -> AsyncIOMotorClient:
    uri = (bindings or {}).get("db", {}).get("uri")
    if not uri:
        raise ValueError("Mongo bindings must include db.uri")
    allow_invalid = os.getenv("KEHRNEL_MONGO_TLS_ALLOW_INVALID_CERTS", "false").lower() in ("1", "true", "yes")
    tls_ca_file = os.getenv("KEHRNEL_MONGO_TLS_CA_FILE")
    if not tls_ca_file:
        try:
            import certifi  # type: ignore
            tls_ca_file = certifi.where()
        except Exception:
            tls_ca_file = None
    kwargs = {
        "serverSelectionTimeoutMS": 5000,
        "tlsAllowInvalidCertificates": allow_invalid,
    }
    if tls_ca_file and not allow_invalid:
        kwargs["tlsCAFile"] = tls_ca_file
    return AsyncIOMotorClient(
        uri,
        **kwargs,
    )


def get_database(bindings: dict):
    client = get_client(bindings)
    db_name = (bindings or {}).get("db", {}).get("database")
    if not db_name:
        raise ValueError("Mongo bindings must include db.database")
    return client[db_name]

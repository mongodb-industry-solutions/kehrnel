"""Motor client factory."""
from __future__ import annotations

import os
from motor.motor_asyncio import AsyncIOMotorClient


def get_client(bindings: dict) -> AsyncIOMotorClient:
    uri = (bindings or {}).get("db", {}).get("uri")
    if not uri:
        raise ValueError("Mongo bindings must include db.uri")
    
    # Only apply TLS settings if explicitly requested in URI or environment
    use_tls = uri.startswith("mongodb+srv://") or "tls=true" in uri.lower() or "ssl=true" in uri.lower()
    allow_invalid = os.getenv("KEHRNEL_MONGO_TLS_ALLOW_INVALID_CERTS", "false").lower() in ("1", "true", "yes")
    
    kwargs = {
        "serverSelectionTimeoutMS": 5000,
    }
    
    if use_tls or allow_invalid:
        tls_ca_file = os.getenv("KEHRNEL_MONGO_TLS_CA_FILE")
        if not tls_ca_file and use_tls:
            try:
                import certifi  # type: ignore
                tls_ca_file = certifi.where()
            except Exception:
                tls_ca_file = None
        
        if allow_invalid:
            kwargs["tlsAllowInvalidCertificates"] = allow_invalid
        if tls_ca_file and use_tls and not allow_invalid:
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

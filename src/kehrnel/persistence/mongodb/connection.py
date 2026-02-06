"""Motor client factory."""
from __future__ import annotations

from motor.motor_asyncio import AsyncIOMotorClient


def get_client(bindings: dict) -> AsyncIOMotorClient:
    uri = (bindings or {}).get("db", {}).get("uri")
    if not uri:
        raise ValueError("Mongo bindings must include db.uri")
    return AsyncIOMotorClient(uri, serverSelectionTimeoutMS=5000, tlsAllowInvalidCertificates=True)


def get_database(bindings: dict):
    client = get_client(bindings)
    db_name = (bindings or {}).get("db", {}).get("database")
    if not db_name:
        raise ValueError("Mongo bindings must include db.database")
    return client[db_name]

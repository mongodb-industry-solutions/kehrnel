"""
Tiny helper that converts a DSN into a Store instance.
Actual back-ends live OUTSIDE core, e.g.:

    pip install kehrnel-store-mongo
"""
from importlib import import_module
from typing import Any

from .base import Store


def get_store(dsn: str) -> Store:
    """
    Supported DSN examples
    ----------------------
    mongo://host:27017/db
    file:///path/to/folder
    """
    scheme, rest = dsn.split("://", 1)

    match scheme:
        case "mongo":
            mod  = import_module("store.mongodb_store")
            return mod.MongoStore(uri=f"mongodb://{rest}")
        case "file":
            mod  = import_module("store.file_store")
            return mod.FileStore(rest)
        case _:
            raise ValueError(f"Unknown store scheme: {scheme}")
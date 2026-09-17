"""Entrypoint shim — implementation lives in ``scripts/strategy.py``."""

from kehrnel.engine.strategies.fhir.resource_store.scripts.strategy import (
    DEFAULTS_PATH,
    FHIRResourceStoreStrategy,
    MANIFEST,
    MANIFEST_PATH,
    SCHEMA_PATH,
)

__all__ = [
    "DEFAULTS_PATH",
    "FHIRResourceStoreStrategy",
    "MANIFEST",
    "MANIFEST_PATH",
    "SCHEMA_PATH",
]

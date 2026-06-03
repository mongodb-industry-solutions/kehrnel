"""Entrypoint shim — implementation lives in ``scripts/strategy.py``."""

from kehrnel.engine.strategies.fhir.rps_canonical.scripts.strategy import (
    DEFAULTS_PATH,
    FHIRRPSCanonicalStrategy,
    MANIFEST,
    MANIFEST_PATH,
    SCHEMA_PATH,
)

__all__ = [
    "DEFAULTS_PATH",
    "FHIRRPSCanonicalStrategy",
    "MANIFEST",
    "MANIFEST_PATH",
    "SCHEMA_PATH",
]

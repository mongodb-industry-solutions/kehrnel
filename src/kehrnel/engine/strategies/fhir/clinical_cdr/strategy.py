"""Entrypoint shim — implementation lives in ``scripts/strategy.py``."""

from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.strategy import (
    DEFAULTS_PATH,
    FHIRClinicalCDRStrategy,
    MANIFEST,
    MANIFEST_PATH,
    SCHEMA_PATH,
)

__all__ = [
    "DEFAULTS_PATH",
    "FHIRClinicalCDRStrategy",
    "MANIFEST",
    "MANIFEST_PATH",
    "SCHEMA_PATH",
]

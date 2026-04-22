"""Backward-compatible wrapper for ``kehrnel.engine.strategies.openehr.rps_dual_ibm.strategy``."""

from kehrnel.engine.strategies.openehr.rps_dual_ibm.strategy import (
    DEFAULTS_PATH,
    MANIFEST,
    RPSDualIBMStrategy,
    load_json,
)

__all__ = ["RPSDualIBMStrategy", "MANIFEST", "DEFAULTS_PATH", "load_json"]

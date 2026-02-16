"""Backward-compatible wrapper for ``kehrnel.engine.strategies.openehr.rps_dual.strategy``."""

from kehrnel.engine.strategies.openehr.rps_dual.strategy import (
    DEFAULTS_PATH,
    MANIFEST,
    RPSDualStrategy,
    load_json,
)

__all__ = ["RPSDualStrategy", "MANIFEST", "DEFAULTS_PATH", "load_json"]


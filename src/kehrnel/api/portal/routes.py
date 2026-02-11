"""Compatibility shim for legacy portal imports.

Use `kehrnel.api.strategies.openehr.rps_dual.routes` as the canonical location.
"""

from kehrnel.api.strategies.openehr.rps_dual.routes import router

__all__ = ["router"]

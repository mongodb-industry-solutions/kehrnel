"""Compatibility shim for legacy imports.

Legacy stack imports `kehrnel.legacy.transform.flattener_g.CompositionFlattener`.
The canonical implementation now lives under
`kehrnel.strategies.openehr.rps_dual.ingest.flattener`.
"""

from kehrnel.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener

__all__ = ["CompositionFlattener"]

"""Compatibility shim for legacy imports.

Legacy stack imports `kehrnel.legacy.transform.flattener_g.CompositionFlattener`.
The canonical implementation now lives under
`kehrnel.strategies.openehr.rps_dual.ingest.flattener_f`.
"""

from kehrnel.strategies.openehr.rps_dual.ingest.flattener_f import CompositionFlattener

__all__ = ["CompositionFlattener"]

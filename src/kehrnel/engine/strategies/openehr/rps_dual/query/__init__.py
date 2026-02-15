"""AQL→MQL compiler implementation for the rps_dual strategy."""
from __future__ import annotations

from kehrnel.engine.domains.openehr.aql.ir import AqlQueryIR
from .compiler import build_query_pipeline
from .executor import execute

__all__ = ["AqlQueryIR", "build_query_pipeline", "execute"]

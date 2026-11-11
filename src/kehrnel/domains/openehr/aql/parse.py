"""Legacy import shim for AQL parsing.

Tests and older clients may import:
`kehrnel.domains.openehr.aql.parse:parse_aql`
"""

from kehrnel.engine.domains.openehr.aql.parse import parse_aql

__all__ = ["parse_aql"]


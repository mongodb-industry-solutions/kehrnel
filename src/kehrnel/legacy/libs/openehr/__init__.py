"""
Shared OpenEHR utilities (extracted facade).
Currently re-exports existing implementations; future work can move logic here for reuse
across multiple openEHR strategies without duplicating code.
"""

from .flattener import (
    get_flattener,
    get_transformer_with_rules,
    build_shortcuts,
    build_rules_engine,
    load_codes,
)
from .transformer import get_transformer
from .remap import remap_fields_for_config
from .coding import get_codec
from .traversal import flatten_nodes
from .validator import get_validator

__all__ = [
    "get_flattener",
    "get_transformer",
    "get_transformer_with_rules",
    "remap_fields_for_config",
    "get_codec",
    "load_codes",
    "flatten_nodes",
    "build_shortcuts",
    "build_rules_engine",
    "get_validator",
]

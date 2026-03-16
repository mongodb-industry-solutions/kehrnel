"""
kehrnel – mapping layer.

Simply provides a proper package so callers can
    >>> from kehrnel.engine.common.mapping import mapping_engine
"""

from importlib import import_module as _imp

# expose mapping_engine at package root for convenience
mapping_engine = _imp("kehrnel.engine.common.mapping.mapping_engine")

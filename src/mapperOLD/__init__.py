"""
kehrnel – mapping layer.

Simply provides a proper package so callers can
    >>> from mapper import mapping_engine
"""

from importlib import import_module as _imp

# expose mapping_engine at package root for convenience
mapping_engine = _imp("mapper.mapping_engine")
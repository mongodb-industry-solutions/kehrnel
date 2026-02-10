"""
Canonical ⇄ Flattened transformer utilities.

Public helpers
--------------
flatten_one()  → (full_doc, search_doc)
expand_one()   → canonical JSON
"""
from .single import flatten_one, expand_one   # re-export for convenience

__all__ = ["flatten_one", "expand_one"]
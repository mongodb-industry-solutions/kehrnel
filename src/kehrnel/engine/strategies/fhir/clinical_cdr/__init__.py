"""FHIR Clinical CDR strategy pack (fhir-gen + fhir-mql)."""

from __future__ import annotations

import sys
from importlib import import_module

_SUBMODULES = (
    "bridge",
    "denormalize",
    "generation",
    "indexes",
    "query",
    "stats",
    "strategy",
    "watermark",
)

for _name in _SUBMODULES:
    _mod = import_module(f".scripts.{_name}", __name__)
    sys.modules[f"{__name__}.{_name}"] = _mod

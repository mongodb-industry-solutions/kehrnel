"""Optional per-resource enrichers (Prompts 9–13)."""

from __future__ import annotations

from typing import Any, Callable

from . import clinical, financial, medication, specialized, workflow

ENRICHERS: dict[str, Callable[..., dict[str, Any]]] = {
    **clinical.ENRICHERS,
    **medication.ENRICHERS,
    **workflow.ENRICHERS,
    **financial.ENRICHERS,
    **specialized.ENRICHERS,
}

__all__ = [
    "ENRICHERS",
    "clinical",
    "financial",
    "medication",
    "specialized",
    "workflow",
]

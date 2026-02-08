"""Compatibility facade for legacy imports (`core.*`)."""

from kehrnel.domains.openehr.templates import (
    TemplateParser,
    kehrnelGenerator,
    kehrnelValidator,
    Severity,
    ValidationIssue,
    Store,
    get_store,
)

__all__ = [
    "TemplateParser",
    "kehrnelGenerator",
    "kehrnelValidator",
    "ValidationIssue",
    "Severity",
    "Store",
    "get_store",
]

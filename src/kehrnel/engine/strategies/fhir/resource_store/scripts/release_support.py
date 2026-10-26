"""Explicit release support tiers for the FHIR resource-store accelerator.

R4, R5 and R6 are all backed by bundled release schemas.
Keeping this declaration separate makes the support boundary visible and
easy to update without changing the API or Healthcare Data Lab contracts.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qsl

from kehrnel.engine.core.errors import KehrnelError


SUPPORTED_RELEASES = frozenset({"R4", "R5", "R6"})
SCHEMA_BACKED_RELEASES = frozenset({"R4", "R5", "R6"})

# R4 is now fully backed by the bundled fhir.schema.v4.json and 146 R4 search
# configs.  The minimal-baseline constants are kept as empty sentinels so that
# any code that still imports them does not break at import time.
R4_MINIMAL_SEARCH_PARAMETERS: dict[str, frozenset[str]] = {}
R4_MINIMAL_RESOURCE_TYPES: frozenset[str] = frozenset()


def normalize_release(value: Any) -> str:
    release = str(value or "R5").strip().upper()
    if release not in SUPPORTED_RELEASES:
        raise ValueError(f"Unsupported FHIR release: {value!r}")
    return release


def release_evidence(release: Any) -> dict[str, Any]:
    normalized = normalize_release(release)
    if normalized == "R4":
        return {
            "release": normalized,
            "support_tier": "package-backed",
            "base_schema_validation": True,
            "generated_release_assets": True,
            "description": (
                "Full R4 support: 146 resource types, base schema validation via "
                "fhir.schema.v4.json, and complete R4 search configs."
            ),
        }
    return {
        "release": normalized,
        "support_tier": "package-backed",
        "base_schema_validation": True,
        "generated_release_assets": True,
        "description": f"Bundled {normalized} schema and search-package support.",
    }


def allowed_search_parameters(
    release: Any, resource_type: str
) -> frozenset[str] | None:
    """Return a release-specific allowlist, or ``None`` when package config owns it."""
    # R4 is now fully package-backed; no restriction allowlist applies.
    return None


def validate_search_scope(
    release: Any,
    resource_type: str,
    *,
    query_string: str | None = None,
    compartment: dict[str, Any] | None = None,
    sort_value: str | None = None,
) -> None:
    """Validate search scope against the active release config.

    R4 is now fully package-backed (146 resource types, complete search configs),
    so no provisional restrictions are applied.  The fhir-mql converter is the
    authority for all releases.
    """
    # All releases: no provisional restrictions — the converter is the authority.
    return

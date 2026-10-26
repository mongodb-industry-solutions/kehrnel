"""Explicit release support tiers for the FHIR resource-store accelerator.

R5 and R6 are backed by bundled release schemas.  R4 is intentionally a small
interoperability baseline until the generated R4 assets land: it supports
structural validation and a reviewed subset of the existing projection paths.
Keeping this declaration separate makes the temporary boundary visible and
easy to replace without changing the API or Healthcare Data Lab contracts.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qsl

from kehrnel.engine.core.errors import KehrnelError


SUPPORTED_RELEASES = frozenset({"R4", "R5", "R6"})
SCHEMA_BACKED_RELEASES = frozenset({"R5", "R6"})

# Only resource/search shapes reviewed as stable across R4 and the bundled R5
# mappings belong here.  Desh's generated R4 package will supersede this list.
R4_MINIMAL_SEARCH_PARAMETERS: dict[str, frozenset[str]] = {
    "Patient": frozenset(
        {
            "_id",
            "active",
            "birthdate",
            "family",
            "gender",
            "given",
            "identifier",
            "name",
        }
    ),
    "Observation": frozenset(
        {
            "_id",
            "category",
            "code",
            "date",
            "encounter",
            "identifier",
            "patient",
            "status",
            "subject",
        }
    ),
}
R4_MINIMAL_RESOURCE_TYPES = frozenset(R4_MINIMAL_SEARCH_PARAMETERS)


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
            "support_tier": "minimal",
            "base_schema_validation": False,
            "generated_release_assets": False,
            "description": (
                "Provisional R4 interoperability baseline: Patient and Observation "
                "structural validation, canonical storage, and reviewed search parameters."
            ),
            "extension_owner": "Desh",
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
    if normalize_release(release) != "R4":
        return None
    return R4_MINIMAL_SEARCH_PARAMETERS.get(resource_type, frozenset())


def validate_search_scope(
    release: Any,
    resource_type: str,
    *,
    query_string: str | None = None,
    compartment: dict[str, Any] | None = None,
    sort_value: str | None = None,
) -> None:
    """Fail closed when a request exceeds the provisional R4 contract."""
    if normalize_release(release) != "R4":
        return
    allowed = allowed_search_parameters("R4", resource_type) or frozenset()
    if resource_type not in R4_MINIMAL_RESOURCE_TYPES:
        raise KehrnelError(
            code="FHIR_R4_RESOURCE_NOT_IN_MINIMAL_SCOPE",
            status=400,
            message=f"{resource_type} is not in the provisional R4 resource scope",
            details={"supported_resource_types": sorted(R4_MINIMAL_RESOURCE_TYPES)},
        )
    if compartment:
        raise KehrnelError(
            code="FHIR_R4_COMPARTMENT_SEARCH_UNAVAILABLE",
            status=400,
            message="Compartment search is not included in the provisional R4 baseline",
        )

    requested = []
    for raw_name, _value in parse_qsl(query_string or "", keep_blank_values=True):
        base_name = raw_name.split(":", 1)[0]
        # Chained/reverse-chained parameters are outside every current contract.
        if "." in base_name or base_name.startswith("_has"):
            requested.append(raw_name)
        elif base_name not in allowed:
            requested.append(raw_name)
    for raw_sort in str(sort_value or "").split(","):
        name = raw_sort.strip().lstrip("-")
        if name and name not in allowed:
            requested.append(f"_sort={name}")
    if requested:
        raise KehrnelError(
            code="FHIR_R4_SEARCH_OUTSIDE_MINIMAL_SCOPE",
            status=400,
            message="One or more search parameters are outside the provisional R4 baseline",
            details={
                "resource_type": resource_type,
                "unsupported": sorted(set(requested)),
                "supported": sorted(allowed),
            },
        )

"""Versioned MongoDB document contract for the FHIR Clinical CDR.

FHIR ``meta.versionId`` belongs to the clinical resource and must never be used
to version Kehrnel's persistence mechanics.  Those mechanics live under the
reserved ``_kehrnel`` field instead.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable


STORED_DOCUMENT_SCHEMA_VERSION = "2"
PROJECTION_VERSION_ALGORITHM = "1"
OPERATIONAL_METADATA_FIELD = "_kehrnel"
CUSTOM_DATA_FIELD = "_custom"
ENRICHMENT_DATA_FIELD = "_enrichments"
PRESERVED_EXTENSION_FIELDS = frozenset({CUSTOM_DATA_FIELD, ENRICHMENT_DATA_FIELD})


def _digest(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _compartment_contract(definitions_dir: str | None) -> dict[str, str]:
    """Return content hashes, not paths, so versions remain portable."""
    if not definitions_dir:
        return {}
    root = Path(definitions_dir)
    if not root.is_dir():
        raise ValueError(f"Compartment definitions directory does not exist: {root}")
    files = sorted((*root.glob("*.json"), *root.glob("*.yaml"), *root.glob("*.yml")))
    if not files:
        raise ValueError(f"No compartment definitions found in: {root}")
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in files}


def _version(payload: Any) -> str:
    return f"v{PROJECTION_VERSION_ALGORITHM}:{_digest(payload)}"


@dataclass(frozen=True)
class ProjectionVersions:
    """Global projection contract plus independently actionable type versions."""

    fhir_release: str
    projection_contract_version: str
    resource_projection_versions: dict[str, str]

    def for_resource(self, resource_type: str) -> str:
        try:
            return self.resource_projection_versions[resource_type]
        except KeyError as exc:
            raise ValueError(f"No projection version for {resource_type}") from exc


def build_projection_versions(
    config_loader: Any,
    *,
    fhir_release: str,
    compartment_definitions_dir: str | None,
    resource_types: Iterable[str] | None = None,
) -> ProjectionVersions:
    """Fingerprint effective search and compartment configuration.

    The global version detects any projection-contract change.  The per-resource
    version permits targeted re-projection when only one resource YAML changes.
    A compartment-definition change intentionally invalidates every type because
    membership rules are shared across the model.
    """
    release = str(fhir_release or "").strip().upper()
    all_types = sorted({str(value) for value in config_loader.list_resources()})
    selected = (
        all_types
        if resource_types is None
        else sorted({str(value) for value in resource_types})
    )
    unknown = sorted(set(selected) - set(all_types))
    if unknown:
        raise ValueError(
            f"No search projection configuration for: {', '.join(unknown)}"
        )

    compartment_contract = _compartment_contract(compartment_definitions_dir)
    compartment_version = _digest(compartment_contract)
    all_configs = {
        resource_type: config_loader.get_config(resource_type)
        for resource_type in all_types
    }
    global_payload = {
        "algorithm": PROJECTION_VERSION_ALGORITHM,
        "fhir_release": release,
        "resources": all_configs,
        "compartments": compartment_contract,
    }
    per_resource = {
        resource_type: _version(
            {
                "algorithm": PROJECTION_VERSION_ALGORITHM,
                "fhir_release": release,
                "resource": all_configs[resource_type],
                "compartments": compartment_version,
            }
        )
        for resource_type in selected
    }
    return ProjectionVersions(
        fhir_release=release,
        projection_contract_version=_version(global_payload),
        resource_projection_versions=per_resource,
    )


def normalize_projection_buckets(resource: dict[str, Any]) -> dict[str, Any]:
    """Guarantee both mandatory projection buckets exist and have stable shapes."""
    result = dict(resource)
    search = result.get("_search")
    compartments = result.get("_compartments")
    if search is None:
        result["_search"] = {}
    elif not isinstance(search, dict):
        raise ValueError("_search must be an object")
    if compartments is None:
        result["_compartments"] = {}
    elif not isinstance(compartments, dict):
        raise ValueError("_compartments must be an object")
    return result


def stamp_projection_metadata(
    resource: dict[str, Any],
    versions: ProjectionVersions,
    *,
    projected_at: datetime | None = None,
    provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Attach Kehrnel-owned version metadata to a projected resource."""
    resource_type = str(resource.get("resourceType") or "")
    if not resource_type:
        raise ValueError("Resource must have resourceType before metadata is stamped")
    result = normalize_projection_buckets(resource)
    existing = result.get(OPERATIONAL_METADATA_FIELD)
    metadata = dict(existing) if isinstance(existing, dict) else {}
    metadata.update(
        {
            "storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
            "projection_contract_version": versions.projection_contract_version,
            "resource_projection_version": versions.for_resource(resource_type),
            "fhir_release": versions.fhir_release,
            "projected_at": projected_at or datetime.now(UTC),
        }
    )
    if provenance:
        metadata["provenance"] = {
            str(key): value
            for key, value in provenance.items()
            if value is not None and value != ""
        }
    result[OPERATIONAL_METADATA_FIELD] = metadata
    return result


def metadata_set_fields(
    versions: ProjectionVersions,
    resource_type: str,
    *,
    projected_at: datetime | None = None,
) -> dict[str, Any]:
    """MongoDB dotted-field update used when re-projecting existing documents."""
    prefix = OPERATIONAL_METADATA_FIELD
    return {
        f"{prefix}.storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
        f"{prefix}.projection_contract_version": versions.projection_contract_version,
        f"{prefix}.resource_projection_version": versions.for_resource(resource_type),
        f"{prefix}.fhir_release": versions.fhir_release,
        f"{prefix}.projected_at": projected_at or datetime.now(UTC),
    }

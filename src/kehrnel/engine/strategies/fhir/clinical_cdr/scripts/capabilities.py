"""Authoritative resource capability resolution for the FHIR accelerator.

FHIR release schemas, search mappings, synthetic generation, and named example
recipes are independent concerns.  Keeping their inventories separate prevents
a small demonstration recipe from accidentally becoming the write allowlist for
the resource store.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.validation import (
    schema_resource_types,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.release_support import (
    normalize_release,
)


@dataclass(frozen=True)
class ResourceCapabilitySets:
    """Resolved capability sets for one activated FHIR release."""

    schema_supported: frozenset[str]
    searchable: frozenset[str]
    storable: frozenset[str]
    generatable: frozenset[str]
    synthetic_writable: frozenset[str]
    recipe_resources: frozenset[str]


def resolve_resource_capabilities(
    cfg: dict[str, Any],
    config_loader: Any,
) -> ResourceCapabilitySets:
    """Resolve capabilities without deriving persistence from example recipes.

    The current persistence contract requires a search/compartment projection
    for every stored resource.  A resource is therefore writable when it exists
    in the selected release schema *and* has an active fhir-mql configuration.
    As Desh adds generated or reviewed mappings, those resources become writable
    automatically; no generation recipe change is required.
    """

    release = normalize_release(cfg.get("schema_version") or "R5")
    schema_supported = frozenset(schema_resource_types(release))
    configured_search = frozenset(
        str(value) for value in bridge.supported_search_resource_types(config_loader)
    )
    searchable = configured_search & schema_supported
    generatable = (
        frozenset()
        if release == "R4"
        else frozenset(bridge.known_generation_resource_types()) & schema_supported
    )
    storable = searchable
    return ResourceCapabilitySets(
        schema_supported=schema_supported,
        searchable=searchable,
        storable=storable,
        generatable=generatable,
        synthetic_writable=generatable & storable,
        recipe_resources=(
            frozenset()
            if release == "R4"
            else frozenset(bridge.configured_cdr_resource_types(cfg))
        ),
    )

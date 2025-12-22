from __future__ import annotations

from typing import Any, Dict

from strategy_sdk import (
    AdapterRequirements,
    StrategyBindings,
    StrategyCapability,
    StrategyCompatibility,
    StrategyContext,
    StrategyInitResult,
    StrategyManifest,
    StrategyPlugin,
    StrategyUI,
)
from adapters.search_base import SearchAdapter


MANIFEST = StrategyManifest(
    id="fhir.resource_first",
    name="FHIR Resource-First",
    version="0.1.0",
    summary="Stores native FHIR resources with search mirror for optimized queries.",
    protocols=["FHIR"],
    capabilities=[StrategyCapability.INGEST, StrategyCapability.SEARCH],
    entrypoint="strategies.fhir.resource_first:FHIRResourceFirstStrategy",
    adapters=AdapterRequirements(storage=["mongo"], search=["atlas_search"]),
    ui=StrategyUI(
        tags=["FHIR", "resource-first"],
        protocol_badge="FHIR",
        icon="fhir",
        accent_color="#E05E3E",
        works_with=["atlas_search"],
        links={
            "source": "src/strategies/fhir/resource_first.py",
            "docs": "docs/strategies-guide.md",
        },
    ),
    compatibility=StrategyCompatibility(storage=["mongo"], search=["atlas_search"]),
    config_schema={
        "type": "object",
        "properties": {
            "database": {"type": "string"},
            "collections": {
                "type": "object",
                "properties": {
                    "resources": {"type": "string"},
                    "search": {"type": "string"},
                },
            },
        },
        "required": ["database", "collections"],
    },
    default_config={
        "database": "fhir_cdr",
        "collections": {"resources": "fhir_resources", "search": "fhir_search"},
    },
    maturity="experimental",
    license="Apache-2.0",
)


class FHIRResourceFirstStrategy(StrategyPlugin):
    """
    Simple FHIR strategy stub: stores native resources and an optional search mirror.
    This is deliberately minimal to demonstrate multi-protocol routing; enrich/search
    are not implemented yet.
    """

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        super().__init__(manifest)
        self.config: Dict[str, Any] = {}

    def initialize(self, config: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> StrategyInitResult:
        self.config = config or self.manifest.default_config
        return StrategyInitResult(config_applied=self.config, notes=["FHIR strategy stub; not yet implemented"])

    def ingest(self, payload: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Dict[str, Any]:
        storage = getattr(bindings, "storage", None) if bindings else None
        base_doc = payload
        search_doc = self._build_search_mirror(payload)
        if storage and hasattr(storage, "insert_one"):
            storage.insert_one(base_doc, search=False)
            if search_doc:
                storage.insert_one(search_doc, search=True)
        return {"base": base_doc, "search": search_doc}

    def search(self, query: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Any:
        search_adapter: SearchAdapter = getattr(bindings, "search", None)  # type: ignore
        if not search_adapter:
            raise ValueError("No search adapter bound")
        return search_adapter.search(query)

    # ─── Helpers ──────────────────────────────────────────────────────────
    def _build_search_mirror(self, resource: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Extremely lightweight search mirror: captures resourceType, id, meta, text, subject/reference,
        and common date fields. Replace with real SearchParameter evaluation later.
        """
        if not isinstance(resource, dict):
            return None
        rid = resource.get("id")
        rtype = resource.get("resourceType", "Resource")
        search_doc = {
            "_id": rid or None,
            "resourceType": rtype,
            "meta": resource.get("meta", {}),
            "text": resource.get("text", {}),
        }
        for field in ("subject", "patient", "encounter", "basedOn", "author", "performer"):
            if field in resource:
                search_doc[field] = resource[field]
        for date_field in ("effectiveDateTime", "issued", "authoredOn", "recordedDate"):
            if date_field in resource:
                search_doc[date_field] = resource[date_field]
        # strip None/empty fields
        search_doc = {k: v for k, v in search_doc.items() if v not in (None, {}, [], "")}
        if not search_doc:
            return None
        return search_doc

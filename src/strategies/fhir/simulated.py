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


MANIFEST = StrategyManifest(
    id="fhir.simulated_skeleton",
    name="FHIR Simulated Strategy (Skeleton)",
    version="0.0.1",
    summary="Skeleton FHIR strategy demonstrating manifest + capability wiring without real processing.",
    protocols=["FHIR"],
    capabilities=[StrategyCapability.INGEST, StrategyCapability.SEARCH],
    entrypoint="strategies.fhir.simulated:FHIRSimulatedStrategy",
    adapters=AdapterRequirements(storage=["mongo"], search=["opensearch", "atlas_search"]),
    ui=StrategyUI(
        tags=["FHIR", "skeleton", "testing"],
        protocol_badge="FHIR",
        icon="fhir",
        accent_color="#E05E3E",
        works_with=["opensearch"],
        links={
            "source": "src/strategies/fhir/simulated.py",
            "docs": "docs/strategies-guide.md",
        },
    ),
    compatibility=StrategyCompatibility(storage=["mongo"], search=["opensearch", "atlas_search"]),
    config_schema={
        "type": "object",
        "properties": {
            "mode": {"type": "string", "enum": ["noop", "echo"]},
            "collections": {"type": "object"},
            "search": {"type": "object"},
        },
        "required": ["mode"],
    },
    default_config={
        "mode": "echo",
        "collections": {"resources": "fhir_sim_resources", "search": "fhir_sim_search"},
        "search": {"backend": "opensearch", "index": "fhir_sim_search"},
    },
    maturity="experimental",
    license="Apache-2.0",
)


class FHIRSimulatedStrategy(StrategyPlugin):
    """
    Skeleton strategy that demonstrates ingest/search hooks without domain logic.
    - ingest: stores or echoes payload based on mode
    - search: returns a canned response
    """

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        super().__init__(manifest)
        self.config: Dict[str, Any] = {}

    def initialize(self, config: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> StrategyInitResult:
        self.config = config or self.manifest.default_config
        return StrategyInitResult(config_applied=self.config, notes=["Simulated strategy; no domain validation performed"])

    def ingest(self, payload: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Dict[str, Any]:
        mode = (self.config or {}).get("mode", "echo")
        storage = getattr(bindings, "storage", None) if bindings else None
        if mode == "noop":
            return {"base": payload, "search": None, "status": "noop"}
        if storage and hasattr(storage, "insert_one"):
            storage.insert_one(payload, search=False)
        return {"base": payload, "search": None, "status": "stored" if storage else "echo"}

    def search(self, query: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Any:
        return {"status": "simulated", "query": query}

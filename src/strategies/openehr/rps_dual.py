from __future__ import annotations

from typing import Any, Dict, Tuple

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
from libs.openehr.remap import remap_fields_for_config

# Reference manifest for the existing reversed-path dual-collection strategy.
# This wraps the current CompositionFlattener pipeline as a plugin.
MANIFEST = StrategyManifest(
    id="openehr.rps_dual",
    name="Reversed Path Search - Dual Collection",
    version="0.1.0",
    summary="Optimizes indexing with a slim search collection alongside canonical nodes.",
    description="Reference OpenEHR strategy using reversed paths, dual collections, and Atlas Search dual-mode querying.",
    protocols=["openEHR"],
    capabilities=[
        StrategyCapability.TRANSFORM,
        StrategyCapability.INGEST,
        StrategyCapability.VALIDATE,
        StrategyCapability.SEARCH,
    ],
    entrypoint="strategies.openehr.rps_dual:OpenEHRReversedPathStrategy",
    adapters=AdapterRequirements(storage=["mongo"], search=["atlas_search"]),
    ui=StrategyUI(
        tags=["openEHR", "dual-collection", "atlas-search"],
        protocol_badge="openEHR",
        icon="openehr",
        accent_color="#5B8C5A",
        works_with=["atlas_search_dual"],
        links={
            "source": "src/strategies/openehr/rps_dual.py",
            "docs": "docs/strategies-guide.md",
            "libs": "libs/openehr",
            "flattener": "src/transform/flattener_g.py",
            "remap": "libs/openehr/remap.py",
        },
    ),
    compatibility=StrategyCompatibility(
        storage=["mongo"],
        search=["atlas_search_dual"],
        protocols=["openEHR-RM"],
    ),
    config_schema={
        "type": "object",
        "properties": {
            "database": {"type": "string"},
            "collections": {"type": "object"},
            "coding": {"type": "object"},
            "fields": {"type": "object"},
            "node_representation": {"type": "object"},
            "query_engine": {"type": "object"},
        },
        "required": ["database", "collections", "fields"],
        "additionalProperties": True,
    },
    default_config={
        "database": "openehr_cdr",
        "collections": {
            "compositions": {
                "name": "compositions_rps",
                "store_canonical": True,
                "nodes_field": "cn",
                "reverse_paths": True,
                "store_nodes": True,
            },
            "search": {
                "name": "compositions_search",
                "enabled": True,
                "nodes_field": "sn",
                "atlas_index_name": "search_nodes_index",
                "slim_projection": {
                    "time": "data.time.value",
                    "code": "data.defining_code.code_string",
                    "text": "data.value.value",
                },
            },
        },
        "coding": {
            "archetype_ids": {"enabled": True, "dictionary": "arcodes", "sequential": True},
            "atcodes": {"enabled": True, "strategy": "negative_int", "store_original": True},
        },
        "fields": {
            "composition": {"nodes": "cn", "path": "p", "archetype_path": "ap", "ehr_id": "ehr_id", "comp_id": "cid"},
            "search": {"nodes": "sn", "path": "p", "ehr_id": "ehr_id", "comp_id": "cid"},
        },
        "node_representation": {"path": {"mode": "reversed", "token_joiner": "."}},
        "query_engine": {"mode": "atlas_search_dual", "search_first": True, "lookup_full_composition": True, "supports_multi_predicate": True},
    },
    maturity="alpha",
    license="Apache-2.0",
)


class OpenEHRReversedPathStrategy(StrategyPlugin):
    """
    Adapter that leverages the existing CompositionFlattener to produce base/search docs.
    - On initialize, it expects a configured flattener in bindings.extras["flattener"].
    - ingest() will transform and optionally persist if the storage adapter exposes insert_one(doc, search=False).
    """

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        super().__init__(manifest)
        self.config: Dict[str, Any] = {}
        self.flattener = None

    def initialize(self, config: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> StrategyInitResult:
        self.config = config or self.manifest.default_config
        self.flattener = bindings.extras.get("flattener") if bindings else None
        notes = []
        if self.flattener:
            notes.append("Using provided CompositionFlattener from bindings.extras['flattener'].")
        else:
            notes.append("No flattener bound; transform/ingest will require one passed via bindings.extras.")
        return StrategyInitResult(config_applied=self.config, notes=notes)

    def _get_flattener(self, bindings: StrategyBindings):
        flattener = self.flattener or (bindings.extras.get("flattener") if bindings else None)
        if flattener is None:
            raise ValueError("CompositionFlattener not available; provide via bindings.extras['flattener'].")
        return flattener

    def transform(self, payload: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        flattener = self._get_flattener(bindings)
        base_doc, search_doc = flattener.transform_composition(payload)
        cfg = getattr(bindings, "extras", {}).get("runtime_config") or {}
        base_doc, search_doc, _ = remap_fields_for_config(base_doc, search_doc, cfg)
        return base_doc, search_doc

    def ingest(self, payload: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Dict[str, Any]:
        base_doc, search_doc = self.transform(payload, bindings, context)
        storage = getattr(bindings, "storage", None) if bindings else None
        if storage and hasattr(storage, "insert_one"):
            storage.insert_one(base_doc, search=False)
            if search_doc:
                try:
                    storage.insert_one(search_doc, search=True)
                except TypeError:
                    storage.insert_one(search_doc)
        return {"base": base_doc, "search": search_doc}

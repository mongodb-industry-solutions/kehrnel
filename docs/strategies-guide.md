# Adding Strategies to Kehrnel (Plugins, Activation, Portal)

This guide explains how to add a new strategy plugin, register it, and interact with it from the Healthcare Data Lab portal. The examples use the OpenEHR and FHIR strategies included in the repo. It replaces the former `kernel-usage.md` to avoid duplication.

## 1) Define the Strategy Manifest
- Create a module under `src/strategies/<protocol>/` with a `MANIFEST` (StrategyManifest) and a `StrategyPlugin` subclass.
- Declare:
  - `id`, `name`, `version`, `protocols`
  - `capabilities` (ingest/transform/search/enrich/embed/etc.)
  - `entrypoint` (`module:Class`)
  - `config_schema` (JSON Schema) + `default_config`
  - `adapters` (storage/search/vector/queue) and UI metadata
- Example (simulated FHIR skeleton): `strategies.fhir.simulated.MANIFEST`.

## 2) Implement the Plugin
- Subclass `StrategyPlugin` and override the hooks you advertise:
  - `initialize(config, bindings, context)` to validate/configure.
  - `ingest(payload, bindings, context)` to transform/persist.
  - `search(query, bindings, context)` if you expose search.
- Use shared libs to avoid duplication:
  - OpenEHR: `libs/openehr` (flattener/transformer, remap, codec helpers).
  - Storage/search adapters: bind via `StrategyBindings` (MongoStorageAdapter, OpenSearchAdapter, etc.).
- Example: `strategies.fhir.simulated` shows a no-op/echo ingest and a simulated search response.

## 3) Register the Manifest
- Add the manifest to the runtime registry in `src/app/strategy_runtime.py` (or register via API if you expose dynamic registration).
- Built-in registration currently includes: openEHR RPS dual, FHIR resource-first, FHIR simulated skeleton.

## 4) Activate a Strategy
- Use the admin API:
  - `GET /v1/strategies` — list manifests and active strategies.
  - `POST /v1/strategies/activate` with body:
    ```json
    {
      "strategy_id": "fhir.simulated_skeleton",
      "config": { "mode": "echo" }
    }
    ```
  - Activations are persisted to file when `KEHRNEL_REGISTRY_PATH` is set; restored on startup.
- Bindings are built from config:
  - Storage: from `config.target.*` (Mongo) or defaults.
  - Search: from `config.search` (e.g., `{ "backend": "opensearch", "index": "..." }`).
  - Extras: e.g., `{"flattener": CompositionFlattener}` for openEHR.

## 5) Use a Strategy via Ingest API
- Call `/v1/ingestions/body` with headers/query:
  - `strategy_id`: e.g., `openehr.rps_dual` or `fhir.resource_first`.
  - Optional `strategy_protocol`: e.g., `openEHR` or `FHIR` (for protocol-aware routing).
- The runtime routes to the strategy’s `ingest` (preferred) or `transform`.
- For openEHR, the current `CompositionFlattener` is provided via bindings; strategies can remap fields via `libs/openehr/remap`.

## 6) Portal Integration (discovery + activation)
- The portal can call `GET /v1/strategies` to populate the catalog (manifests contain tags, protocol badges, “works_with” metadata, links).
- Activation flow in portal:
  1. User selects a strategy/version → portal loads its manifest (including `config_schema`).
  2. Portal renders a config form from `config_schema` (JSON Schema UI).
  3. Portal posts to `/v1/strategies/activate` with the chosen `strategy_id` and config.
  4. Portal polls `GET /v1/strategies` to show active strategy per environment.
- For local dev, activations are file-backed when `KEHRNEL_REGISTRY_PATH` is set.

## 7) Skeleton Strategy Template (pseudo-code)
```python
from strategy_sdk import StrategyPlugin, StrategyManifest, StrategyCapability, AdapterRequirements

MANIFEST = StrategyManifest(
    id="myproto.my_strategy",
    name="My Strategy",
    version="0.1.0",
    protocols=["MyProto"],
    capabilities=[StrategyCapability.INGEST],
    entrypoint="strategies.myproto.my_strategy:MyStrategy",
    adapters=AdapterRequirements(storage=["mongo"]),
    config_schema={...},
    default_config={...},
)

class MyStrategy(StrategyPlugin):
    def __init__(self, manifest=MANIFEST):
        super().__init__(manifest)
        self.config = {}
    def initialize(self, config, bindings, context):
        self.config = config or self.manifest.default_config
        return StrategyInitResult(config_applied=self.config)
    def ingest(self, payload, bindings, context):
        storage = getattr(bindings, "storage", None)
        # transform/persist payload as needed
        if storage:
            storage.insert_one(payload)
        return {"base": payload, "search": None}
```

## 8) Tips for Strategy Authors
- Keep protocol-specific traversal/validation in shared libs to reuse across variants.
- Declare adapter needs clearly in the manifest to guide portal and runtime bindings.
- Provide UI metadata (tags, badges, works_with) to improve portal discoverability.
- Add JSON Schema defaults and examples for a smoother activation UX.
- If you need remote isolation, expose the strategy via a service URL and register it externally (future work).

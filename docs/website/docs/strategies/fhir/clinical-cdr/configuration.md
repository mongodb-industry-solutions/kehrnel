---
sidebar_position: 2
---

# FHIR Clinical CDR Configuration

Use `src/kehrnel/engine/strategies/fhir/clinical_cdr/specification/defaults.json` as the authoritative activation baseline for `fhir.clinical_cdr`.

That file is what the runtime merges into an environment when the strategy is activated. Apply small, explicit overlays for your deployment rather than copying the entire pack into client code.

## Strategy pack layout

| Path | Purpose |
|------|---------|
| `specification/manifest.json` | Strategy identity, capabilities, ops, `ui.docs` link |
| `specification/defaults.json` | Default activation config |
| `specification/schema.json` | User-facing validation for activation overrides |
| `specification/spec.json` | Machine-readable storage and index specification |
| `specification/*.json` | Sample activation / synthetic-job payloads |
| `scripts/strategy.py` | Runtime: `compile_query`, `execute_query`, `run_op` |
| `scripts/bridge.py` | fhir-gen / fhir-mql adapters, Mongo resolution |
| `scripts/generation.py`, `denormalize.py`, `indexes.py`, `query.py` | Feature modules |

Install optional FHIR libraries (vendored under `src/kehrnel/engine/domains/fhir/libs/`):

```bash
pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql
pip install -e ".[api,mongo,fhir]"
```

## Activation baseline

```json
{
  "database": "fhir_cdr",
  "schema_version": "R5",
  "collection_prefix": "",
  "collections": { "mode": "per_resource_type" },
  "search": {
    "enabled": true,
    "denormalize_on_generate": true,
    "auto_index": true,
    "config_dir": null,
    "compartment_definitions_dir": null
  },
  "generation": {
    "seed": 42,
    "use_enrichers": true,
    "watermark": { "enabled": true }
  }
}
```

### Key fields

| Field | Description |
|-------|-------------|
| `database` | Required strategy-owned MongoDB database, distinct from the environment/core database |
| `schema_version` | FHIR release (`R5` or `R6`); this is not the stored-document schema version |
| `collection_prefix` | Optional prefix on collection names (e.g. `dev_` → `dev_Patient`) |
| `collections.mode` | Must be `per_resource_type` |
| `search.enabled` | Required `true`; FHIR search is part of the strategy contract |
| `search.denormalize_on_generate` | Required `true`; generated data is projected before persistence |
| `search.auto_index` | Required `true`; configured indexes are ensured before writes |
| `search.config_dir` | Override fhir-mql resource YAML directory (null = bundled) |
| `search.compartment_definitions_dir` | Override compartment JSON directory |
| `generation.seed` | Default RNG seed for synthetic jobs |
| `generation.use_enrichers` | Enable fhir-gen enrichers |
| `generation.watermark.enabled` | Add Kehrnel synthetic meta before MongoDB save |

## Bindings

The environment binding supplies MongoDB connectivity and credentials. The
strategy-owned database is selected only by the reviewed `config.database`:

```json
{
  "strategy_id": "fhir.clinical_cdr",
  "version": "0.1.0",
  "domain": "fhir",
  "config": {
    "database": "fhir_cdr",
    "schema_version": "R5",
    "collections": { "mode": "per_resource_type" },
    "search": {
      "enabled": true,
      "denormalize_on_generate": true,
      "auto_index": true
    }
  },
  "bindings": {
    "db": {
      "provider": "mongodb",
      "uri": "mongodb://localhost:27017",
      "database": "fhir_cdr"
    }
  },
  "allow_plaintext_bindings": true
}
```

Packaged example: `src/kehrnel/engine/strategies/fhir/clinical_cdr/specification/activate_dev.json`.

For production, prefer `bindings_ref` with `KEHRNEL_BINDINGS_RESOLVER` instead
of inline URIs. A database embedded in the URI or environment metadata does not
override `config.database`. The resolver also rejects an activation whose
strategy database equals that environment database, preventing FHIR collections
from being written to the tenant's core/transversal database.

## Environment activation (HTTP)

```bash
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

curl -sS -X POST "${RUNTIME_URL}/environments/dev/activate" \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/clinical_cdr/specification/activate_dev.json
```

Confirm activation:

```bash
curl -sS "${RUNTIME_URL}/environments/dev/activations/fhir"
```

## CLI context (optional)

```bash
kehrnel setup \
  --runtime-url "$RUNTIME_URL" \
  --env dev \
  --domain fhir \
  --strategy fhir.clinical_cdr

kehrnel core env show --env dev
kehrnel strategy list --domain fhir
```

## fhir-mql config overrides

When `search.config_dir` and `search.compartment_definitions_dir` are null, the strategy uses fhir-mql defaults shipped with the installed package. Set these paths when you maintain custom resource search YAML or compartment definitions in your own repo.

After changing YAML, run **`fhir_denormalize`** for affected resource types. It
rebuilds projections, stamps new versions, and ensures indexes automatically.

## Stored-document versions

FHIR `meta.versionId` remains canonical clinical metadata. Kehrnel persistence
metadata is isolated under `_kehrnel`:

- `storage_schema_version` versions the MongoDB document shape.
- `projection_contract_version` fingerprints the complete active search and compartment contract.
- `resource_projection_version` fingerprints one resource configuration plus the shared compartment definitions, allowing targeted reprojection.
- `fhir_release`, `projected_at`, and `stored_at` provide operational context.

`_search` and `_compartments` remain top-level because the MQL and index contract
queries them directly. `_kehrnel`, both projection buckets, and MongoDB `_id` are
removed from every FHIR-facing response.

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
  "implementation_guides": { "packages": [], "active_profiles": [] },
  "index_policy": {
    "max_managed_indexes_per_collection": 63,
    "materialize_on_activation": false
  },
  "semantic": { "enabled": false, "pipelines": [] },
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
| `schema_version` | FHIR release (`R4`, `R5`, or `R6`); R4 is currently a minimal Patient/Observation tier |
| `collection_prefix` | Optional prefix on collection names (e.g. `dev_` → `dev_Patient`) |
| `collections.mode` | Must be `per_resource_type` |
| `search.enabled` | Required `true`; FHIR search is part of the strategy contract |
| `search.denormalize_on_generate` | Required `true`; generated data is projected before persistence |
| `search.auto_index` | Required `true`; configured indexes are ensured before writes |
| `search.config_dir` | Override fhir-mql resource YAML directory (null = bundled) |
| `search.compartment_definitions_dir` | Override compartment JSON directory |
| `implementation_guides.packages` | Optional local FHIR NPM packages; an empty list is normal FHIR Core mode |
| `implementation_guides.compiled_root` | Required only with packages; destination for immutable activation artifacts |
| `implementation_guides.active_profiles` | Optional canonical profile URLs selected from enabled packages; empty means no profile constraint |
| `implementation_guides.profile_validation` | Optional `disabled` or fail-closed `required` enforcement through a validation adapter |
| `index_policy.max_managed_indexes_per_collection` | Hard budget for generated indexes, excluding MongoDB `_id` |
| `index_policy.materialize_on_activation` | `false` creates indexes lazily before writes; `true` creates the full configured set at activation |
| `semantic.enabled` | Enables configured semantic projection definitions; activation itself never generates embeddings |
| `semantic.pipelines` | Optional named field-selection, chunking, model, trigger and sidecar-storage contracts |
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

## Optional implementation-guide overlays

Customers do not need an IG to start. The selected Core tier and the implemented
Kehrnel capability matrix form the default. R4 is currently minimal; R5 and R6
are package-backed. To inspect customer
constraints, configure one or more checksum-pinned package directories or
archives:

```json
{
  "implementation_guides": {
    "compiled_root": "/var/lib/kehrnel/fhir-ig-cache",
    "packages": [
      {
        "enabled": true,
        "source": "/config/fhir/packages/customer.fhir.ig-1.0.0.tgz",
        "sha256": "<64 hexadecimal characters>"
      }
    ],
    "active_profiles": [
      "https://example.org/fhir/StructureDefinition/customer-observation"
    ],
    "profile_validation": {
      "mode": "required",
      "binding": "validation_engine",
      "fail_on_warning": false
    }
  }
}
```

`source` and `compiled_root` are operator-controlled paths on the Kehrnel host
or mounted container volumes. For portal users, configure an allowlisted staging
root and quotas:

```bash
export KEHRNEL_FHIR_IG_STAGING_ROOT=/var/lib/kehrnel/fhir-ig-staging
export KEHRNEL_FHIR_IG_UPLOAD_MAX_BYTES=33554432
export KEHRNEL_FHIR_IG_STAGING_MAX_BYTES=536870912
```

Healthcare Data Lab can then upload a `.tgz` from the FHIR Configuration Center.
Kehrnel validates archive paths, file count, expanded size, package JSON, checksum,
and release compatibility before returning an activation-ready entry. Staging is
not activation: an authorized user must still review that entry in Strategy
Studio, activate the package, and voluntarily select any profiles.

Activation writes an immutable package lock, resource/profile catalog, search
plan, and review evidence. Simple FHIRPath expressions become candidate search
paths; complex expressions are marked for a reviewed override. Package discovery
does not expand the REST capability statement beyond interactions implemented by
Kehrnel.

Profile selection and enforcement are separate. The default
`profile_validation.mode: disabled` catalogs selected profiles without claiming
conformance. `mode: required` makes writes fail closed unless a configured
`validation_engine` adapter validates resources against the selected profiles.
The adapter can wrap the official HL7 validator or HAPI validator and receives a
bounded `kehrnel-validation/v1` JSON envelope containing the release, package
sources, active profiles, and resources. Kehrnel does not implement a partial
FHIRPath validator internally.

Every layer is voluntary: no packages means FHIR Core mode, an empty
`active_profiles` array means no profile constraint, and selecting profiles does
not force enforcement unless the customer chooses `mode: required`. Multiple
packages and profiles can be selected simultaneously.

## Stored-document versions

FHIR `meta.versionId` remains canonical clinical metadata. Kehrnel persistence
metadata is isolated under `_kehrnel`:

- `storage_schema_version` versions the MongoDB document shape.
- `projection_contract_version` fingerprints the complete active search and compartment contract.
- `resource_projection_version` fingerprints one resource configuration plus the shared compartment definitions, allowing targeted reprojection.
- `fhir_release`, `projected_at`, and `stored_at` provide operational context.

`_search` and `_compartments` remain top-level because the MQL and index contract
queries them directly. `_custom` and `_enrichments` are reserved for customer and
partner data and survive canonical FHIR updates. `_kehrnel`, both projection
buckets, both extension namespaces, and MongoDB `_id` are removed from every
FHIR-facing response.

See [Semantic projections](./semantic-projections.md) for the optional enrichment
configuration and its separate execution lifecycle.

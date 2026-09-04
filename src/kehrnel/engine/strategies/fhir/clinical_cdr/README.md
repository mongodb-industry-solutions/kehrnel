# FHIR Clinical CDR (`fhir.clinical_cdr`)

Strategy pack for **native FHIR R5 and R6** resources in MongoDB:

- **Generation:** **fhir-gen** (`fhir_gen`) — `src/kehrnel/engine/domains/fhir/libs/fhir-data-generation`
- **Search:** **fhir-mql** (`fhir_search_to_mql`) — `src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql`

The active resource scope is derived at runtime from the configured recipes and
the shipped fhir-mql search configs; it is not duplicated as a hard-coded UI list.

**Also:** [FHIR_TESTING.md](../../../../../../FHIR_TESTING.md) · Portal `/guide/docs/strategies/fhir/clinical-cdr`

## Pack layout

| Path | Contents |
|------|----------|
| `specification/` | `manifest.json`, `schema.json`, `defaults.json`, `recipes.json`, sample activate/job JSON |
| `scripts/` | Runtime Python (`strategy.py`, `bridge.py`, `generation.py`, …) + `spike_generate_and_search.py` |
| `strategy.py` | Entrypoint shim → `scripts/strategy.py` |
| [`engine/domains/fhir/libs/`](../../../domains/fhir/libs/README.md) | Vendored fhir-gen + fhir-mql |

## Status

| State | Ops |
|-------|-----|
| **Implemented** | `synthetic_generate_batch` (with `recipe`), `fhir_denormalize`, `fhir_ensure_indexes`, `fhir_search`, `fhir_list_search_params`, `fhir_resource_catalog`, `fhir_capabilities`, `fhir_stats`, `fhir_import_resources` |

---

## Runtime scripts (`scripts/`)

Imported by Kehrnel via `manifest.json` entrypoint `kehrnel.engine.strategies.fhir.clinical_cdr.strategy:FHIRClinicalCDRStrategy`.

| Module | Role |
|--------|------|
| `strategy.py` | `StrategyPlugin` — `run_op`, `compile_query`, `validate_config` |
| `bridge.py` | Config merge, Mongo bindings, fhir-gen / fhir-mql clients, recipe resolution |
| `generation.py` | `synthetic_generate_batch` |
| `denormalize.py` | `fhir_denormalize` |
| `indexes.py` | `fhir_ensure_indexes` |
| `query.py` | `fhir_search`, compile/execute FHIR queries |
| `resource_catalog.py` | Read-only package-backed resource structures, search projections, and index definitions |
| `stats.py` | `fhir_stats` |
| `watermark.py` | Synthetic provenance tags on save |

### Spike (no API)

```bash
# From kehrnel repo root
python src/kehrnel/engine/strategies/fhir/clinical_cdr/scripts/spike_generate_and_search.py --db fhir_kehrnel_spike
```

Hardcoded minimal corpus: 2 `Patient` + 5 `Observation`. Options: `--uri`, `--db`, `--seed`.

---

## Specification JSON (`specification/`)

JSON files define discovery, activation, and sample API payloads. Three groups:

| Group | Files | Loaded by runtime? |
|-------|-------|-------------------|
| **Pack registry** | `manifest.json`, `spec.json` | Yes — startup / pack discovery |
| **Activation config** | `defaults.json`, `schema.json`, `recipes.json` | Yes — merged on activate and ops |
| **API samples** | `activate_dev.json`, `job_generate_*.json` | No — `curl -d @file` templates only |

### How configuration merges

When an environment activates `fhir.clinical_cdr`, effective config is built in this order (later wins):

```
load_pack_defaults()     ← defaults.json + recipes.json (bridge.py)
  → manifest defaults    ← same on strategy init
  → activation config    ← POST /activate "config"
  → per-op overrides     ← e.g. job payload "seed"
```

MongoDB connectivity comes from **bindings** (`db.uri`). The logical database
comes from the reviewed activation `config.database` and overrides any database
embedded in the URI or generic environment binding.

After editing JSON under `specification/`, **restart the API** or re-activate the environment.

### Resource model catalog

FHIR base resource definitions are package assets, not tenant data. Healthcare
Data Lab reads the active catalog through Kehrnel:

```text
GET /api/domains/fhir/resource-catalog
GET /api/domains/fhir/resource-catalog/Patient
```

The list is restricted to the active Clinical CDR scope. A detail response joins
the selected R5/R6 schema definition with its fhir-mql search parameters,
denormalization projections, collection name, and declared MongoDB indexes. The
list also exposes release-compatible generation recipes and names configured
types omitted by the selected release. The response declares
`database_backed: false`; clients must not copy these standard definitions into
`user-data-models` merely to render them.

Tenant-stored FHIR conformance artifacts need a separate lifecycle. Before
removing legacy FHIR rows from `user-data-models`, classify them so custom
`StructureDefinition`, `ValueSet`, and `CodeSystem` content is not mistaken for a
generated copy of the base resource catalog.

### `manifest.json`

Strategy pack manifest: id, version, entrypoint, capabilities, ops list.

| Change | Update |
|--------|--------|
| New op | `"ops"` array + implement in `scripts/strategy.py` |
| Version bump | `"version"` (sync with activate samples) |
| UI / docs | `"ui.docs"`, `"summary"`, `"description"` |

Do not put environment Mongo URIs here — use `activate_dev.json`.

### `spec.json`

Logical model metadata (`manifest.json` → `"spec.path"`). Not used for runtime config validation.

| Field | Update when |
|-------|-------------|
| `logicalModel.generation.defaultRecipe` | Primary recipe renamed in `recipes.json` |
| `ownedSearchResourceCount` / `shippedSearchResourceCount` | fhir-mql YAML set changes |

### `defaults.json`

Default activation **config** (no resource counts).

Activation materializes the storage and index contract only. It never generates
or imports FHIR resources. Data is written only by an explicit import/API request
or an explicitly submitted `synthetic_generate_batch` job.

| Field | Default | Meaning |
|-------|---------|---------|
| `database` | `fhir_cdr` | Required strategy-owned DB, distinct from the environment DB |
| `schema_version` | `R5` | FHIR release (not the MongoDB document-schema version) |
| `collections.mode` | `per_resource_type` | Required |
| `collection_prefix` | `""` | Optional collection name prefix |
| `search.enabled` | `true` | FHIR search ops |
| `search.denormalize_on_generate` | `true` (required) | Search and compartment projection is a storage invariant |
| `search.auto_index` | `true` (required) | Resource search indexes are a storage invariant |
| `search.config_dir` | `null` | `null` = bundled fhir-mql YAML |
| `generation.seed` | `42` | Default RNG seed |
| `generation.use_enrichers` | `true` | fhir-gen enrichers |
| `generation.watermark.enabled` | `true` | Synthetic meta tags |

Example override:

```json
{
  "database": "my_fhir_db",
  "generation": { "seed": 1001, "watermark": { "enabled": false } },
  "search": { "config_dir": null, "compartment_definitions_dir": null }
}
```

Validated by `schema.json`.

### `schema.json`

JSON Schema for activation `config`. Extend when adding new config keys (and wire in `bridge.py`).

### `recipes.json`

Named generation recipes merged at load time. Reference by name in jobs:

```json
{ "recipe": "clinical_dev" }
```

| Recipe | Types | ~Docs | Sample job |
|--------|------:|------:|------------|
| `clinical_dev` | 10 | 535 | `job_generate_dev.json` |
| `clinical_full84` | 52 | 1,000+ | `job_generate_full84.json` |

Recipe shape:

```json
{
  "clinical_dev": {
    "description": "Label",
    "resources": { "Patient": 50, "Observation": 200 },
    "scenarios": ["Patient:deceased_datetime"],
    "seed": 42
  }
}
```

Job `resources` **overrides** per-type counts without editing `recipes.json`. Resource types must exist in fhir-gen; prefer types with fhir-mql YAML for search.

### API sample files

| File | Purpose |
|------|---------|
| `activate_dev.json` | Sample `POST /environments/{env}/activate` — edit `config.database`, `bindings.db.uri` |
| `job_generate_small.json` | Minimal: 2 Patient + 5 Observation (CI / spike) |
| `job_generate_dev.json` | `recipe: clinical_dev` |
| `job_generate_full84.json` | `recipe: clinical_full84` soak/regression |

Activate and run (API on `:8080`):

```bash
curl -X POST http://localhost:8080/environments/dev/activate \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/clinical_cdr/specification/activate_dev.json

curl -X POST http://localhost:8080/environments/dev/synthetic/jobs \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/clinical_cdr/specification/job_generate_dev.json
```

### Quick config workflows

**Default database:** Edit `defaults.json` + `activate_dev.json` bindings → restart / re-activate.

**Corpus size:** Edit `recipes.json`, or job `payload.resources`, or POST `recipe` / `resources` directly.

**Projection and indexes:** Always applied before imported or generated resources become visible. They cannot be disabled by activation or job payload.

**Validate:**

```powershell
pytest tests/contract/clinical_cdr/test_generation.py -k recipe -q
$env:FHIR_CONTRACT_MONGO = "1"
pytest tests/contract/clinical_cdr -q
```

### `synthetic_generate_batch` payload

| Field | Required | Description |
|-------|----------|-------------|
| `recipe` | no* | Name from `recipes.json` |
| `resources` / `resource_counts` | no* | Overrides recipe counts |
| `seed` | no | Overrides `generation.seed` |
| `plan_only` / `dry_run` | no | Plan or in-memory only |

\* One of `recipe`, `resources`, or `resource_counts` required.

`fhir_denormalize` is now a repair/reprojection operation. It always rebuilds `_search` and `_compartments`, stamps current projection versions, and ensures the configured indexes.

### Golden search tests

[`tests/contract/clinical_cdr/fixtures/fhir_golden_queries.json`](../../../../../../tests/contract/clinical_cdr/fixtures/fhir_golden_queries.json)

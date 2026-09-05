# FHIR Clinical CDR (`fhir.clinical_cdr`)

Strategy pack for native FHIR resources in MongoDB: a deliberately minimal R4
Patient/Observation tier plus package-backed R5 and R6 support.

- **Generation:** **fhir-gen** (`fhir_gen`) — `src/kehrnel/engine/domains/fhir/libs/fhir-data-generation`
- **Search:** **fhir-mql** (`fhir_search_to_mql`) — `src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql`

The active capability sets are derived independently from the selected release
schema, shipped fhir-mql search configs, fhir-gen schemas, and example recipes;
recipes never act as the write allowlist. `generatable_resource_types` means
preview/inspection support; `synthetic_writable_resource_types` is the stricter
intersection that can be denormalized, indexed, and persisted. The capability
response also reports their difference as `generation_only_resource_types`.

**Also:** [FHIR_TESTING.md](../../../../../../FHIR_TESTING.md) · Portal `/guide/docs/strategies/fhir/clinical-cdr`

## Pack layout

| Path | Contents |
|------|----------|
| `specification/` | Manifest/config, flat recipes, versioned cohort blueprints and their JSON Schema, and sample payloads |
| `scripts/` | Runtime Python (`strategy.py`, `bridge.py`, `generation.py`, …) + `spike_generate_and_search.py` |
| `strategy.py` | Entrypoint shim → `scripts/strategy.py` |
| [`engine/domains/fhir/libs/`](../../../domains/fhir/libs/README.md) | Vendored fhir-gen + fhir-mql |

## Status

| State | Ops |
|-------|-----|
| **Implemented** | `synthetic_generate_batch` (R5/R6 flat recipes and patient-centred cohorts), `fhir_cohort_catalog`, `fhir_cohort_plan`, `fhir_denormalize`, `fhir_ensure_indexes`, `fhir_index_manifest`, `fhir_search`, `fhir_list_search_params`, `fhir_resource_catalog`, `fhir_capabilities`, `fhir_support_matrix`, `fhir_stats`, `fhir_import_resources`, checkpointed `fhir_migration_*` operations, `fhir_reference_integrity`, `fhir_compile_implementation_guides`, `fhir_semantic_preview`, `fhir_semantic_materialize`, `fhir_semantic_search` |

---

## Runtime scripts (`scripts/`)

Imported by Kehrnel via `manifest.json` entrypoint `kehrnel.engine.strategies.fhir.clinical_cdr.strategy:FHIRClinicalCDRStrategy`.

| Module | Role |
|--------|------|
| `strategy.py` | `StrategyPlugin` — `run_op`, `compile_query`, `validate_config` |
| `bridge.py` | Config merge, Mongo bindings, fhir-gen / fhir-mql clients, recipe resolution |
| `generation.py` | Flat and cohort execution through `synthetic_generate_batch` |
| `cohort_blueprints.py` | Catalog, contract validation, deterministic planning, patient graphs, clinical rules, and measured quality evidence |
| `denormalize.py` | `fhir_denormalize` |
| `indexes.py` | `fhir_ensure_indexes` |
| `index_manifest.py` | Deterministic index inventory, digest, and per-collection budget |
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
| **Cohort assets** | `cohort_blueprints.json`, `cohort_blueprint.schema.json` | Yes — read and validated by cohort catalog/plan/generation |
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

The list covers the selected release schema and marks each resource's separate
storage, search, generation, and recipe capabilities. A detail response joins
the selected release definition with its reviewed fhir-mql search parameters,
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
| `schema_version` | `R5` | FHIR release (`R4`, `R5`, or `R6`; R4 is currently minimal) |
| `collections.mode` | `per_resource_type` | Required |
| `collection_prefix` | `""` | Optional collection name prefix |
| `search.enabled` | `true` | FHIR search ops |
| `search.denormalize_on_generate` | `true` (required) | Search and compartment projection is a storage invariant |
| `search.auto_index` | `true` (required) | Resource search indexes are a storage invariant |
| `search.config_dir` | `null` | `null` = bundled fhir-mql YAML |
| `implementation_guides.packages` | `[]` | Optional local package overlays; empty means FHIR Core mode |
| `implementation_guides.active_profiles` | `[]` | Optional selected profile canonical URLs; empty means unconstrained |
| `implementation_guides.profile_validation` | `disabled` | Optional fail-closed enforcement using a configured external validator |
| `semantic.enabled` | `false` | Opt into semantic pipeline declarations; activation never generates vectors |
| `semantic.pipelines` | `[]` | Named FHIR field-selection and chunking contracts |
| `index_policy.materialize_on_activation` | `false` | Create indexes lazily before the first write unless explicitly eager |
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
Named recipes are filtered to the types present in the selected release and the
result reports `omitted_recipe_resource_types`. An explicitly requested unknown
type still fails; it is never silently discarded.

### Patient-centred cohort blueprints

Use a bundled blueprint or submit an inline blueprint that satisfies
`cohort_blueprint.schema.json`. The workflow is discover → plan → preview →
asynchronous generation:

```text
GET  /api/domains/fhir/synthetic/cohorts
POST /api/domains/fhir/synthetic/cohorts/plan
POST /api/domains/fhir/synthetic/cohorts/preview
POST /environments/{env}/synthetic/jobs
```

Bundled assets cover cardiometabolic monitoring, an oncology care pathway, and
a payer/claims journey. Plans are deterministic for the blueprint version,
overrides, reference date, and seed. Execution returns count, reference,
patient-linkage, clinical-rule, and base-schema evidence. `curated-demo` is an
explicit demonstration-data maturity level, not an epidemiological or clinical
validity claim. See the portal page **Synthetic cohorts and the Healthcare Data
Lab journey** for the complete frontend contract.

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
| `cohort.blueprint_id` | no* | Bundled patient-centred cohort asset |
| `cohort.blueprint` | no* | Inline customer blueprint using `fhir-cohort-blueprint/v1` |
| `cohort.patients` | no | Patient count, bounded to 10,000 (preview is capped at 10) |
| `cohort.history_years` / `reference_date` | no | Deterministic longitudinal window |
| `cohort.population` | no | Replacement age-band and gender distributions |
| `cohort.per_patient_resources` | no | Per-type min/max/probability overrides |
| `cohort.shared_resources` | no | Shared directory/catalog resource-count overrides |
| `cohort.clinical_rules` | no | Replacement supported-rule configuration |
| `include_sample` / `sample_limit` | no | Include bounded canonical examples in a dry run or preview |

\* One of `recipe`, `resources`, `resource_counts`, or `cohort` required.

`fhir_denormalize` is now a repair/reprojection operation. It always rebuilds `_search` and `_compartments`, stamps current projection versions, and ensures the configured indexes.

### Golden search tests

[`tests/contract/clinical_cdr/fixtures/fhir_golden_queries.json`](../../../../../../tests/contract/clinical_cdr/fixtures/fhir_golden_queries.json)

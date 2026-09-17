---
sidebar_position: 3
---

# FHIR Resource Store CLI Workflows

End-to-end path for `fhir.resource_store` on a local Kehrnel runtime.

1. Start Kehrnel and install the `[fhir]` extra
2. Activate `fhir.resource_store` on an environment
3. Import a resource, Bundle, or NDJSON file, or generate synthetic data
4. Denormalize and ensure indexes (if not inline)
5. Search via domain API or universal `run`
6. Inspect stats and search parameters

Packaged specification and API samples live under `src/kehrnel/engine/strategies/fhir/resource_store/specification/` in the kehrnel repository.

## Prerequisites

```bash
./startKehrnel
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"
export KEHRNEL_AUTH_ENABLED=false   # local dev only
```

MongoDB (default `mongodb://localhost:27017`) must match activation bindings.

Configure the reusable CLI context once:

```bash
kehrnel setup \
  --runtime-url "$RUNTIME_URL" \
  --env dev \
  --domain fhir \
  --strategy fhir.resource_store \
  --non-interactive
```

## Dedicated FHIR CLI

`kehrnel fhir` is the supported command surface for the normal resource-store
journey. It calls the same tenant-scoped Kehrnel endpoints as Healthcare Data
Lab and never bypasses activation, validation, projection, or index policy.

```bash
kehrnel fhir metadata --env dev
kehrnel fhir capabilities --env dev
kehrnel fhir support-matrix --env dev --format markdown --out fhir-support-matrix.md
kehrnel fhir resource-catalog Patient --env dev
kehrnel fhir search-parameters Observation --env dev
kehrnel fhir index-manifest --env dev
kehrnel fhir stats --env dev --summary-only
```

Run an interoperable search, inspect the generated MQL, and then collect real
MongoDB index evidence without returning clinical resources in the diagnostic
response:

```bash
kehrnel fhir search 'Observation?status=final&date=ge2025-01-01' \
  --env dev --count 50

kehrnel fhir explain 'Observation?status=final&date=ge2025-01-01' --env dev
kehrnel fhir explain 'Observation?status=final&date=ge2025-01-01' \
  --env dev --execution-stats
```

`support-matrix`, `resource-catalog`, `search-parameters`, and
`index-manifest` are runtime-derived evidence. Do not replace them with a fixed
resource count in customer material.

Optional spike (libraries only, no strategy):

```bash
python src/kehrnel/engine/strategies/fhir/resource_store/scripts/spike_generate_and_search.py --db fhir_kehrnel_spike
```

## 1. Activate the strategy

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/activate" \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/resource_store/specification/activate_dev.json
```

The equivalent CLI activation keeps the database, release, search policy, IGs,
profiles, validation adapter, and terminology mode in a reviewed config file:

```bash
kehrnel core env activate \
  --env dev \
  --domain fhir \
  --strategy fhir.resource_store \
  --config ./fhir-activation.yaml \
  --bindings-ref tenant-fhir-mongodb
```

### Stage an IG and select profiles voluntarily

Start in FHIR Core mode when the customer has no IG. To add a jurisdictional,
customer, or project package, first stage the original NPM archive:

```bash
export KEHRNEL_FHIR_IG_STAGING_ROOT=/var/lib/kehrnel/fhir-ig-staging

kehrnel fhir stage-ig ./customer.fhir.ig-1.0.0.tgz \
  --env dev \
  --release R5 \
  --out ./customer-ig-stage.json
```

`--release` lets the package be staged while the activation is still a draft.
If the FHIR strategy is already active, the active release is authoritative and
the option may be omitted. The runtime validates the archive, release
compatibility, safe paths, quotas, and checksum. The output contains its catalog
and an `activation_patch`; staging does not activate the package or select
profiles. Review the evidence and merge the returned patch into the activation
config. Select zero, one, or multiple profile canonical URLs:

```yaml
database: customer_fhir
schema_version: R5
implementation_guides:
  compiled_root: /var/lib/kehrnel/fhir-ig-staging/dev/compiled
  packages:
    - enabled: true
      source: /var/lib/kehrnel/fhir-ig-staging/dev/<sha256>.tgz
      sha256: <sha256>
  active_profiles:
    - https://example.org/fhir/StructureDefinition/customer-observation
  profile_validation:
    mode: required
    binding: validation_engine
    fail_on_warning: false
```

Activate the reviewed file with `kehrnel core env activate`. Package ingestion
extends the catalog, search candidates, terminology plan, and profile evidence;
it does not claim interactions that Kehrnel has not implemented. Selecting a
profile catalogs the constraint. `profile_validation.mode: required` is the
separate fail-closed switch and requires a configured validation adapter.

## 2. Import through the universal CLI

The strategy's advertised `ingest` capability is backed by the same bounded,
validated, projected write pipeline as the FHIR REST import endpoint. A JSON
file can contain one resource, a resource array, or a Bundle; NDJSON is expanded
to a resource batch by the CLI.

```bash
kehrnel run ingest \
  --env dev \
  --domain fhir \
  --strategy fhir.resource_store \
  --set file_path=./patient-bundle.json \
  --set dry_run=true
```

Remove `dry_run=true` only after reviewing the report. Import is never triggered
by strategy activation.

For the normal path, the dedicated command preserves the original JSON/NDJSON
bytes and is a dry run unless `--execute` is explicit:

```bash
kehrnel fhir import-data ./patient-bundle.json --env dev
kehrnel fhir import-data ./patient-bundle.json --env dev --execute
kehrnel fhir import-data ./export.ndjson --env dev --mode upsert --execute
```

The HTTP import boundary accepts the same practical shapes directly:

```bash
# Individual resource or collection/searchset Bundle JSON
curl -sS -X POST \
  -H 'Content-Type: application/fhir+json' \
  -H 'x-active-env: dev' \
  --data-binary @patient-bundle.json \
  "${RUNTIME_URL}/api/domains/fhir/import?validation_level=base&mode=create&dry_run=true"

# NDJSON
curl -sS -X POST \
  -H 'Content-Type: application/fhir+ndjson' \
  -H 'x-active-env: dev' \
  --data-binary @export.ndjson \
  "${RUNTIME_URL}/api/domains/fhir/import?validation_level=base&mode=create&dry_run=true"
```

`transaction` and `batch` Bundles are rejected by bulk import because their
`entry.request` instructions are executable FHIR interactions. Kehrnel does not
silently flatten those semantics. Use a collection/searchset Bundle or issue the
supported interactions through the FHIR API.

The bounded import validates the full request before persistence, but it is not
a FHIR transaction across resource collections. Retain the report and use stable
logical IDs so a database conflict can be investigated and retried safely.

## 3. Resumable migration runs

For a real corpus, create a tenant-scoped run and stream bounded chunks. Kehrnel
stores only metadata, content digests, checkpoints, and bounded reports in the
FHIR strategy database. It does **not** copy the source payload into the core job
database.

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/fhir/migration/runs" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "source_name": "export.ndjson",
    "source_format": "ndjson",
    "total_resources": 2000,
    "total_chunks": 4,
    "chunk_size": 500,
    "validation_level": "base",
    "mode": "upsert"
  }'
```

Send chunks in order, setting `final=true` only on the declared final chunk:

```bash
curl -sS -X POST \
  "${RUNTIME_URL}/api/domains/fhir/migration/runs/RUN_ID/chunks/0?final=false" \
  -H "Content-Type: application/fhir+ndjson" \
  -H "x-active-env: dev" \
  --data-binary @export.part-000.ndjson
```

An exact retry of a completed chunk returns its stored report without writing
again. Different content at the same chunk index fails with a conflict. Inspect
or cancel the checkpoint explicitly:

```bash
curl -sS -H "x-active-env: dev" \
  "${RUNTIME_URL}/api/domains/fhir/migration/runs/RUN_ID"

curl -sS -X POST -H "x-active-env: dev" \
  "${RUNTIME_URL}/api/domains/fhir/migration/runs/RUN_ID/cancel"
```

After import, run the informational reference-integrity report. Missing
references are evidence for migration decisions; they do not mutate or delete
resources.

```bash
curl -sS -X POST \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{}' \
  "${RUNTIME_URL}/api/domains/fhir/migration/runs/RUN_ID/reference-integrity"
```

Healthcare Data Lab presents bounded imports as **Import FHIR Data** in Data
Factory. Advanced/resumable mode performs this chunking and reporting while
keeping the selected source file in the browser.

## 4. Patient-centred cohort generation

Discover the backend catalog and review an exact plan before writing anything:

```bash
curl -sS -H "x-active-env: dev" \
  "${RUNTIME_URL}/api/domains/fhir/synthetic/cohorts"

curl -sS -X POST \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "blueprint_id": "cardiometabolic-monitoring",
    "patients": 100,
    "history_years": 4,
    "seed": 7812
  }' \
  "${RUNTIME_URL}/api/domains/fhir/synthetic/cohorts/plan"
```

Generate the reviewed plan through the asynchronous job API:

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/synthetic/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "fhir",
    "op": "synthetic_generate_batch",
    "payload": {
      "cohort": {
        "blueprint_id": "cardiometabolic-monitoring",
        "patients": 100,
        "history_years": 4,
        "seed": 7812
      },
      "store_canonical": true
    }
  }'
```

See [Synthetic cohorts](./synthetic-cohorts.md) for the blueprint, preview,
quality-evidence, and Healthcare Data Lab contracts.

## 5. Advanced flat generation job

Small batch (`src/kehrnel/engine/strategies/fhir/resource_store/specification/job_generate_small.json`):

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/synthetic/jobs" \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/resource_store/specification/job_generate_small.json
```

Larger scheduling-oriented batch:

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/synthetic/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "fhir",
    "op": "synthetic_generate_batch",
    "payload": {
      "seed": 42,
      "resources": {
        "Patient": 50,
        "Schedule": 3,
        "Slot": 100,
        "Appointment": 40
      },
      "store_canonical": true
    }
  }'
```

Poll until `status` is `completed` (replace `JOB_ID`):

```bash
curl -sS "${RUNTIME_URL}/environments/dev/synthetic/jobs/JOB_ID"
```

### Notable `synthetic_generate_batch` fields

| Field | Description |
|-------|-------------|
| `resources` / `resource_counts` | ResourceType → count |
| `cohort` | Patient-centred blueprint id and reviewed overrides |
| `seed` | Overrides `generation.seed` |
| `scenarios` | fhir-gen scenario tags (e.g. `Patient:deceased_datetime`) |
| `dry_run` / `plan_only` | Plan or generate in memory only |
| `store_canonical` | Write canonical JSON to MongoDB (default true) |
| `include_sample` / `sample_limit` | Return a bounded canonical sample, primarily for dry-run preview |

Stored output is always validated, projected into `_search` and `_compartments`,
version-stamped, and indexed. There is no persistence opt-out for those steps.

## 6. Bounded native MongoDB reads

FHIR Search remains the interoperable API. For implementation work, the
accelerator also exposes a constrained, read-only MongoDB `find` operation over
one configured resource collection:

```bash
curl -sS -X POST \
  -H 'Content-Type: application/json' \
  -H 'x-active-env: dev' \
  -d '{
    "resource_type": "Observation",
    "filter": {"status": "final", "valueQuantity.value": {"$gte": 5}},
    "projection": {"id": 1, "status": 1, "valueQuantity": 1},
    "sort": {"effectiveDateTime": -1},
    "limit": 25
  }' \
  "${RUNTIME_URL}/api/domains/fhir/native-query"
```

This operational endpoint rejects writes, aggregation pipelines, executable
operators, cross-collection access, deep expressions, and result sets above 200
rows. Set `include_operational: true` only when you deliberately need to inspect
the `_search`, `_compartments`, or `_kehrnel` projections.

## 7. Maintenance ops (`/run`)

Denormalize Patient documents:

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "fhir",
    "operation": "op",
    "payload": {
      "op": "fhir_denormalize",
      "payload": {
        "resource_types": ["Patient"],
        "batch_size": 500
      }
    }
  }'
```

Ensure indexes after denormalize:

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "fhir",
    "operation": "op",
    "payload": {
      "op": "fhir_ensure_indexes",
      "payload": { "resource_types": ["Patient"] }
    }
  }'
```

Compile + execute search (`fhir_search` op):

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "fhir",
    "operation": "op",
    "payload": {
      "op": "fhir_search",
      "payload": {
        "resource_type": "Patient",
        "criteria": { "family": "Smith" },
        "_count": 20
      }
    }
  }'
```

List supported search parameters:

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "fhir",
    "operation": "op",
    "payload": {
      "op": "fhir_list_search_params",
      "payload": { "resource_type": "Patient" }
    }
  }'
```

Database diagnostics:

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "fhir",
    "operation": "op",
    "payload": { "op": "fhir_stats", "payload": {} }
  }'
```

## 8. Universal query API

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "fhir",
    "operation": "query",
    "payload": {
      "query": {
        "resource_type": "Slot",
        "criteria": { "status": "free" },
        "_count": 10
      }
    }
  }'
```

Note: `compile_query` / `execute_query` expect search input under a `query` object when using the universal runner.

## 9. FHIR domain search (Bundle)

Requires active `fhir.resource_store` on the environment. Pass `x-active-env` (or configured default):

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/fhir/search" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "resource_type": "Patient",
    "criteria": { "family": "Smith" },
    "limit": 20
  }'
```

Optional FHIR search URL string:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/fhir/search" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "resource_type": "Patient",
    "fhir_search": "family=Smith&given=John",
    "limit": 10
  }'
```

## 10. Inspect the active contract

```bash
kehrnel fhir capabilities --env dev
kehrnel fhir resource-catalog --env dev
kehrnel fhir index-manifest --env dev
kehrnel fhir support-matrix --env dev --format markdown --out fhir-support-matrix.md
```

The domain endpoint can download the same runtime-derived evidence as Markdown:

```bash
curl -sS -H "x-active-env: dev" \
  "${RUNTIME_URL}/api/domains/fhir/support-matrix?format=markdown" \
  -o fhir-support-matrix.md
```

For an R4 activation these commands explicitly report the provisional minimal
tier. Patient and Observation are available with structural validation and a
reviewed search subset; synthetic generation remains unavailable.

## 11. Preview a semantic projection

After activating an opt-in semantic pipeline:

```bash
kehrnel core env run fhir_semantic_preview \
  --env dev --domain fhir \
  --payload semantic-preview.json
```

The same payload can be sent to
`POST /api/domains/fhir/semantic/preview` with `x-active-env: dev`. Preview is
read-only and never invokes the configured embedding provider.

## 12. Use the vendored libraries directly

The strategy is the supported operational boundary, but both libraries remain
useful as embeddable accelerators in customer or partner code. Install them from
the Kehrnel checkout:

```bash
pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation
pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql
```

Generate release-specific canonical resources without a database:

```python
from fhir_gen import ResourceGenerator

generator = ResourceGenerator(seed=42, schema_version="R5")
patients = generator.generate("Patient", count=5)
observations = generator.generate("Observation", count=20)
evidence = generator.conformance_report()
```

Create the mandatory search projections and compile FHIR search into MQL:

```python
from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer

denormalizer = ResourceDenormalizer()  # bundled resource YAML by default
stored_patient = denormalizer.denormalize(patients[0])

converter = FHIRSearchConverter()
plan = converter.convert_fhir_search(
    "Observation?status=final&date=ge2025-01-01&_sort=-date"
)
```

The library CLIs expose the same lower-level building blocks:

```bash
fhir-gen --schema-version R5 --seed 42 generate Patient --count 5 --no-save
fhir-mql resources --format json
fhir-mql convert --fhir-search 'Patient?name=Smith&birthdate=ge1970-01-01'
```

Direct library consumers own persistence boundaries, response projection,
authorization, package/profile locks, validation adapters, and index lifecycle.
Use Kehrnel instead when those controls must be consistent across a tenant.

## 13. Contract tests (developers)

```bash
pytest tests/contract/resource_store -v
```

Set `FHIR_CONTRACT_MONGO=1` to force Mongo-backed execute tests when a local instance is available.

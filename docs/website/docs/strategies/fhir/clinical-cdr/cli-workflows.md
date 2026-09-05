---
sidebar_position: 3
---

# FHIR Clinical CDR CLI Workflows

End-to-end path for `fhir.clinical_cdr` on a local Kehrnel runtime.

1. Start Kehrnel and install the `[fhir]` extra
2. Activate `fhir.clinical_cdr` on an environment
3. Import a resource, Bundle, or NDJSON file, or generate synthetic data
4. Denormalize and ensure indexes (if not inline)
5. Search via domain API or universal `run`
6. Inspect stats and search parameters

Packaged specification and API samples live under `src/kehrnel/engine/strategies/fhir/clinical_cdr/specification/` in the kehrnel repository.

## Prerequisites

```bash
./startKehrnel
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"
export KEHRNEL_AUTH_ENABLED=false   # local dev only
```

MongoDB (default `mongodb://localhost:27017`) must match activation bindings.

Optional spike (libraries only, no strategy):

```bash
python src/kehrnel/engine/strategies/fhir/clinical_cdr/scripts/spike_generate_and_search.py --db fhir_kehrnel_spike
```

## 1. Activate the strategy

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/activate" \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/clinical_cdr/specification/activate_dev.json
```

## 2. Import through the universal CLI

The strategy's advertised `ingest` capability is backed by the same bounded,
validated, projected write pipeline as the FHIR REST import endpoint. A JSON
file can contain one resource, a resource array, or a Bundle; NDJSON is expanded
to a resource batch by the CLI.

```bash
kehrnel run ingest \
  --env dev \
  --domain fhir \
  --strategy fhir.clinical_cdr \
  --set file_path=./patient-bundle.json \
  --set dry_run=true
```

Remove `dry_run=true` only after reviewing the report. Import is never triggered
by strategy activation.

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

Healthcare Data Lab performs this chunking and reporting directly from the
Migration Workbench, keeping the selected file in the browser.

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

Small batch (`src/kehrnel/engine/strategies/fhir/clinical_cdr/specification/job_generate_small.json`):

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/synthetic/jobs" \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/clinical_cdr/specification/job_generate_small.json
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

## 6. Maintenance ops (`/run`)

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

## 6. Universal query API

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

## 7. FHIR domain search (Bundle)

Requires active `fhir.clinical_cdr` on the environment. Pass `x-active-env` (or configured default):

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

## 8. Inspect the active contract

```bash
kehrnel core env run fhir_capabilities --env dev --domain fhir
kehrnel core env run fhir_resource_catalog --env dev --domain fhir
kehrnel core env run fhir_index_manifest --env dev --domain fhir
kehrnel core env run fhir_support_matrix --env dev --domain fhir
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

## 9. Preview a semantic projection

After activating an opt-in semantic pipeline:

```bash
kehrnel core env run fhir_semantic_preview \
  --env dev --domain fhir \
  --payload semantic-preview.json
```

The same payload can be sent to
`POST /api/domains/fhir/semantic/preview` with `x-active-env: dev`. Preview is
read-only and never invokes the configured embedding provider.

## 10. Contract tests (developers)

```bash
pytest tests/contract/clinical_cdr -v
```

Set `FHIR_CONTRACT_MONGO=1` to force Mongo-backed execute tests when a local instance is available.

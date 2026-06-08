---
sidebar_position: 3
---

# FHIR Clinical CDR CLI Workflows

End-to-end path for `fhir.clinical_cdr` on a local Kehrnel runtime.

1. Start Kehrnel and install the `[fhir]` extra
2. Activate `fhir.clinical_cdr` on an environment
3. Start a synthetic generation job
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

## 2. Synthetic generation job

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
      "denormalize_after": true,
      "denormalize_resource_types": ["Patient", "Schedule", "Slot", "Appointment"]
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
| `seed` | Overrides `generation.seed` |
| `scenarios` | fhir-gen scenario tags (e.g. `Patient:deceased_datetime`) |
| `denormalize_after` | Inline `fhir_denormalize` after save |
| `denormalize_resource_types` | Subset for denormalize |
| `dry_run` / `plan_only` | Plan or generate in memory only |
| `store_canonical` | Write canonical JSON to MongoDB (default true) |

## 3. Maintenance ops (`/run`)

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

## 4. Universal query API

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

## 5. FHIR domain search (Bundle)

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

## 6. Contract tests (developers)

```bash
pytest tests/contract/clinical_cdr -v
```

Set `FHIR_CONTRACT_MONGO=1` to force Mongo-backed execute tests when a local instance is available.

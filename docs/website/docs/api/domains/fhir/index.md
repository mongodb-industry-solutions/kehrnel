---
sidebar_position: 4
---

# Domain API: FHIR

The FHIR domain exposes HTTP operations backed by the active FHIR strategy on an environment. Production workflows should use **`fhir.clinical_cdr`**.

## Strategy documentation

Full configuration, data model, and CLI workflows:

- [FHIR Clinical CDR strategy](/docs/strategies/fhir/clinical-cdr/)

## Search

`POST /api/domains/fhir/search` executes FHIR search against MongoDB via the active strategy’s compile/execute path and returns a **FHIR Bundle** (`searchset`).

Requirements:

- Environment resolved from `x-active-env` (or platform default)
- Active activation with `strategy_id: fhir.clinical_cdr` and `domain: fhir`
- Resources denormalized for the requested parameters (`fhir_denormalize`)

### Request body

| Field | Type | Description |
|-------|------|-------------|
| `resource_type` | string | FHIR resource type (e.g. `Patient`) |
| `criteria` | object | Search parameter map (`family`, `status`, …) |
| `fhir_search` | string | Optional raw query string (`family=Smith&given=John`) |
| `limit` | integer | Page size (maps to `_count`) |
| `offset` | integer | Skip (maps to `_offset`) |

### Example

```bash
curl -sS -X POST "http://localhost:8080/api/domains/fhir/search" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "resource_type": "Patient",
    "criteria": { "family": "Smith" },
    "limit": 20
  }'
```

Configure the default strategy with `KEHRNEL_FHIR_STRATEGY_ID` (default `fhir.clinical_cdr`).

## Platform operations

Generation, denormalization, indexes, and diagnostics use the universal runtime and synthetic jobs APIs with `domain: fhir`. See [CLI workflows](/docs/strategies/fhir/clinical-cdr/cli-workflows).

| API | Purpose |
|-----|---------|
| `POST /environments/{env}/activate` | Bind `fhir.clinical_cdr` + MongoDB |
| `POST /environments/{env}/synthetic/jobs` | Async `synthetic_generate_batch` |
| `POST /environments/{env}/run` | `fhir_denormalize`, `fhir_search`, `fhir_stats`, … |

## Discovery and enrichment

| API | Purpose |
|-----|---------|
| `GET /api/domains/fhir/metadata` | Standard CapabilityStatement for the activated release and implemented interactions |
| `GET /api/domains/fhir/capabilities` | Detailed release tier, resource sets, IGs, active profiles, validation and semantic execution status |
| `GET /api/domains/fhir/support-matrix` | JSON support matrix generated from runtime capabilities; use `?format=markdown` to download evidence |
| `GET /api/domains/fhir/resource-catalog` | Package-backed resource and MongoDB model catalog |
| `GET /api/domains/fhir/synthetic/cohorts` | Versioned patient-centred cohort blueprint catalog |
| `POST /api/domains/fhir/synthetic/cohorts/plan` | Deterministic resource-distribution plan; never writes data |
| `POST /api/domains/fhir/synthetic/cohorts/preview` | Bounded, non-persistent sample and quality evidence |
| `POST /api/domains/fhir/implementation-guides/stage` | Validate and stage a bounded `.tgz` for later voluntary activation |
| `POST /api/domains/fhir/import` | Validate and optionally import Bundle, NDJSON or a resource |
| `POST /api/domains/fhir/migration/runs` | Start a resumable tenant-scoped migration without retaining the source payload |
| `GET /api/domains/fhir/migration/runs` | List persisted migration history and checkpoints |
| `POST /api/domains/fhir/migration/runs/{run}/chunks/{index}` | Import one bounded, ordered, idempotently replayable chunk |
| `POST /api/domains/fhir/migration/runs/{run}/cancel` | Cooperatively cancel before or during a chunk boundary |
| `POST /api/domains/fhir/migration/runs/{run}/reference-integrity` | Report unresolved relative references without modifying resources |
| `POST /api/domains/fhir/semantic/preview` | Preview configured field extraction and chunking; never embeds or writes |
| `POST /api/domains/fhir/semantic/materialize` | Explicitly embed selected stored resources and persist rebuildable sidecar chunks |
| `POST /api/domains/fhir/semantic/search` | Embed a query and execute Atlas Vector Search over configured sidecars |

Every operation is also available through `POST /environments/{env}/run` and
the CLI `kehrnel core env run ...`. Configuration is supplied once during
strategy activation; credentials remain in environment bindings.

Migration history is stored in `_kehrnel_fhir_migration_runs` and
`_kehrnel_fhir_migration_chunks` inside the activated strategy database. These
collections contain audit metadata, digests, checkpoints, and bounded reports—not
the imported clinical source documents. Source bytes are streamed by the client
and remain subject to the caller's retention policy.

## Maturity

The FHIR resource-store accelerator is delivered as `fhir.clinical_cdr`. R4 is
currently a minimal Patient/Observation tier; R5 and R6 are package-backed. Its
authoritative scope is the active CapabilityStatement and detailed capability
response. Semantic execution is advertised only when its embedding, storage,
index, and Atlas adapters are actually available.

`fhir.clinical_cdr` is the only FHIR strategy in kehrnel.

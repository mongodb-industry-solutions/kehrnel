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

## Maturity

The FHIR resource-store accelerator is delivered as `fhir.clinical_cdr`. Its authoritative scope is the active CapabilityStatement and strategy manifest; it does not advertise natural-language search or agentic operations.

`fhir.clinical_cdr` is the only FHIR strategy in kehrnel.

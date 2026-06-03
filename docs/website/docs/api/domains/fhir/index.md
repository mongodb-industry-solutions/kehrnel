---
sidebar_position: 4
---

# Domain API: FHIR

The FHIR domain exposes HTTP operations backed by the active FHIR strategy on an environment. Production workflows should use **`fhir.rps_canonical`**.

## Strategy documentation

Full configuration, data model, and CLI workflows:

- [FHIR RPS Canonical strategy](/docs/strategies/fhir/rps-canonical/)

## Search

`POST /api/domains/fhir/search` executes FHIR search against MongoDB via the active strategy’s compile/execute path and returns a **FHIR Bundle** (`searchset`).

Requirements:

- Environment resolved from `x-active-env` (or platform default)
- Active activation with `strategy_id: fhir.rps_canonical` and `domain: fhir`
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

Configure the default strategy with `KEHRNEL_FHIR_STRATEGY_ID` (default `fhir.rps_canonical`).

## Platform operations

Generation, denormalization, indexes, and diagnostics use the universal runtime and synthetic jobs APIs with `domain: fhir`. See [CLI workflows](/docs/strategies/fhir/rps-canonical/cli-workflows).

| API | Purpose |
|-----|---------|
| `POST /environments/{env}/activate` | Bind `fhir.rps_canonical` + MongoDB |
| `POST /environments/{env}/synthetic/jobs` | Async `synthetic_generate_batch` |
| `POST /environments/{env}/run` | `fhir_denormalize`, `fhir_search`, `fhir_stats`, … |

## Maturity

Domain search and strategy-backed generation are **beta** (`fhir.rps_canonical` v0.1.0). Natural-language search (`negotiate_fhir_search`) and agentic endpoints are not yet production-ready.

Legacy preview strategy `fhir.resource_first` remains for compatibility; do not use it for new integrations.

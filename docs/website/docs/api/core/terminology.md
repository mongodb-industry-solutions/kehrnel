---
sidebar_position: 4
---

# Terminology API

The terminology API is a provider-neutral, environment-scoped gateway at `/api/terminology` (also mounted under `/v1`). It routes each request to a configured terminology provider by code system, so callers use one contract regardless of where a code system is served.

## Providers and routing

Two provider types are supported:

| Type | Description |
|------|-------------|
| `kehrnel-strategy` | A native \{kehrnel\} strategy. When `snomedct.mongodb` is activated, it is registered automatically as `snomed-local` for `http://snomed.info/sct` |
| `fhir-terminology` | An external FHIR terminology server, called with FHIR `Parameters` |

Routing is deterministic and fails closed: a request is resolved by an exact ValueSet route, an exact system (and optional version) route, a single provider declaring the system, or a default that declares the system. Otherwise the request returns an unsupported or ambiguous error. An explicit `provider_id` cannot bypass a provider's declared code systems.

Responses identify the selected provider and provider type.

## Requirements

- Environment resolved from `x-active-env` (or the `env_id` query parameter, or the platform default); a missing environment returns `400`
- The caller's API key must be allowed to access that environment (`403` otherwise)
- `PUT /api/terminology/configuration` additionally requires admin access

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/terminology/capabilities` | List providers, routes, systems, and supported operations (`include_readiness`, default `false`) |
| `GET` | `/api/terminology/configuration` | Read the environment's terminology configuration, with credentials redacted |
| `PUT` | `/api/terminology/configuration` | Validate and replace the non-secret terminology configuration (admin) |
| `GET` | `/api/terminology/providers/{provider_id}/capabilities` | Native readiness contract, or the external server's FHIR `metadata` summary |
| `POST` | `/api/terminology/providers/{provider_id}/test` | Run a real `validate-code` against one provider and return the evidence |
| `POST` | `/api/terminology/lookup` | Look up a code |
| `POST` | `/api/terminology/validate-code` | Validate a code, optional display, and optional ValueSet membership |
| `POST` | `/api/terminology/validate-codes` | Validate a batch of 1 to 100 codings; returns per-item results and a summary |
| `POST` | `/api/terminology/subsumes` | Test subsumption between `code_a` and `code_b` |
| `POST` | `/api/terminology/expand` | Expand a ValueSet URL or a native ECL `expression` |
| `POST` | `/api/terminology/translate` | Translate a code through a ConceptMap |
| `POST` | `/api/terminology/search` | Text search within a code system |

Operations available per provider type:

- `kehrnel-strategy`: `capabilities`, `lookup`, `validate-code`, `subsumes`, `expand`, `search`, `ecl`, `ground`
- `fhir-terminology`: `lookup`, `validate-code`, `subsumes`, `expand`, `translate`

## Request fields

| Endpoint | Fields |
|----------|--------|
| `lookup` | `system`, `code`, optional `version`, `language`, `provider_id` |
| `validate-code`, `providers/{provider_id}/test` | lookup fields plus optional `display`, `url` (ValueSet), `value_set_version` |
| `validate-codes` | `codings`: list of codings with the `validate-code` fields plus an optional caller-owned `path` echoed in the result |
| `subsumes` | `system`, `code_a`, `code_b`, optional `version`, `provider_id` |
| `expand` | optional `system`, `version`, `url`, `value_set_version`, `expression`, `concept_id`, `filter`, `offset`, `count` (1 to 1000), `include_designations`, `active_only`, `provider_id` |
| `translate` | `code`, optional `url` (ConceptMap), `concept_map_version`, `system`, `version`, `target`, `target_system`, `reverse`, `provider_id` |
| `search` | `system`, `q`, optional `version`, `language`, `offset`, `limit` (1 to 100), `provider_id` |

## Example

```bash
curl -sS -X POST "http://localhost:8080/api/terminology/validate-code" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "system": "http://snomed.info/sct",
    "code": "73211009",
    "display": "Diabetes mellitus"
  }'
```

## CLI

The `kehrnel terminology` group calls this API for `capabilities`, `provider-capabilities`, `configure`, `lookup`, `validate-code`, `subsumes`, `expand`, and `translate`. SNOMED CT release, ValueSet, and grounding commands in the same group call the [SNOMED CT Domain API](/docs/api/domains/snomedct).

## FHIR operations

The same gateway backs the FHIR terminology operations on the FHIR domain (`CodeSystem/$lookup`, `CodeSystem/$validate-code`, `CodeSystem/$subsumes`, `ValueSet/$validate-code`, `ValueSet/$expand`, `ConceptMap/$translate`). See [Shared Terminology Service](/docs/strategies/snomedct/mongodb/terminology-service).

## Related

- [Core Layer API](/docs/api/core)
- [SNOMED CT Domain API](/docs/api/domains/snomedct)
- [FHIR Domain API](/docs/api/domains/fhir/)
- [SNOMED CT Domain](/docs/domains/snomedct)

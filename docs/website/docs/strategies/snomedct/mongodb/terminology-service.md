---
sidebar_position: 6
---

# Shared Terminology Service

The native SNOMED CT strategy can serve its data through three compatible boundaries:

| Boundary | Purpose |
|---|---|
| `/api/domains/snomedct/*` | Native SNOMED search, ECL, hierarchy, model, release, and grounding capabilities |
| `/api/terminology/*` | Provider-neutral terminology contract used by all Kehrnel domains |
| `/api/domains/fhir/CodeSystem/$...`, `/ValueSet/$...`, and `/ConceptMap/$translate` | Standard FHIR terminology operations |

The provider-neutral layer is intentionally separate from the SNOMED data model. A tenant can use `snomedct.mongodb` for SNOMED and route LOINC, ICD, or another system to a customer-managed FHIR terminology server.

## Native SNOMED Provider

When `snomedct.mongodb` is activated, Kehrnel exposes it automatically as `snomed-local` for the canonical system `http://snomed.info/sct`. The inferred provider is explicit in the capabilities response and is not a fallback for another system.

```bash
kehrnel terminology capabilities --env dev --readiness
kehrnel terminology provider-capabilities snomed-local --env dev
kehrnel terminology snomed-releases --env dev
kehrnel terminology inspect-snomed-release --env dev \
  --file-name edicion_20260601.json --limit 1000
kehrnel terminology ingest-snomed-release --env dev \
  --release-id 20260601 --file-name edicion_20260601.json \
  --execute --license-acknowledged
kehrnel terminology ensure-snomed-indexes --env dev --execute
kehrnel terminology validate-code 73211009 \
  --env dev \
  --system http://snomed.info/sct \
  --display "Diabetes mellitus"
kehrnel terminology subsumes 404684003 73211009 \
  --env dev \
  --system http://snomed.info/sct
kehrnel terminology expand --env dev \
  --system http://snomed.info/sct \
  --ecl "<< 73211009"
kehrnel terminology register-value-set --env dev \
  --url https://example.org/fhir/ValueSet/diabetes \
  --version 1.0.0 \
  --release-id 20260601 \
  --ecl "<< 73211009"
kehrnel terminology validate-value-set 73211009 --env dev \
  --url https://example.org/fhir/ValueSet/diabetes \
  --value-set-version 1.0.0
kehrnel terminology suggest "myocard" --language en --env dev
kehrnel terminology concept-history 73211009 --language en --env dev
kehrnel terminology save-grounding-review review.json --env dev
kehrnel terminology query-grounded-corpus 404684003 --env dev
kehrnel terminology benchmark-retrieval retrieval-cases.json --env dev
```

## External FHIR Terminology Provider

Store non-secret provider configuration in environment metadata through the terminology configuration API:

```json
{
  "providers": [
    {
      "id": "customer-loinc",
      "label": "Customer LOINC service",
      "type": "fhir-terminology",
      "base_url": "https://terminology.example/fhir",
      "systems": ["http://loinc.org"],
      "auth": {
        "type": "bearer-hdl",
        "secret_ref": "customer-loinc"
      }
    }
  ],
  "routes": [
    {
      "system": "http://loinc.org",
      "provider_id": "customer-loinc"
    }
  ]
}
```

Healthcare Data Lab encrypts `bearer-hdl` and `api-key-hdl` credentials in the environment secret store. Standalone Kehrnel deployments can instead use `bearer-env` or `api-key-env` and name the server-side environment variable. Raw tokens, headers, and credentials are rejected from environment metadata.

Configure from the CLI:

```bash
kehrnel terminology configure terminology.json --env dev
```

Provider verification is an explicit real operation, not a superficial URL
ping: `POST /api/terminology/providers/{provider_id}/test` executes
`$validate-code` for the supplied system/code and returns provider, release,
correlation, latency, request-digest, result-digest, and cache-status evidence.
An explicit provider id cannot bypass that provider's declared code-system
scope.

`GET /api/terminology/providers/{provider_id}/capabilities` performs a separate
capability check. Native SNOMED returns its runtime/readiness contract; an
external provider is queried at its FHIR `metadata` endpoint. Kehrnel compares
the operations advertised by the server with the operations declared in the
tenant configuration and returns a digest of the inspected capability
statement without exposing credentials.

## Routing Rules

Routing is deterministic and fails closed:

1. an exact ValueSet route;
2. an exact system and optional version route;
3. one provider that uniquely declares the system;
4. a configured default only when it declares the requested system;
5. otherwise an explicit unsupported or ambiguous error.

Kehrnel never silently sends a coding to a provider for another code system.

## FHIR Operations

The FHIR boundary exposes GET and POST variants for:

- `CodeSystem/$lookup`
- `CodeSystem/$validate-code`
- `CodeSystem/$subsumes`
- `ValueSet/$validate-code`
- `ValueSet/$expand`
- `ConceptMap/$translate`

POST requests use FHIR `Parameters`; responses are FHIR `Parameters` or `ValueSet`. The active FHIR `CapabilityStatement` advertises only operations provided by the current tenant terminology configuration. Response headers identify the selected provider, correlation id, and native release when available.

Example:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/fhir/CodeSystem/\$validate-code" \
  -H "Content-Type: application/fhir+json" \
  -H "x-active-env: dev" \
  -d '{
    "resourceType": "Parameters",
    "parameter": [
      {"name": "system", "valueUri": "http://snomed.info/sct"},
      {"name": "code", "valueCode": "73211009"},
      {"name": "display", "valueString": "Diabetes mellitus"}
    ]
  }'
```

## Current Boundary

This slice provides lookup, single and bounded-batch validation, subsumption, expansion, paged search/navigation, typeahead, cross-release concept history, ECL, candidate grounding, reviewer-confirmed coding, ancestor-based reviewed-corpus queries, bounded retrieval benchmarks, external ConceptMap translation, and a native tenant ValueSet registry. Native ValueSets are immutable by canonical URL and version, can use an ECL or explicit-code definition, and retain the pinned SNOMED release plus a definition digest. FHIR imports can optionally validate codings for an explicit list of systems in `advisory` or fail-closed `required` mode.

Grounding reviews live in the tenant strategy database. Kehrnel stores a source reference, source-text hash, reviewer evidence, release, and accepted/rejected codings. Source text is not stored unless `grounding.store_source_text=true` is explicitly activated. This is a governed coding workbench, not autonomous clinical coding.

FHIR IG compilation now inventories bindings and invariants from
`StructureDefinition` resources, but inventory is not enforcement. Full
binding-aware validation still requires route/version locking and a configured
profile validator. Complex FHIR `ValueSet.compose` import, native ConceptMap
authoring, closure tables, the complete ECL grammar, model-based clinical
mention extraction, and production-scale benchmark jobs remain separate
milestones.

---
sidebar_position: 4
---

# SNOMED CT Domain

This section centralizes SNOMED CT terminology documentation for \{kehrnel\}.

The SNOMED CT domain is served by the `snomedct.mongodb` strategy. Domain routes require an active activation with `domain: snomedct` and that strategy on the environment resolved from `x-active-env` (or the platform default); another strategy id returns `409`. The default strategy id can be overridden with `KEHRNEL_SNOMEDCT_STRATEGY_ID`.

## Scope

SNOMED CT data can be reached through three boundaries that share the same tenant routing:

| Boundary | Purpose |
|----------|---------|
| `/api/domains/snomedct/*` | Native SNOMED CT API: releases, lookup, hierarchy, ECL, search and scope, ValueSets, note mapping, grounding, benchmarks, readiness |
| `/api/terminology/*` | Provider-neutral terminology contract (lookup, validate, subsumes, expand, translate, search) |
| `/api/domains/fhir/CodeSystem/$...`, `/ValueSet/$...`, `/ConceptMap/$translate` | Standard FHIR terminology operations |

When `snomedct.mongodb` is activated, the terminology gateway exposes it automatically as the native provider `snomed-local` for the system `http://snomed.info/sct`. Other code systems can be routed to an external FHIR terminology server (provider type `fhir-terminology`) through the terminology configuration; routing never sends a coding to a provider that does not declare its system.

### Licensing

\{kehrnel\} does not distribute SNOMED CT content. Each environment brings its own licensed SNOMED CT JSON release, staged in the configured local release folder. Persisting a release requires an explicit execute flag and a license acknowledgement (`license_acknowledged` in the API, `--execute --license-acknowledged` in the CLI).

## Quick Navigation

### Domain API

- [SNOMED CT Domain API](/docs/api/domains/snomedct)
- [Provider-neutral Terminology API](/docs/api/core/terminology)
- [FHIR Domain API](/docs/api/domains/fhir/) (terminology operations)

### Strategy

- [SNOMED CT on MongoDB Strategy](/docs/strategies/snomedct/mongodb/)
- [SNOMED CT Configuration](/docs/strategies/snomedct/mongodb/configuration)
- [SNOMED CT Workflows and API](/docs/strategies/snomedct/mongodb/workflows)
- [SNOMED CT Data Model](/docs/strategies/snomedct/mongodb/data-model)
- [SNOMED CT Search and Grounding](/docs/strategies/snomedct/mongodb/search-grounding)
- [Shared Terminology Service](/docs/strategies/snomedct/mongodb/terminology-service)

`snomedct.mongodb` manifest: version `0.6.0`, maturity **preview**. Endpoints and behaviour can still change between releases.

### Core CLI Entry Points

- [CLI Overview](/docs/cli/overview)
- [Domain CLI Layer](/docs/cli/domains)
- [Strategy CLI Layer](/docs/cli/strategies)

The `kehrnel terminology` command group covers both the provider-neutral API and the native SNOMED CT routes:

| Area | Commands | API root |
|------|----------|----------|
| Gateway | `capabilities` (`--readiness`), `provider-capabilities`, `configure` | `/api/terminology` |
| Coding operations | `lookup`, `validate-code`, `subsumes`, `expand`, `translate` | `/api/terminology` |
| Releases | `snomed-releases`, `inspect-snomed-release`, `diff-snomed-release`, `ingest-snomed-release`, `rebuild-snomed-sidecar`, `ensure-snomed-indexes` | `/api/domains/snomedct` |
| ValueSets | `value-sets`, `register-value-set`, `validate-value-set` | `/api/domains/snomedct` |
| Search and history | `suggest`, `concept-history`, `hybrid-search`, `resolve-scope` | `/api/domains/snomedct` |
| Grounding | `nlp-map`, `llm-ground`, `save-grounding-review`, `query-grounded-corpus` | `/api/domains/snomedct` |
| Benchmarks | `benchmark-retrieval` (inline cases or `--gold-note-ids`), `gold-notes`, `save-gold-note`, `delete-gold-note` | `/api/domains/snomedct` |

```bash
kehrnel terminology capabilities --env dev --readiness
kehrnel terminology validate-code 73211009 --env dev \
  --system http://snomed.info/sct --display "Diabetes mellitus"
kehrnel terminology expand --env dev \
  --system http://snomed.info/sct --ecl "<< 73211009"
```

## Recommended Reading Order

1. strategy introduction and configuration
2. workflows (stage, inspect, diff, ingest, sidecar, indexes)
3. SNOMED CT domain API
4. search and grounding
5. shared terminology service and FHIR operations

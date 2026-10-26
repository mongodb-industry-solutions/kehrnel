---
sidebar_position: 6
---

# Domain API: SNOMED CT

The SNOMED CT domain exposes native terminology operations backed by the active `snomedct.mongodb` strategy on an environment. The router is mounted at `/api/domains/snomedct` and also under `/v1`.

:::info Preview
`snomedct.mongodb` has manifest maturity **preview** (`0.6.0`). Endpoints and behaviour can still change between releases.
:::

## Strategy documentation

- [SNOMED CT on MongoDB strategy](/docs/strategies/snomedct/mongodb/)
- [Workflows and API examples](/docs/strategies/snomedct/mongodb/workflows)
- [Search and grounding](/docs/strategies/snomedct/mongodb/search-grounding)

## Requirements

- Environment resolved from `x-active-env` (or the `env_id` query parameter, or the platform default)
- Active activation with `domain: snomedct` and `strategy_id: snomedct.mongodb` (override with `KEHRNEL_SNOMEDCT_STRATEGY_ID`); any other strategy returns `409`
- Routes marked **admin** call the runtime's admin-access check: when authentication is enabled they require a valid `x-api-key`

Every route dispatches to a strategy operation (for example `snomed_search`, `snomed_readiness`) on the active activation, so the same work is also reachable through `POST /environments/{env_id}/run` and the `kehrnel terminology` CLI.

## Releases and maintenance

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/domains/snomedct/releases` | List staged SNOMED CT JSON release files (optional `file_pattern`) |
| `POST` | `/api/domains/snomedct/releases/inspect` | Inspect a staged release file |
| `POST` | `/api/domains/snomedct/releases/diff` | Diff a staged release against the current canonical collection |
| `POST` | `/api/domains/snomedct/releases/ingest` | Dry-run or ingest a staged release (**admin**; persistence requires license acknowledgement) |
| `POST` | `/api/domains/snomedct/sidecar/rebuild` | Rebuild the term sidecar (**admin**) |
| `POST` | `/api/domains/snomedct/indexes/ensure` | Create the strategy indexes (**admin**) |
| `GET` | `/api/domains/snomedct/capabilities` | Strategy capabilities (optional `release_id`, `include_readiness`, default `true`) |
| `GET` | `/api/domains/snomedct/readiness` | Readiness report (optional `release_id`) |

## Concepts and hierarchy

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/domains/snomedct/concepts/{concept_id}` | Look up a concept (optional `release_id`) |
| `GET` | `/api/domains/snomedct/concepts/{concept_id}/history` | Cross-release concept history (`language`, default `es`; `offset`; `limit` up to 100) |
| `POST` | `/api/domains/snomedct/concepts/{concept_id}/children` | Paged children |
| `POST` | `/api/domains/snomedct/concepts/{concept_id}/descendants` | Paged descendants |
| `POST` | `/api/domains/snomedct/concepts/{concept_id}/ancestors` | Paged ancestors |
| `POST` | `/api/domains/snomedct/validate-code` | Validate a code and optional display |
| `POST` | `/api/domains/snomedct/subsumes` | Hierarchy-based subsumption test between two codes |

## Search

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/domains/snomedct/search` | Term search over the sidecar, optionally scoped by `ecl` and/or `area_concept_id` |
| `POST` | `/api/domains/snomedct/suggest` | Typeahead suggestions |
| `POST` | `/api/domains/snomedct/hybrid-search` | Lexical, vector, or fused retrieval (`mode`) |
| `POST` | `/api/domains/snomedct/scope` | Resolve an ECL and/or area constraint into a cached concept-id scope |
| `POST` | `/api/domains/snomedct/relationships/search` | Relationship search |
| `POST` | `/api/domains/snomedct/semantic-facets` | Semantic facets |

`search` and `hybrid-search` accept `ecl` and/or `area_concept_id` to confine
retrieval to a resolved scope. `hybrid-search` also accepts `mode`
(`lexical`, `vector`, or `hybrid`); when no vector index is configured it reports
`strategy: "lexical-fallback"` and an `explain.vector.reason` instead of
silently returning lexical results as if they were semantic.

Results carry `matchTier`, `matchReason`, and `matchedText` so the ordering is
explainable rather than implied by database row order.

## ECL and ValueSets

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/domains/snomedct/ecl` | Execute an ECL expression (supported subset) |
| `POST` | `/api/domains/snomedct/ecl/parse` | Parse an ECL expression |
| `POST` | `/api/domains/snomedct/ecl/compile` | Compile an ECL expression without executing it |
| `POST` | `/api/domains/snomedct/expand` | Paged value-set expansion |
| `GET` | `/api/domains/snomedct/value-sets` | List registered ValueSets (optional `status`; `limit` up to 500) |
| `POST` | `/api/domains/snomedct/value-sets` | Register an immutable named ValueSet pinned to a release (**admin**) |
| `POST` | `/api/domains/snomedct/value-sets/$validate-code` | Validate membership of a code in a registered ValueSet |

## Grounding and benchmarks

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/domains/snomedct/ground` | Generate candidate codings for clinical mentions |
| `POST` | `/api/domains/snomedct/nlp-map` | Map a clinical note to candidates with context, evidence graph, and principal-diagnosis candidates |
| `POST` | `/api/domains/snomedct/llm-ground` | Guarded LLM adjudication over already-retrieved candidates |
| `POST` | `/api/domains/snomedct/ground/reviews` | Save a reviewer decision on a grounding (**admin**) |
| `POST` | `/api/domains/snomedct/ground/corpus/search` | Ancestor-based query over reviewed groundings |
| `GET` | `/api/domains/snomedct/gold-notes` | List stored benchmark gold notes |
| `POST` | `/api/domains/snomedct/gold-notes` | Create or replace a gold note (**admin**) |
| `DELETE` | `/api/domains/snomedct/gold-notes/{note_id}` | Delete a gold note (**admin**) |
| `POST` | `/api/domains/snomedct/benchmarks/retrieval` | Bounded retrieval benchmark, optionally graded against `gold_note_ids` |
| `POST` | `/api/domains/snomedct/usage-events` | Record a product/usage event |

`nlp-map` extracts mentions and their context, grounds them against the release,
and returns an evidence graph plus principal-diagnosis candidates. The extractor
never produces codes, and `llm-ground` may only choose among candidates
retrieval already returned — decisions naming anything else are dropped. Both
degrade honestly: without an `llm` binding, note mapping falls back to
deterministic extraction and `llm-ground` returns `503 SNOMED_LLM_UNAVAILABLE`.

## Example

```bash
curl -sS -X POST "http://localhost:8080/api/domains/snomedct/validate-code" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{"code": "73211009", "display": "Diabetes mellitus"}'
```

## Related

- [Provider-neutral Terminology API](/docs/api/core/terminology)
- [FHIR Domain API](/docs/api/domains/fhir/) — `CodeSystem`, `ValueSet`, and `ConceptMap` operations
- [SNOMED CT Domain](/docs/domains/snomedct)

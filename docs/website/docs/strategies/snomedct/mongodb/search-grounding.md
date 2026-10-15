---
sidebar_position: 5
---

# SNOMED CT Search and Grounding

Search and grounding use the term sidecar. If `collections.sidecar_enabled=false`, the strategy still supports canonical lookup, basic ECL, release diff, and readiness, but it cannot search or ground clinical mentions.

## Query Modes

| Mode | Runtime input | Collection | Requires sidecar |
|------|---------------|------------|------------------|
| `lookup` | `concept_id`, `release_id` | canonical | No |
| `ecl` | `expression`, `release_id`, `limit` | canonical | No |
| `expand` | `expression` or `concept_id`, `release_id`, `limit` | canonical | No |
| `relationship_search` | `type_id`, `destination_id`, `release_id`, `limit` | canonical | No |
| `search` | `q`, `language`, `release_id`, `limit`, optional `ecl`/`area_concept_id` | sidecar | Yes |
| `hybrid_search` | `q`, `mode`, `ancestor_id`, `area_tag`, `semantic_tag_key`, optional `ecl`/`area_concept_id` | sidecar | Yes |
| `semantic_facets` | `q`, `ancestor_id`, `language`, `release_id` | sidecar | Yes |
| `ground` | `mentions` or `text`, `language`, `release_id` | sidecar | Yes |
| `nlp_map` | `text`, `language`, `release_id`, optional `ecl`/`area_concept_id`/`target_context` | sidecar | Yes |

## Search Pipeline

The baseline search strategy is `normalized_regex`:

1. Normalize the query by lowercasing, removing diacritics, stripping punctuation, and collapsing whitespace.
2. Match sidecar `normalizedTerm` for the requested release and language.
3. Fetch a retrieval pool wider than the requested page, then rank it in the application.
4. Return sidecar candidates with concept IDs, descriptions, FSN, preferred term, semantic tag, roots, and area tags.

This is deterministic and works on MongoDB Atlas and non-Atlas MongoDB.

### Tiered Rerank

The database's row order is not the answer order. Every candidate is classified
into an explicit tier — exact SCTID, exact preferred term, exact synonym, exact
FSN, lemma variant, prefix, contains, fuzzy — and each result carries
`matchTier`, `matchReason`, and `matchedText` so the ordering is explainable.
Ties break on length distance, preferred flag, `termRank`, then lexical score.

Query variants cover the common misspellings and singular/plural drift that
would otherwise return nothing ("diabetis" → "diabetes").

### Query Planning

`normalizedTerm` is matched with an unanchored regex, which cannot produce index
bounds on its own. The index that serves it is `term_release_language`
(`releaseId, languageCode, normalizedTerm`), because a regex on the *last* key of
a compound index is evaluated against the index entries — only matching
documents are fetched.

The MongoDB planner does not reliably pick that index. On a 150k-term sidecar it
chose `term_semantic_lookup` instead, whose keys do not include `normalizedTerm`,
so every document in the release/language range was fetched and filtered:

| Plan | Documents examined | Time |
|------|--------------------|------|
| Planner default (`term_semantic_lookup`) | 75,000 | 89 ms |
| Hinted (`term_release_language`) | 6,784 | 43 ms |

The strategy therefore hints `term_release_language` on the lexical path. The
hint is a hint, not a requirement: it is omitted when the query filters on
`semanticTagKey` or `areaTags` (those predicates have their own indexes and are
served better by them), and if the index is absent the query is retried without
the hint rather than failing. The chosen hint is reported in `explain.hint`.

### Note Mapping Cost

`snomed_nlp_map` grounds every extracted phrase. Grounding each phrase with its
own search costs one index scan and one round trip per phrase, which dominates
note latency. Phrases are instead grounded in a single aggregation: an `$or`
narrows the sidecar once, and `$facet` buckets that stream per phrase in memory.
Each result row records `strategy: "tiered-lexical-batched"`.

Measured on a 150k-term sidecar with a 10-phrase note:

| Approach | Aggregations | Documents examined | Time |
|----------|--------------|--------------------|------|
| One search per phrase | 10 | 150,464 | 2,307 ms |
| One batched aggregation | 1 | 717 | 80 ms |

Batching is an optimisation, not a requirement: if the batched plan is
unavailable or fails (for example the `$facet` memory limit), the operation falls
back to per-phrase searches and reports `tiered-lexical` per row.

### Retrieval Modes

`snomed_hybrid_search` accepts a `mode`:

| Mode | Behaviour |
|------|-----------|
| `lexical` | Tiered lexical retrieval with ontology filters (ancestor, area tag, semantic tag) |
| `vector` | Atlas `$vectorSearch` over the sidecar's embedded term text |
| `hybrid` | Both branches, fused with reciprocal rank fusion, then optionally reranked |

When `retrieval.vector_index` is empty, or the vector branch is unavailable, the
response says so: `mode: "hybrid"` with `strategy: "lexical-fallback"` and an
`explain.vector.reason`. It never claims a semantic retrieval that did not run.
Cross-encoder reranking is applied only when a `rerank` binding exists, and the
response reports `reranked: false` when it did not.

## Semantic Scope

`snomed_resolve_scope` (and the `ecl` / `area_concept_id` fields on search,
hybrid search, suggest, and note mapping) resolve a constraint into the concrete
set of concept ids a query is confined to:

- `ecl` compiles through the same ECL planner used by `/ecl`.
- `area_concept_id` expands to the focus concept plus its active descendants.
- Both together intersect.
- The result is capped at `search.scope_max_ids` and cached per environment with
  `search.scope_cache_ttl_seconds` / `search.scope_cache_max_entries`.

The response reports `mode`, `expandedCount`, `appliedScopeCount`, `truncated`,
and per-clause `cache` and `timings` metadata, so a slow first query and a fast
repeat are distinguishable.

A scope never widens a query: `*` is rejected as a scope expression.

## ECL Subset

The canonical planner supports the subset that maps cleanly to indexed MongoDB fields:

| Expression | Meaning |
|------------|---------|
| `*` | Active concepts in the release |
| `73211009` | Exact concept |
| `< 73211009` | Descendants only |
| `<< 73211009` | Self or descendants |
| `> 73211009` | Ancestors only, compiled as a target-first aggregation |
| `>> 73211009` | Self or ancestors, compiled as a target-first aggregation |
| `^ 900000000000497000` | Members of a reference set |
| `AND`, `OR`, `MINUS` | Boolean combinations over supported constraints |
| `( ... )` | Parenthesized constraints |
| `< 404684003 : 363698007 = 39057004` | Exact attribute refinement using `relationshipAttributeKeys` |
| `< 404684003 : { 363698007 = 39057004, 116676008 = 123037004 }` | Grouped exact attribute refinements |
| `<< 404684003 : 363698007 = << 80891009` | Attribute target subsumption: the target's descendants are expanded once at compile time and matched through indexed `relationshipAttributeKeys` values |

Attribute target subsumption (`attribute = <` / `<<`) is bounded by `search.scope_max_ids` (default 20000) and fails closed with `ECL_TARGET_EXPANSION_TOO_LARGE` beyond it. The planner still does not claim full transitive relationship reasoning or attribute **type** subsumption; those cases require either runtime joins that may be expensive on full releases or a deliberate materialized relationship strategy.

## MongoDB-Native Terminology APIs

SNOMED CT for MongoDB also exposes APIs that are not strict ECL but are useful for applications and education:

| API | Purpose |
|-----|---------|
| `/concepts/{id}/children` | Direct hierarchy navigation using `inferredParentIds` |
| `/concepts/{id}/descendants` | Value-set style branch expansion using `inferredAncestorIds` |
| `/concepts/{id}/ancestors` | Target-first ancestor expansion without retaining descendant arrays |
| `/expand` | Reusable value-set expansion from ECL or a focus concept |
| `/relationships/search` | Exact relationship type/destination search over canonical concepts |
| `/semantic-facets` | Sidecar facets over area tags, semantic tags, and top roots |
| `/scope` | Resolve an ECL and/or area constraint into a cached concept-id scope |
| `/hybrid-search` | Lexical, vector, or fused retrieval with ontology scope and semantic tags |
| `/hybrid-search` | Lexical search constrained by hierarchy scope and semantic tags |

## Grounding

`snomed_ground_note` receives extracted mentions and runs candidate search per mention. It deliberately does not perform LLM disambiguation itself. That separation keeps Kehrnel's terminology retrieval deterministic and lets downstream applications choose their LLM, prompt, evidence graph, and adjudication policy.

Input:

```json
{
  "mentions": ["type 2 diabetes mellitus", "diabetic nephropathy"],
  "language": "en",
  "release_id": "20260601",
  "limit_per_mention": 5
}
```

Output shape:

```json
{
  "ok": true,
  "grounded": [
    {
      "mention": "type 2 diabetes mellitus",
      "candidates": []
    }
  ]
}
```

## Clinical Note Mapping

`snomed_nlp_map` is the higher-level operation: it takes a whole note and
returns grounded mentions with the context that scopes them.

Extraction runs in two modes. The deterministic extractor splits the note into
sections and sentences, detects cue words (negation, family history, history,
plan, certainty, and relation cues), and scores candidate phrases. When an `llm`
binding is present it can instead ask the model for mentions and their context —
but the model returns *text and context only*, never codes.

Retrieval is unchanged either way: every candidate comes from the canonical
release through the same tiered search described above, optionally constrained
by a scope.

Each result carries:

- `status` — `accepted`, `review`, or `abstain`, with `confidence`,
  `confidencePct`, `marginToNext`, and `decisionReasons`.
- Context — `assertion`, `contextType`, `experiencer` derived from the cue
  signals that overlap the mention.
- `topCandidate` and up to three `alternatives`.

The candidate scorer penalises candidates that do not fit the phrase's context:
an unattributed laterality ("left" in the term but not in the note), a
population qualifier ("neonatal"), a severity qualifier ("uncontrolled"), a
workflow term ("follow-up") in a non-plan sentence, or a compound descendant
("due to X") that duplicates a relation cue.

Alongside the flat results, the response includes:

- `evidenceGraph` — sections, sentences, spans, cue objects, mentions,
  relations, and grounded nodes, keyed by a hash of the note rather than the
  note text.
- `principalDiagnosisCandidates` — present, patient mentions aggregated by
  concept and ranked by section, assertion, evidence type, and semantic-tag fit
  against `target_context`.

`extract_only: true` returns the graph without any retrieval, which is useful
for reviewing extraction quality on its own.

### Guarded LLM Adjudication

`snomed_llm_ground` is a separate, optional step. It takes mentions the caller
already grounded, along with the candidates retrieval returned, and asks the
model to confirm one concept per mention, name a primary diagnosis, and
summarise its reasoning.

The model cannot invent a code. Any decision whose `conceptId` is not in that
mention's candidate list is dropped, and `primaryConceptId` is reset to `none`
unless it appears in some candidate list. With no `llm` binding the operation
fails closed with `503 SNOMED_LLM_UNAVAILABLE`.

The separation matters: retrieval stays deterministic and auditable, the LLM
only ever reorders what the terminology server already returned, and a human
still confirms the result through `snomed_save_grounding_review`.

## Readiness

Use `snomed_readiness` or `/api/domains/snomedct/readiness` to confirm:

- canonical concept count
- active concept count
- sidecar enabled/ready status
- whether descendant arrays are retained
- retrieval configuration: lexical strategy, whether a vector index and reranker
  are bound, Atlas Search index health, and semantic-scope cache statistics
- gold-note count
- feature availability for lookup, ECL, search, vector search, semantic scope,
  grounding, note mapping, LLM grounding, and benchmarks


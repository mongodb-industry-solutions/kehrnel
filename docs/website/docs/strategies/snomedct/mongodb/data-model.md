---
sidebar_position: 4
---

# SNOMED CT Data Model

`snomedct.mongodb` stores the official JSON release as canonical documents, derives a term sidecar for search, keeps tenant-authored named ValueSets in a separate registry, and isolates reviewed grounding artifacts from both.

## Canonical Collection

Default collection: `snomed_concepts`.

Identity:

```text
_id = releaseId + "|" + conceptId
```

Representative fields:

```json
{
  "_id": "20260601|73211009",
  "releaseId": "20260601",
  "releaseLabel": "SNOMED CT International + Spain + Medicines extension",
  "conceptId": "73211009",
  "active": true,
  "effectiveTime": "20260601",
  "moduleId": "900000000000207008",
  "definitionStatusId": "900000000000074008",
  "inferredParentIds": ["44054006"],
  "inferredAncestorIds": ["404684003"],
  "descriptions": [],
  "relationships": [],
  "concreteRelationships": [],
  "relationshipAttributeKeys": ["116680003|44054006"],
  "releaseDate": "2026-06-01T00:00:00Z",
  "releaseAppliedAt": "2026-07-10T00:00:00Z"
}
```

The strategy drops `inferredDescendantIds` by default. Those arrays can be very large and are redundant for common ancestor/subsumption queries when `inferredAncestorIds` are present. Set `ingest.include_descendants=true` only when a downstream workload needs direct descendant materialization.

## Term Sidecar

Default collection: `snomed_terms`.

Identity:

```text
_id = releaseId + "|" + conceptId + "|" + descriptionId + "|" + languageCode
```

Representative fields:

```json
{
  "_id": "20260601|73211009|101|en",
  "releaseId": "20260601",
  "conceptId": "73211009",
  "descriptionId": "101",
  "languageCode": "en",
  "term": "Diabetes mellitus",
  "normalizedTerm": "diabetes mellitus",
  "preferredTerm": "Diabetes mellitus",
  "fsn": "Diabetes mellitus (disorder)",
  "termType": "synonym",
  "preferred": true,
  "semanticTag": "disorder",
  "parentIds": ["44054006"],
  "ancestorIds": ["404684003"],
  "areaTags": ["clinical-finding", "disorder"],
  "termRank": 54,
  "embedText": "Diabetes mellitus | Diabetes mellitus | Diabetes mellitus (disorder) | disorder"
}
```

The sidecar is a projection. It can be deleted and rebuilt from canonical concepts.

## ValueSet Registry

Default collection: `snomed_value_sets`.

Identity is the canonical `url` plus immutable `version`. Each definition is
either an ECL expression or an explicit list of concept ids and is pinned to a
SNOMED `releaseId`. `definitionDigest` makes idempotent replay observable and
prevents a published URL/version from silently changing meaning.

```json
{
  "url": "https://example.org/fhir/ValueSet/diabetes",
  "version": "1.0.0",
  "name": "Diabetes mellitus descendants",
  "status": "active",
  "system": "http://snomed.info/sct",
  "releaseId": "20260601",
  "definition": {"type": "ecl", "expression": "<< 73211009"},
  "definitionDigest": "sha256…",
  "createdAt": "2026-09-05T00:00:00+00:00"
}
```

## Reviewed Grounding Artifacts

Default collection: `snomed_grounding_reviews`.

Each document represents one idempotent human review. Accepted and rejected
codings retain the SNOMED release and materialized `ancestorIds`. The document
also carries a de-duplicated `acceptedAncestorIds` projection for the common
present-patient query, avoiding a fragile compound multikey index across nested
coding arrays.
By default only `sourceRef` and `sourceTextHash` are stored; the source text is
retained only when `grounding.store_source_text=true` is explicitly activated.

This collection contains tenant-authored evidence, never licensed terminology
content, and it is not an autonomous coding decision log.

## Gold Notes

Default collection: `snomed_gold_notes`.

A gold note is the tenant's own benchmark fixture: a note text plus the facts a
correct mapping should find. `snomed_benchmark_retrieval` turns each stored note
into one case per expected concept, so retrieval quality can be tracked against
a governed corpus instead of ad-hoc queries.

```json
{
  "id": "gold-en-diabetes",
  "title": "Type 2 diabetes follow-up",
  "languageCode": "en",
  "text": "Patient has type 2 diabetes mellitus.",
  "facts": [
    {"phrase": "type 2 diabetes mellitus", "assertion": "present", "subject": "patient", "section": "active", "conceptId": "44054006", "term": "Diabetes mellitus type 2"}
  ],
  "source": "custom",
  "createdAt": "2026-10-06T00:00:00+00:00",
  "updatedAt": "2026-10-06T00:00:00+00:00"
}
```

Only facts that are `present`, `patient`, and carry a `conceptId` become expected
answers; negated, family, and planned facts stay in the fixture as distractors.

## Usage Events

Default collection: `snomed_usage_events`.

One document per product event (search executed, result selected, feedback
submitted). The raw query is never stored: `queryPreview` is truncated to 120
characters and `queryHash` is a truncated SHA-256 of the lowercased query.

```json
{
  "eventType": "explorer.search.submitted",
  "at": "2026-10-06T00:00:00+00:00",
  "source": "snomedct.mongodb",
  "tenantId": "demo-hospital",
  "releaseId": "20260601",
  "languageCode": "en",
  "conceptIds": ["73211009"],
  "queryPreview": "diabetes mellitus",
  "queryHash": "9f2c…",
  "metadata": {"mode": "lexical"}
}
```

## Indexes

`snomed_ensure_indexes` creates these baseline indexes when enabled:

| Index | Collection | Fields | Purpose |
|-------|------------|--------|---------|
| `concept_release_unique` | canonical | `releaseId`, `conceptId` | Release-aware lookup |
| `ancestor_lookup` | canonical | `releaseId`, `inferredAncestorIds` | Descendant/subsumption queries |
| `parent_lookup` | canonical | `releaseId`, `inferredParentIds` | Navigation |
| `relationship_attribute_lookup` | canonical | `releaseId`, `relationshipAttributeKeys` | Attribute-value lookup |
| `term_release_language` | sidecar | `releaseId`, `languageCode`, `normalizedTerm` | Lexical retrieval |
| `term_concept` | sidecar | `releaseId`, `conceptId` | Concept expansion |
| `term_rank` | sidecar | `releaseId`, `languageCode`, `termRank` | Candidate ordering |
| `term_area_lookup` | sidecar | `releaseId`, `languageCode`, `areaTags` | Area-scoped hybrid search and facets |
| `term_semantic_lookup` | sidecar | `releaseId`, `languageCode`, `semanticTagKey` | Semantic-tag filtering and facets |
| `term_text` | sidecar | `term`, `preferredTerm`, `fsn` | Optional MongoDB text index |
| `valueset_url_version_unique` | ValueSet registry | `url`, `version` | Immutable canonical identity |
| `valueset_status_updated` | ValueSet registry | `status`, `updatedAt` | Operational listing |
| `grounding_review_unique` | grounding reviews | `reviewId` | Idempotent reviewer decision |
| `grounding_source_updated` | grounding reviews | `sourceRef`, `reviewedAt` | Source audit history |
| `grounding_ancestor_query` | grounding reviews | `acceptedAncestorIds`, `reviewedAt` | Reviewed corpus query |
| `gold_note_id_unique` | gold notes | `id` | Idempotent fixture identity |
| `usage_event_at` | usage events | `at` | Recent-event listing |
| `usage_event_type_at` | usage events | `eventType`, `at` | Per-event-type analytics |
| `usage_event_tenant_at` | usage events | `tenantId`, `at` | Per-tenant analytics |

Atlas Search is intentionally not required. The baseline pack preserves parity across MongoDB deployments.

## Query Coverage

Every query the strategy issues is index-backed; there are no collection scans.
The exact counts below come from `explain` on a demo-scale dataset (25k concepts,
150k terms, 120 ValueSets, 5k grounding reviews):

| Query | Index | Documents examined |
|-------|-------|--------------------|
| Concept lookup, hierarchy, ECL | `concept_release_unique` / `ancestor_lookup` / `relationship_attribute_lookup` | matched only |
| Term search (hinted) | `term_release_language` | matched only |
| Term search (planner default) | `term_semantic_lookup` | entire release/language range |
| Semantic-tag or area filtering | `term_semantic_lookup` / `term_area_lookup` | filtered range |
| ValueSet browse | `valueset_url_version_desc` | page size |
| Grounded corpus by ancestor | `grounding_ancestor_query` | matched only |
| Gold-note list and lookup | `gold_note_id_unique` | matched only |
| Readiness counts | `concept_release_unique` / `term_concept` / `valueset_release_status` | 0 (index-only) |

The one deliberate exception is the exact `count_documents({releaseId})` over the
term sidecar in `snomed_readiness`. It examines every index key for the release
(~32 ms at 150k terms) because an exact count cannot stop early. Readiness needs
exactness, and it is a diagnostic call rather than a hot path.

## Feature Matrix

| Feature | Required store(s) |
|---------|-------------------|
| Concept lookup, ECL, hierarchy, release history | Canonical |
| Search, typeahead, candidate grounding, retrieval benchmark | Canonical + sidecar |
| Semantic scope resolution (ECL and/or area) | Canonical |
| Note mapping, evidence graph, principal-diagnosis candidates | Canonical + sidecar |
| Vector retrieval and rank fusion | Sidecar + Atlas vector index (optional) |
| Named expansion and membership validation | Canonical + ValueSet registry |
| Human-confirmed coding and ancestor corpus query | Canonical + grounding reviews |
| Gold-note graded benchmarks | Canonical + sidecar + gold notes |
| Product telemetry | Usage events |


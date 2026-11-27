---
sidebar_position: 4
---

# SNOMED CT Data Model

`snomedct.mongodb` stores the official JSON release as canonical documents and derives a term sidecar for search.

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

Atlas Search is intentionally not required. The baseline pack preserves parity across MongoDB deployments.

## Feature Matrix

| Feature | Canonical only | Canonical + sidecar |
|---------|----------------|---------------------|
| Concept lookup | Yes | Yes |
| Basic ECL/subsumption | Yes | Yes |
| Release diff | Yes | Yes |
| Terminology search | No | Yes |
| Clinical mention grounding | No | Yes |
| Benchmark retrieval candidates | No | Yes |

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
| `search` | `q`, `language`, `release_id`, `limit` | sidecar | Yes |
| `hybrid_search` | `q`, `ancestor_id`, `area_tag`, `semantic_tag_key` | sidecar | Yes |
| `semantic_facets` | `q`, `ancestor_id`, `language`, `release_id` | sidecar | Yes |
| `ground` | `mentions` or `text`, `language`, `release_id` | sidecar | Yes |

## Search Pipeline

The baseline search strategy is `normalized_regex`:

1. Normalize the query by lowercasing, removing diacritics, stripping punctuation, and collapsing whitespace.
2. Match sidecar `normalizedTerm` for the requested release and language.
3. Score exact matches above prefix matches above contains matches.
4. Sort by score, `termRank`, and term.
5. Return sidecar candidates with concept IDs, descriptions, FSN, preferred term, semantic tag, roots, and area tags.

This is deterministic and works on MongoDB Atlas and non-Atlas MongoDB. More advanced Atlas Search or vector candidate generation can be added later as optional strategies while preserving the canonical collection.

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

The planner deliberately does not claim full transitive relationship reasoning or attribute type/value subsumption yet. Those cases require either runtime joins that may be expensive on full releases or a deliberate materialized relationship strategy.

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

## Readiness

Use `snomed_readiness` or `/api/domains/snomedct/readiness` to confirm:

- canonical concept count
- active concept count
- sidecar enabled/ready status
- whether descendant arrays are retained
- feature availability for lookup, ECL, search, and grounding

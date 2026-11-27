---
sidebar_position: 1
---

# SNOMED CT on MongoDB Strategy

The **SNOMED CT on MongoDB** strategy (`snomedct.mongodb`) is \{kehrnel\}'s terminology persistence strategy for licensed SNOMED CT JSON releases.

It stores the official release as canonical MongoDB documents and derives a term sidecar for search, clinical mention grounding, and benchmark retrieval.

## Overview

The strategy has two stores:

| Store | Collection | Required | Purpose |
|-------|------------|----------|---------|
| Canonical concepts | `collections.concepts` | Yes | Official concept payload plus normalized release fields, ancestors, parents, relationships, and relationship attribute keys |
| Term sidecar | `collections.terms` | Default-on | Description-level search projection by language, preferred term, FSN, semantic tag, rank, parents, and ancestors |

The canonical collection is enough for lookup, hierarchy navigation, basic ECL/subsumption, readiness checks, and release diffs. The sidecar is required for terminology search and NLP grounding.

## Why MongoDB

SNOMED CT concepts are document-shaped. A concept naturally contains descriptions, language refsets, relationships, hierarchy fields, module metadata, and release metadata. MongoDB lets the strategy preserve that official JSON shape while adding operational indexes.

The baseline pack only requires MongoDB collections and B-tree/text indexes. It works with MongoDB Atlas and non-Atlas MongoDB. Atlas Search or vector retrieval can be added later without changing the canonical collection contract.

## Use Cases

| Use case | Strategy support |
|----------|------------------|
| Licensed tenant release ingestion | Customer stages JSON locally, then `snomed_list_releases`, `snomed_ingest_release` |
| Release update review | `snomed_inspect_release`, `snomed_diff_release` |
| Terminology search and navigation | `snomed_search`, lookup, hierarchy APIs, ECL subset |
| MongoDB-native terminology APIs | Hybrid search, semantic facets, relationship search, value-set expansion |
| Clinical note grounding | `snomed_ground_note` and `/api/domains/snomedct/ground` |
| LLM benchmark retrieval | Term sidecar candidates with deterministic ranking |

## API Placement

| Layer | Responsibility |
|-------|----------------|
| Core | Environment activation and universal dispatch |
| Domain | `/api/domains/snomedct/*` search, lookup, ECL, grounding, readiness |
| Strategy | Local release discovery, inspect, diff, ingest, sidecar rebuild, index creation |
| MongoDB | Canonical and sidecar collections, tenant database, indexes |

## Maturity

Manifest maturity: **preview** (`0.1.0`).

Implemented: transform, local release discovery, inspect, diff, ingest, rebuild sidecar, ensure indexes, lookup, ECL subset/subsumption, hierarchy expansion, value-set expansion, relationship search, hybrid search, semantic facets, grounding, readiness, domain API registration, and strategy-pack validation.

Not yet complete enough to call "final": full ECL transitive relationship semantics, production Atlas Search/vector specs, large-release soak tests against the official 3.4 GB file, and end-to-end MongoDB validation with real tenant bindings.

## Next Steps

- [Configuration](./configuration.md)
- [CLI and API workflows](./workflows.md)
- [Data model](./data-model.md)
- [Search and grounding](./search-grounding.md)

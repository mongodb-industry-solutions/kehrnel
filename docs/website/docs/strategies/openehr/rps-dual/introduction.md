---
sidebar_position: 1
---

# openEHR RPS Dual Strategy

The **RPS Dual** (Reversed-Path Storage, Dual Collection) strategy is \{kehrnel\}'s primary persistence strategy for openEHR Clinical Data Repositories.

This page is strategy-specific. It describes behavior for `openehr.rps_dual`, not cross-strategy platform guarantees.

## Overview

RPS Dual solves a fundamental challenge in openEHR storage: efficiently serving both **patient-centric** and **population-wide** queries from a single system.

## API Placement in the Layered Model

For this strategy, API responsibilities are split as follows:

- **Core layer**: strategy registry and environment activation (`/strategies`, `/environments/{env_id}/...`)
- **Common layer**: auth, error format, and HTTP conventions
- **Domain layer**: canonical openEHR operations (`/api/domains/openehr/...`)
- **Strategy layer**: RPS Dual operations (`/api/strategies/openehr/rps_dual/...`)

Practical implication: clients keep using domain APIs for clinical operations, while RPS Dual endpoints are used for strategy configuration, ingest support, and synthetic workflows.

### The Challenge

Traditional approaches face trade-offs:

| Approach | Patient Queries | Population Queries |
|----------|-----------------|-------------------|
| Shredded/Relational | Good | Expensive JOINs |
| Canonical Documents | Good | Full collection scans |
| Separate Analytics | N/A | Requires ETL sync |

### The Solution

RPS Dual maintains two synchronized collections:

| Collection | Purpose | Index Type |
|------------|---------|------------|
| `compositions_rps` | Full canonical storage | B-tree (EHR ID) |
| `compositions_search` | Slim search projection | Atlas Search |

The query engine automatically routes to the optimal collection based on query scope.

## Performance

Internal benchmark sample at billion-document scale:

| Query Type | Median | P90 |
|------------|--------|-----|
| Patient-scoped | 5ms | 19ms |
| Cross-patient | 13ms | 380ms |

## Key Innovations

### Reversed-Path Encoding

Archetype paths are reversed and numerically encoded:

```
Original: content[0]/data/events[0]/data/items[at0004]
Reversed: items[at0004]/data/events[0]/data/content[0]
Encoded:  -4.13.12.11.15
```

This enables prefix matching for AQL path queries.

### AT-Code Compression

Human-readable `at` codes are encoded as negative integers:

| Code | Encoded |
|------|---------|
| at0001 | -1 |
| at0004 | -4 |
| at0033 | -33 |

### Archetype ID Mapping

Full archetype IDs are mapped to sequential integers via a dictionary:

```
openEHR-EHR-OBSERVATION.blood_pressure.v2 → 42
openEHR-EHR-COMPOSITION.encounter.v1 → 15
```

### Dual-Index Strategy

- **B-tree indexes**: For patient-scoped queries with exact EHR ID matching
- **Atlas Search**: For cross-patient queries with path wildcards

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                  Canonical COMPOSITION               │
└──────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────┐
│              CompositionFlattener                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │Path Reversal│→ │Code Encoding│→ │ Projection  │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  │
└──────────────────────────────────────────────────────┘
              │                           │
              ▼                           ▼
┌─────────────────────────┐  ┌─────────────────────────┐
│   compositions_rps      │  │  compositions_search    │
│   ┌─────────────────┐   │  │   ┌─────────────────┐   │
│   │ _id             │   │  │   │ _id             │   │
│   │ ehr_id          │   │  │   │ ehr_id          │   │
│   │ comp_id         │   │  │   │ comp_id         │   │
│   │ v               │   │  │   │                 │   │
│   │ time_c          │   │  │   │ sort_time       │   │
│   │ tid             │   │  │   │ tid             │   │
│   │ cn: [ ... ]     │   │  │   │ sn: [ ... ]     │   │
│   └─────────────────┘   │  │   └─────────────────┘   │
│        B-tree Index     │  │      Atlas Search       │
└─────────────────────────┘  └─────────────────────────┘
```

## How Kehrnel Loads This Strategy

Kehrnel wires `openehr.rps_dual` in a few explicit steps:

1. The runtime discovers `manifest.json`, which exposes the strategy id,
   entrypoint, capabilities, maintenance ops, and UI metadata.
2. `strategy.py` loads `schema.json` and `defaults.json` and hydrates the
   manifest with that user-facing config contract.
3. Activation merges defaults with any user override, validates the result, and
   normalizes it through `config.py`.
4. `plan` and `apply` use `spec.json` plus the active config to materialize
   collections, B-tree indexes, and the search index definition.
5. `ingest`, `transform`, and query compilation reuse the same normalized config
   so collection names, field labels, path encoding, and code strategies stay
   aligned.

That shared configuration flow is the reason `defaults.json`, `schema.json`,
`config.py`, `spec.json`, and `strategy.py` need to remain congruent.

## Configuration Baseline

`defaults.json` is the recommended starting point for activation. Users can
override it, but the default structure is the baseline that the runtime
understands:

```json
{
  "collections": {
    "compositions": {
      "name": "compositions_rps",
      "encodingProfile": "profile.codedpath"
    },
    "search": {
      "name": "compositions_search",
      "encodingProfile": "profile.search_shortcuts",
      "enabled": true,
      "atlasIndex": {
        "name": "search_nodes_index",
        "definition": "file://bundles/searchIndex/searchIndex.json"
      }
    },
    "codes": {
      "name": "_codes",
      "seed": "file://bundles/dictionaries/_codes.json"
    },
    "shortcuts": {
      "name": "_shortcuts",
      "seed": "file://bundles/shortcuts/shortcuts.json"
    }
  },
  "ids": {
    "ehr_id": "string",
    "composition_id": "objectid"
  },
  "paths": {
    "separator": "."
  },
  "fields": {
    "document": {
      "ehr_id": "ehr_id",
      "comp_id": "comp_id",
      "tid": "tid",
      "v": "v",
      "time_committed": "time_c",
      "sort_time": "sort_time",
      "cn": "cn",
      "sn": "sn"
    },
    "node": {
      "p": "p",
      "pi": "pi",
      "data": "data"
    }
  },
  "transform": {
    "apply_shortcuts": true,
    "coding": {
      "arcodes": { "strategy": "sequential" },
      "atcodes": { "strategy": "negative_int", "store_original": false }
    }
  },
  "bootstrap": {
    "dictionariesOnActivate": {
      "codes": "ensure",
      "shortcuts": "seed"
    }
  }
}
```

Current supported config surface:

- path separator is fixed to `.`
- supported encoding profiles are `profile.codedpath` and
  `profile.search_shortcuts`
- the search-side document carries `_id`, `ehr_id`, `comp_id`, `tid`,
  `sort_time`, and `sn`

For the full packaged dual-collection example, add a small activation overlay
for `transform.mappings` so the search-side projection and the Atlas Search
definition are both derived from the same mapping source. The detailed workflow
and config reference live in:

- [CLI Workflows](/docs/strategies/openehr/rps-dual/cli-workflows)
- [Configuration](/docs/strategies/openehr/rps-dual/configuration)

## Use Cases

### When to Use RPS Dual

- Clinical Data Repositories requiring both patient care and research queries
- High-volume systems with millions of compositions
- Applications needing sub-second query response times
- Systems requiring full openEHR compliance

### When Not to Use

- Simple document storage without query requirements
- Non-openEHR data models
- Applications where eventual consistency is unacceptable

## Next Steps

- [CLI Workflows](/docs/strategies/openehr/rps-dual/cli-workflows) - Practical transform/map/ingest/validate commands
- [Data Model](/docs/strategies/openehr/rps-dual/data-model) - Detailed document structure
- [Query Translation](/docs/strategies/openehr/rps-dual/query-translation) - AQL to MQL compilation
- [Configuration](/docs/strategies/openehr/rps-dual/configuration) - Full configuration reference

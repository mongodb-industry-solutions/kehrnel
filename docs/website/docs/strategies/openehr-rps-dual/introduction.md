---
sidebar_position: 1
---

# openEHR RPS Dual Strategy

The **RPS Dual** (Reversed-Path Storage, Dual Collection) strategy is \{kehrnel\}'s primary persistence strategy for openEHR Clinical Data Repositories.

## Overview

RPS Dual solves a fundamental challenge in openEHR storage: efficiently serving both **patient-centric** and **population-wide** queries from a single system.

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

Tested at billion-document scale:

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
│   │ tid             │   │  │   │ tid             │   │
│   │ n: { ... }      │   │  │   │ sn: [ ... ]     │   │
│   └─────────────────┘   │  │   └─────────────────┘   │
│        B-tree Index     │  │      Atlas Search       │
└─────────────────────────┘  └─────────────────────────┘
```

## Configuration

```json
{
  "database": "kehrnel_db",
  "collections": {
    "compositions": {
      "name": "compositions_rps",
      "encodingProfile": "profile.codedpath"
    },
    "search": {
      "name": "compositions_search",
      "enabled": true,
      "atlasIndex": { "name": "search_nodes_index" }
    },
    "codes": { "name": "_codes", "mode": "extend" },
    "ehr": { "name": "ehr" },
    "contributions": { "name": "contributions" }
  },
  "transform": {
    "coding": {
      "arcodes": { "strategy": "sequential" },
      "atcodes": { "strategy": "negative_int" }
    }
  }
}
```

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

- [Data Model](/docs/strategies/openehr-rps-dual/data-model) - Detailed document structure
- [Query Translation](/docs/strategies/openehr-rps-dual/query-translation) - AQL to MQL compilation
- [Configuration](/docs/strategies/openehr-rps-dual/configuration) - Full configuration reference

---
sidebar_position: 4
---

# Composition Flattening

The CompositionFlattener transforms hierarchical openEHR canonical compositions into the semi-flattened document structure used by \{kehrnel\}.

## Overview

```
┌────────────────────────────────────────────────────────────┐
│                 Canonical COMPOSITION                      │
│  {                                                         │
│    "_type": "COMPOSITION",                                 │
│    "content": [                                            │
│      {                                                     │
│        "_type": "OBSERVATION",                             │
│        "data": {                                           │
│          "events": [...]                                   │
│        }                                                   │
│      }                                                     │
│    ]                                                       │
│  }                                                         │
└────────────────────────────────────────────────────────────┘
                            │
                            ▼
               ┌─────────────────────┐
               │ CompositionFlattener│
               │  ┌───────────────┐  │
               │  │ Path Reversal │  │
               │  │ Code Encoding │  │
               │  │ Value Extract │  │
               │  │ Search Nodes  │  │
               │  └───────────────┘  │
               └─────────────────────┘
                    │           │
                    ▼           ▼
┌─────────────────────────┐  ┌─────────────────────────────┐
│   Base Document         │  │   Search Document           │
│   (compositions_rps)    │  │   (compositions_search)     │
└─────────────────────────┘  └─────────────────────────────┘
```

## Transformation Steps

### 1. Parse Canonical Structure

The flattener walks the canonical JSON tree, identifying:

- Entry types (OBSERVATION, EVALUATION, etc.)
- Data value nodes (DV_QUANTITY, DV_CODED_TEXT, etc.)
- Archetype constraints (archetype_node_id)
- Array indices

### 2. Build Path Map

For each leaf node, construct the full path:

```
content[0]/data[at0001]/events[at0006,0]/data[at0003]/items[at0004]/value
```

### 3. Reverse and Encode Path

Transform the path for storage:

```
Original:  content[0]/data[at0001]/events[at0006,0]/data[at0003]/items[at0004]
Reversed:  items[at0004].data[at0003].events[at0006].data[at0001].content[0]
Encoded:   11.-4.13.-3.12.-6.13.-1.15
```

### 4. Extract Value Data

Transform openEHR data values to compact form:

| Data Type | Canonical | Flattened |
|-----------|-----------|-----------|
| DV_QUANTITY | `{ "_type": "DV_QUANTITY", "magnitude": 120, "units": "mm[Hg]" }` | `{ "m": 120, "u": "mm[Hg]" }` |
| DV_CODED_TEXT | `{ "_type": "DV_CODED_TEXT", "value": "Normal", "defining_code": {...} }` | `{ "val": "Normal", "dc": {...} }` |
| DV_DATE_TIME | `{ "_type": "DV_DATE_TIME", "value": "2025-01-15T10:30:00Z" }` | `{ "val": "2025-01-15T10:30:00Z" }` |

### 5. Build Base Document

Assemble the primary document:

```json
{
  "_id": "composition-uuid::kehrnel::1",
  "ehr_id": "patient-001",
  "tid": 42,
  "cv": 1,
  "ct": "2025-01-15T10:30:00Z",
  "n": {
    "11.-4.13.-3.12.-6.13.-1.15.42": {
      "v": { "m": 120, "u": "mm[Hg]" }
    }
  }
}
```

### 6. Build Search Document

Extract nodes for the search collection:

```json
{
  "_id": "composition-uuid::kehrnel::1",
  "ehr_id": "patient-001",
  "tid": 42,
  "sn": [
    {
      "p": "11.-4.13.-3.12.-6.13.-1.15.42",
      "data": { "m": 120, "u": "mm[Hg]" }
    }
  ]
}
```

## Configuration

### Flattener Mappings

The flattener uses mapping configuration (`flattener_mappings.jsonc`):

```json
{
  "data_types": {
    "DV_QUANTITY": {
      "extract": ["magnitude", "units", "precision"],
      "compact": { "magnitude": "m", "units": "u", "precision": "p" }
    },
    "DV_CODED_TEXT": {
      "extract": ["value", "defining_code"],
      "compact": { "value": "val", "defining_code": "dc" }
    }
  },
  "path_encoding": {
    "content": 15,
    "data": 13,
    "events": 12,
    "items": 11,
    "value": 10
  }
}
```

### Coding Options

```json
{
  "transform": {
    "coding": {
      "arcodes": { "strategy": "sequential" },
      "atcodes": { "strategy": "negative_int" }
    }
  }
}
```

## CLI Usage

### Transform Single Composition

```bash
kehrnel-transform flatten composition.json -o flattened.json
```

### Preview Transformation

```bash
kehrnel-transform flatten composition.json | jq '.base.n | keys'
```

## API Usage

### Preview Endpoint

```bash
curl -X POST "http://localhost:8000/api/strategies/openehr/rps_dual/ingest/preview" \
  -H "Content-Type: application/json" \
  -d @composition.json
```

Response:

```json
{
  "base": {
    "_id": "...",
    "n": { ... }
  },
  "search": {
    "_id": "...",
    "sn": [ ... ]
  },
  "stats": {
    "nodes_extracted": 15,
    "search_nodes": 8,
    "paths_encoded": 15
  }
}
```

## Reverse Transformation

The flattener can reconstruct canonical from flattened:

```bash
# Via API
curl -X POST "http://localhost:8000/api/strategies/openehr/rps_dual/transform/expand" \
  -H "Content-Type: application/json" \
  -d @flattened.json
```

### Composition Retrieval

The composition endpoint automatically unflattens:

```bash
curl "http://localhost:8000/api/domains/openehr/ehr/patient-001/composition/uuid"
```

Or explicitly from flattened storage:

```bash
curl "http://localhost:8000/api/domains/openehr/ehr/patient-001/composition-unflatten/uuid"
```

## Performance

### Typical Transformation Times

| Composition Size | Transform Time |
|------------------|----------------|
| 10KB canonical | ~5ms |
| 50KB canonical | ~15ms |
| 100KB canonical | ~30ms |

### Storage Reduction

| Canonical | Flattened (Base) | Flattened (Search) |
|-----------|------------------|-------------------|
| 10KB | ~3KB (70% reduction) | ~1KB |
| 50KB | ~15KB (70% reduction) | ~5KB |

## Related

- [Reversed Paths](/docs/concepts/reversed-paths) - Path encoding
- [Data Model](/docs/strategies/openehr-rps-dual/data-model) - Document structure
- [Transform CLI](/docs/cli/transform) - Command-line usage

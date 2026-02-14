---
sidebar_position: 2
---

# Dual Collection Architecture

\{kehrnel\}'s dual collection architecture maintains two synchronized views of composition data, each optimized for different query patterns.

## Overview

```
┌────────────────────────────────────────────────────────────┐
│                 Canonical COMPOSITION                      │
└────────────────────────────────────────────────────────────┘
                          │
                          ▼
               ┌─────────────────────┐
               │ CompositionFlattener│
               └─────────────────────┘
                    │           │
                    ▼           ▼
┌─────────────────────────┐  ┌─────────────────────────────┐
│   compositions_rps      │  │    compositions_search      │
│   ┌─────────────────┐   │  │    ┌─────────────────────┐  │
│   │ Full flattened  │   │  │    │ Slim projection     │  │
│   │ documents       │   │  │    │ for search          │  │
│   └─────────────────┘   │  │    └─────────────────────┘  │
│   Index: B-tree         │  │    Index: Atlas Search     │
│   (ehr_id)              │  │    (sn.p, sn.data)         │
└─────────────────────────┘  └─────────────────────────────┘
          │                              │
          ▼                              ▼
┌─────────────────────────┐  ┌─────────────────────────────┐
│ Patient-Scoped Queries  │  │  Cross-Patient Queries      │
│ $match pipeline         │  │  $search pipeline           │
│ O(log n + compositions) │  │  O(log n)                   │
└─────────────────────────┘  └─────────────────────────────┘
```

## Why Two Collections?

### The Challenge

Different query patterns require different optimization strategies:

| Query Type | Pattern | Challenge |
|------------|---------|-----------|
| Patient-scoped | `WHERE ehr_id = 'X'` | Fast with B-tree, but cross-patient is full scan |
| Cross-patient | `WHERE blood_pressure > 140` | Requires full-text search across all documents |

### The Solution

Maintain both:
- **Primary store** (`compositions_rps`): Full data, B-tree indexed
- **Search store** (`compositions_search`): Slim projection, Atlas Search indexed

## Primary Store (compositions_rps)

Contains complete flattened compositions.

### Document Structure

```json
{
  "_id": "composition-uuid::kehrnel::1",
  "ehr_id": "patient-001",
  "tid": 42,
  "pv": 0,
  "cv": 1,
  "ct": "2025-01-15T10:30:00Z",
  "n": {
    "11.-4.13.-3.12.-6.13.-1.15.42": {
      "v": { "m": 120, "u": "mm[Hg]" },
      "meta": { ... }
    },
    "11.-5.13.-3.12.-6.13.-1.15.42": {
      "v": { "m": 80, "u": "mm[Hg]" }
    }
  }
}
```

### Indexes

```javascript
// Primary lookup
db.compositions_rps.createIndex({ "ehr_id": 1, "_id": 1 })

// Template-scoped
db.compositions_rps.createIndex({ "ehr_id": 1, "tid": 1 })

// Version history
db.compositions_rps.createIndex({ "ehr_id": 1, "cv": -1 })
```

### Use Cases

- Patient record retrieval
- Composition versioning
- Full document reconstruction
- Patient timeline queries

## Search Store (compositions_search)

Contains slim projections optimized for Atlas Search.

### Document Structure

```json
{
  "_id": "composition-uuid::kehrnel::1",
  "ehr_id": "patient-001",
  "tid": 42,
  "sn": [
    {
      "p": "11.-4.13.-3.12.-6.13.-1.15.42",
      "data": { "m": 120, "u": "mm[Hg]" }
    },
    {
      "p": "11.-5.13.-3.12.-6.13.-1.15.42",
      "data": { "m": 80, "u": "mm[Hg]" }
    }
  ]
}
```

### Atlas Search Index

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "ehr_id": { "type": "string" },
      "tid": { "type": "number" },
      "sn": {
        "type": "embeddedDocuments",
        "fields": {
          "p": { "type": "string", "analyzer": "keyword" },
          "data": { "type": "document", "dynamic": true }
        }
      }
    }
  }
}
```

### Use Cases

- Population health queries
- Clinical research
- Quality metrics
- Alert/trigger systems

## Synchronization

Both collections are updated atomically during ingestion:

```python
async def ingest_composition(canonical, ehr_id):
    # Transform
    base, search = flattener.transform_composition({
        "canonicalJSON": canonical,
        "ehr_id": ehr_id
    })

    # Atomic writes
    async with client.start_session() as session:
        async with session.start_transaction():
            await compositions_rps.insert_one(base, session=session)
            if search:
                await compositions_search.insert_one(search, session=session)
```

## Query Routing

The query engine automatically selects the appropriate collection:

```python
def should_use_search(query_ast, ehr_id):
    # Patient-scoped → use primary
    if ehr_id:
        return False

    # Cross-patient predicates → use search
    if has_cross_patient_predicates(query_ast):
        return True

    # Default to primary
    return False
```

## Configuration

### Enable/Disable Search Collection

```json
{
  "collections": {
    "search": {
      "name": "compositions_search",
      "enabled": true
    }
  }
}
```

### Force Search Strategy

For testing:

```bash
curl -X POST "http://localhost:8000/api/domains/openehr/query/aql?force_search=true" \
  -H "Content-Type: text/plain" \
  -d "SELECT c/uid/value FROM EHR e CONTAINS COMPOSITION c"
```

## Performance Comparison

| Metric | Primary ($match) | Search ($search) |
|--------|------------------|------------------|
| Patient lookup | 5ms | N/A |
| Cross-patient | Minutes (scan) | 13ms |
| Storage per doc | ~3KB | ~1KB |
| Index type | B-tree | Atlas Search |

## Storage Trade-offs

### Advantages

- Optimized for both query patterns
- No cross-patient full scans
- Patient queries use efficient B-tree

### Considerations

- ~30% additional storage for search collection
- Atlas Search required for cross-patient
- Synchronization overhead during writes

## Related

- [Reversed Paths](/docs/concepts/reversed-paths) - Path encoding
- [Data Model](/docs/strategies/openehr-rps-dual/data-model) - Document structure
- [Query Translation](/docs/strategies/openehr-rps-dual/query-translation) - Pipeline generation

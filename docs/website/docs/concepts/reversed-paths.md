---
sidebar_position: 1
---

# Reversed Path Encoding

Reversed Path Encoding is a key innovation in \{kehrnel\} that enables efficient path-based queries on hierarchical openEHR data.

## The Problem

openEHR compositions are deeply nested hierarchical documents. A typical path might be:

```
content[openEHR-EHR-OBSERVATION.blood_pressure.v2,0]/data[at0001]/events[at0006,0]/data[at0003]/items[at0004]/value
```

This creates challenges for MongoDB:
- Deep nesting requires complex dot notation
- Array indices make paths unpredictable
- Wildcard queries on nested paths are expensive

## The Solution

Reversed Path Encoding transforms hierarchical paths into flat, queryable keys:

```
Original:  content[0]/data/events[0]/data/items[at0004]
           ↓ Reverse order
Reversed:  items[at0004].data.events[0].data.content[0]
           ↓ Encode segments
Final:     -4.13.12.11.15
```

### Why Reverse?

Reversing puts the most specific (leaf) path segments first. This enables:

1. **Prefix Matching**: Query `items[at0004]/*` becomes prefix query on `-4.*`
2. **Efficient Indexing**: Atlas Search can use prefix matching on `sn.p`
3. **Consistent Ordering**: Same data types cluster together

## Encoding Rules

### Segment Encoding

| Segment Type | Encoding | Example |
|-------------|----------|---------|
| AT codes | Negative integers | `at0004` → `-4` |
| Archetype IDs | Sequential integers | `blood_pressure.v2` → `42` |
| Structural names | Fixed integers | `content` → `15`, `data` → `13` |

### Complete Example

```
AQL Path:
content[openEHR-EHR-OBSERVATION.blood_pressure.v2,0]/data[at0001]/events[at0006,0]/data[at0003]/items[at0004]/value

Step 1: Parse segments
- content, archetype=blood_pressure.v2, index=0
- data, at=at0001
- events, at=at0006, index=0
- data, at=at0003
- items, at=at0004
- value

Step 2: Reverse order
- value
- items[at0004]
- data[at0003]
- events[at0006]
- data[at0001]
- content[archetype=42]

Step 3: Encode
- value → (handled separately as leaf)
- items[at0004] → 11.-4
- data[at0003] → 13.-3
- events[at0006] → 12.-6
- data[at0001] → 13.-1
- content[42] → 15.42

Final: 11.-4.13.-3.12.-6.13.-1.15.42
```

## Storage Structure

### In compositions_rps

Paths become object keys:

```json
{
  "n": {
    "11.-4.13.-3.12.-6.13.-1.15.42": {
      "v": { "m": 120, "u": "mm[Hg]" }
    }
  }
}
```

### In compositions_search

Paths become array elements for Atlas Search:

```json
{
  "sn": [
    {
      "p": "11.-4.13.-3.12.-6.13.-1.15.42",
      "data": { "m": 120, "u": "mm[Hg]" }
    }
  ]
}
```

## Query Translation

### Patient-Scoped Query

```aql
SELECT o/data/events/data/items[at0004]/value/magnitude
FROM EHR e CONTAINS OBSERVATION o
WHERE e/ehr_id/value = 'patient-001'
```

Translates to:

```javascript
db.compositions_rps.aggregate([
  { $match: { "ehr_id": "patient-001" } },
  { $project: { "magnitude": "$n.11.-4.13.12.13.v.m" } }
])
```

### Cross-Patient Query

```aql
SELECT o/data/events/data/items[at0004]/value/magnitude
FROM EHR e CONTAINS OBSERVATION o
WHERE o/.../magnitude > 140
```

Translates to:

```javascript
db.compositions_search.aggregate([
  {
    $search: {
      index: "search_nodes_index",
      embeddedDocument: {
        path: "sn",
        operator: {
          compound: {
            must: [
              { text: { path: "sn.p", query: "11.-4*" } },
              { range: { path: "sn.data.m", gt: 140 } }
            ]
          }
        }
      }
    }
  }
])
```

## Code Dictionaries

The `_codes` collection maintains mappings:

```json
// Archetype mapping
{
  "_id": "arcode:openEHR-EHR-OBSERVATION.blood_pressure.v2",
  "code": 42,
  "type": "archetype"
}

// Template mapping
{
  "_id": "template:vital_signs.v1",
  "code": 15,
  "type": "template"
}
```

### Dictionary Operations

```bash
# Ensure dictionaries are initialized
curl -X POST "http://localhost:8000/environments/dev/extensions/openehr.rps_dual/ensure_dictionaries" \
  -H "X-API-Key: admin-key"
```

## Benefits

1. **Flat Structure**: All paths at the same document level
2. **Prefix Queries**: Wildcard matching on path prefixes
3. **Compact Storage**: Numeric encoding reduces size
4. **Index Efficiency**: B-tree and Atlas Search optimized
5. **Consistent Keys**: Deterministic path → key mapping

## Related

- [Data Model](/docs/strategies/openehr-rps-dual/data-model) - Full document structure
- [Query Translation](/docs/strategies/openehr-rps-dual/query-translation) - How queries use paths
- [Flattening](/docs/concepts/flattening) - Transformation process

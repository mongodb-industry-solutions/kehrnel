---
sidebar_position: 2
---

# Data Model

The RPS Dual strategy transforms canonical openEHR compositions into an optimized document structure designed for efficient querying.

## Document Structure

### Primary Store (compositions_rps)

The full flattened composition with all data:

```json
{
  "_id": "composition-uuid::kehrnel::1",
  "ehr_id": "patient-001",
  "tid": 42,
  "pv": 1,
  "cv": 1,
  "ct": "2025-01-15T10:30:00Z",
  "n": {
    "13.12.11.-4": {
      "v": { "m": 120, "u": "mm[Hg]" }
    },
    "13.12.11.-5": {
      "v": { "m": 80, "u": "mm[Hg]" }
    }
  }
}
```

### Search Store (compositions_search)

Slim projection optimized for Atlas Search:

```json
{
  "_id": "composition-uuid::kehrnel::1",
  "ehr_id": "patient-001",
  "tid": 42,
  "sn": [
    {
      "p": "13.12.11.-4",
      "data": { "m": 120, "u": "mm[Hg]" }
    },
    {
      "p": "13.12.11.-5",
      "data": { "m": 80, "u": "mm[Hg]" }
    }
  ]
}
```

## Field Reference

### Root Fields

| Field | Type | Description |
|-------|------|-------------|
| `_id` | string | Version UID (uuid::system::version) |
| `ehr_id` | string | EHR identifier |
| `tid` | integer | Template ID (encoded) |
| `pv` | integer | Previous version number |
| `cv` | integer | Current version number |
| `ct` | datetime | Commit time |

### Node Structure (n)

The `n` field contains encoded path keys mapping to node data:

```json
{
  "n": {
    "<encoded_path>": {
      "v": { ... },      // Value data
      "meta": { ... }    // Metadata (optional)
    }
  }
}
```

### Search Nodes (sn)

The `sn` array contains flattened search nodes:

```json
{
  "sn": [
    {
      "p": "<encoded_path>",  // Path string for indexing
      "data": { ... }         // Value data for search
    }
  ]
}
```

## Path Encoding

### Reversal

Paths are reversed so leaf nodes come first:

```
Original:  content[0]/data/events[0]/data/items[at0004]
Reversed:  items[at0004].data.events[0].data.content[0]
```

### Numeric Encoding

Path segments are encoded numerically:

| Segment | Encoding |
|---------|----------|
| `content` | 15 (archetype code) |
| `data` | 13 |
| `events` | 12 |
| `items` | 11 |
| `at0004` | -4 |

Final path: `13.12.11.-4.15`

## Value Encoding

### DV_QUANTITY

```json
{
  "v": {
    "m": 120,           // magnitude
    "u": "mm[Hg]",      // units
    "p": 0              // precision (optional)
  }
}
```

### DV_CODED_TEXT

```json
{
  "v": {
    "val": "Normal",    // value string
    "dc": {
      "tid": "local",   // terminology ID
      "cs": "at0010"    // code string
    }
  }
}
```

### DV_DATE_TIME

```json
{
  "v": {
    "val": "2025-01-15T10:30:00Z"
  }
}
```

### DV_TEXT

```json
{
  "v": {
    "val": "Patient reports mild headache"
  }
}
```

### DV_BOOLEAN

```json
{
  "v": {
    "val": true
  }
}
```

## Code Dictionaries

### _codes Collection

Maintains mappings between human-readable codes and integers:

```json
{
  "_id": "arcode:openEHR-EHR-OBSERVATION.blood_pressure.v2",
  "code": 42,
  "type": "archetype"
}
```

```json
{
  "_id": "template:vital_signs.v1",
  "code": 15,
  "type": "template"
}
```

### Encoding Strategies

| Type | Strategy | Example |
|------|----------|---------|
| AT codes | negative_int | at0004 → -4 |
| Archetype IDs | sequential | 42, 43, 44... |
| Template IDs | sequential | 15, 16, 17... |

## Indexes

### compositions_rps

```javascript
// Patient-scoped queries
db.compositions_rps.createIndex({ "ehr_id": 1, "_id": 1 })

// Template-filtered queries
db.compositions_rps.createIndex({ "ehr_id": 1, "tid": 1 })
```

### compositions_search (Atlas Search)

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

## Collection Sizing

### Storage Estimates

| Canonical Size | Primary (RPS) | Search |
|----------------|---------------|--------|
| 10KB | ~3KB | ~1KB |
| 50KB | ~15KB | ~5KB |
| 100KB | ~30KB | ~10KB |

The flattened format typically achieves 60-70% storage reduction compared to canonical JSON.

## Related

- [Reversed Paths](/docs/concepts/reversed-paths) - Path encoding explained
- [Flattening](/docs/concepts/flattening) - Transformation process
- [Query Translation](/docs/strategies/openehr-rps-dual/query-translation) - How queries use this model

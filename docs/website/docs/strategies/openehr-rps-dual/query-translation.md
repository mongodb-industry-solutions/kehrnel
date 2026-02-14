---
sidebar_position: 3
---

# Query Translation

\{kehrnel\} translates AQL (Archetype Query Language) queries into MongoDB Aggregation Pipelines (MQL).

## Translation Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│                      AQL Query                              │
│  SELECT c/uid/value, o/data/events/data/items[at0004]/value │
│  FROM EHR e CONTAINS COMPOSITION c CONTAINS OBSERVATION o   │
│  WHERE o/data/.../magnitude > 140                           │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      AQL Parser                             │
│  Parse → AST (Abstract Syntax Tree)                         │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   Path Encoder                              │
│  Reverse paths, encode archetypes, encode at-codes          │
│  content[0]/data/events[0]/items[at0004] → 13.12.11.-4      │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   Strategy Router                           │
│  Has ehr_id? → $match (B-tree)                              │
│  No ehr_id?  → $search (Atlas Search)                       │
└─────────────────────────────────────────────────────────────┘
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
┌─────────────────────────┐   ┌─────────────────────────┐
│   $match Pipeline       │   │   $search Pipeline      │
│   (compositions_rps)    │   │   (compositions_search) │
└─────────────────────────┘   └─────────────────────────┘
```

## Strategy Selection

### Patient-Scoped ($match)

When `ehr_id` is known, the query targets `compositions_rps`:

```javascript
// AQL: WHERE e/ehr_id/value = 'patient-001'
[
  { "$match": { "ehr_id": "patient-001" } },
  { "$match": { "n.13.12.11.-4.v.m": { "$gt": 140 } } },
  { "$project": { "uid": "$_id", "magnitude": "$n.13.12.11.-4.v.m" } }
]
```

### Cross-Patient ($search)

Without `ehr_id`, the query uses Atlas Search on `compositions_search`:

```javascript
[
  {
    "$search": {
      "index": "search_nodes_index",
      "compound": {
        "must": [
          {
            "embeddedDocument": {
              "path": "sn",
              "operator": {
                "compound": {
                  "must": [
                    { "text": { "path": "sn.p", "query": "13.12.11.-4" } },
                    { "range": { "path": "sn.data.m", "gt": 140 } }
                  ]
                }
              }
            }
          }
        ]
      }
    }
  }
]
```

## Path Translation

### FROM/CONTAINS Clause

```aql
FROM EHR e
CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.encounter.v1]
  CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.blood_pressure.v2]
```

Produces archetype constraints:
- Template lookup for `encounter.v1` → tid filter
- Archetype ID `blood_pressure.v2` → archetype code `42`

### SELECT Paths

```aql
SELECT o/data/events/data/items[at0004]/value/magnitude AS systolic
```

Path transformation:
1. Parse: `o/data/events/data/items[at0004]/value/magnitude`
2. Resolve alias `o` → `openEHR-EHR-OBSERVATION.blood_pressure.v2`
3. Encode path: `n.13.12.11.-4.v.m`

### WHERE Predicates

```aql
WHERE o/data/events/data/items[at0004]/value/magnitude > 140
```

Translated to:
- $match: `{ "n.13.12.11.-4.v.m": { "$gt": 140 } }`
- $search: `{ "range": { "path": "sn.data.m", "gt": 140 } }`

## Supported AQL Features

### SELECT

| Feature | Support |
|---------|---------|
| Path expressions | ✅ Full |
| Aliases (AS) | ✅ Full |
| Functions (COUNT, MAX, etc.) | ⚠️ Partial |
| Nested paths | ✅ Full |

### FROM/CONTAINS

| Feature | Support |
|---------|---------|
| EHR containment | ✅ Full |
| COMPOSITION | ✅ Full |
| OBSERVATION, EVALUATION | ✅ Full |
| CLUSTER, ELEMENT | ✅ Full |
| Archetype predicates | ✅ Full |

### WHERE

| Feature | Support |
|---------|---------|
| Comparison operators | ✅ Full |
| AND, OR, NOT | ✅ Full |
| EXISTS | ✅ Full |
| LIKE (patterns) | ⚠️ Partial |
| MATCHES (terminology) | ⚠️ Partial |

### ORDER BY / LIMIT

| Feature | Support |
|---------|---------|
| ORDER BY single column | ✅ Full |
| ORDER BY multiple | ✅ Full |
| ASC/DESC | ✅ Full |
| LIMIT | ✅ Full |
| OFFSET | ✅ Full |

## Debug Endpoints

### View Generated Pipeline

```bash
curl -X POST "http://localhost:8000/api/domains/openehr/query/aql/mql" \
  -H "Content-Type: text/plain" \
  -d "SELECT c/uid/value FROM EHR e CONTAINS COMPOSITION c"
```

Response includes:
- Original AQL
- Parsed AST
- Generated MQL pipeline

### View Strategy Decision

```bash
curl "http://localhost:8000/api/domains/openehr/query/strategy/info?ehr_id=patient-001"
```

## Performance Characteristics

### Patient-Scoped

- Index: B-tree on `ehr_id`
- Complexity: O(log n) for lookup + O(compositions per patient)
- Typical: 5ms median

### Cross-Patient

- Index: Atlas Search
- Complexity: O(log n) for search
- Typical: 13ms median

## Related

- [AQL to MQL Concepts](/docs/concepts/aql-to-mql) - Detailed translation rules
- [Data Model](/docs/strategies/openehr-rps-dual/data-model) - Storage structure
- [Query API](/docs/api/overview) - API reference (see ReDoc)

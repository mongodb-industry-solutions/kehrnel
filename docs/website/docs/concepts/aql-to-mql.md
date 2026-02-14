---
sidebar_position: 3
---

# AQL to MQL Translation

\{kehrnel\} translates Archetype Query Language (AQL) queries into MongoDB Query Language (MQL) aggregation pipelines.

## Translation Overview

```
┌─────────────────────────────────────────────────────────────┐
│  AQL Query                                                  │
│  SELECT c/uid/value AS uid                                  │
│  FROM EHR e CONTAINS COMPOSITION c                          │
│  WHERE e/ehr_id/value = 'patient-001'                       │
└─────────────────────────────────────────────────────────────┘
                            │
         ┌──────────────────┼──────────────────┐
         ▼                  ▼                  ▼
    ┌─────────┐      ┌─────────────┐    ┌──────────┐
    │  Parse  │  →   │   Encode    │ →  │  Build   │
    │   AQL   │      │   Paths     │    │ Pipeline │
    └─────────┘      └─────────────┘    └──────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────┐
│  MQL Pipeline                                               │
│  [                                                          │
│    { "$match": { "ehr_id": "patient-001" } },               │
│    { "$project": { "uid": "$_id" } }                        │
│  ]                                                          │
└─────────────────────────────────────────────────────────────┘
```

## Clause Translation

### SELECT Clause

AQL SELECT expressions map to `$project` stages:

```aql
SELECT
  c/uid/value AS uid,
  c/name/value AS name,
  o/data/events/data/items[at0004]/value/magnitude AS systolic
```

```javascript
{
  $project: {
    uid: "$_id",
    name: "$n.name.v.val",
    systolic: "$n.11.-4.13.-3.12.-6.13.-1.15.42.v.m"
  }
}
```

### FROM/CONTAINS Clause

The FROM clause establishes containment relationships:

```aql
FROM EHR e
CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.encounter.v1]
  CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.blood_pressure.v2]
```

This produces:
1. Alias registration (`e`, `c`, `o`)
2. Archetype constraints (template ID filter)
3. Path prefix context for SELECT/WHERE

### WHERE Clause

WHERE predicates become `$match` or `$search` filters:

```aql
WHERE e/ehr_id/value = 'patient-001'
  AND o/data/.../magnitude > 140
```

**Patient-scoped (with ehr_id):**

```javascript
[
  { $match: { "ehr_id": "patient-001" } },
  { $match: { "n.11.-4.13.-3.12.-6.13.-1.15.42.v.m": { $gt: 140 } } }
]
```

**Cross-patient (without ehr_id):**

```javascript
[
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
]
```

### ORDER BY Clause

```aql
ORDER BY o/data/.../magnitude DESC
```

```javascript
{
  $sort: { "n.11.-4.13.-3.12.-6.13.-1.15.42.v.m": -1 }
}
```

### LIMIT/OFFSET

```aql
LIMIT 10 OFFSET 20
```

```javascript
[
  { $skip: 20 },
  { $limit: 10 }
]
```

## Operator Mapping

### Comparison Operators

| AQL | MQL |
|-----|-----|
| `=` | `$eq` |
| `!=` | `$ne` |
| `>` | `$gt` |
| `>=` | `$gte` |
| `<` | `$lt` |
| `<=` | `$lte` |

### Logical Operators

| AQL | MQL |
|-----|-----|
| `AND` | `$and` |
| `OR` | `$or` |
| `NOT` | `$not` |

### Special Operators

| AQL | MQL |
|-----|-----|
| `EXISTS` | `$exists` |
| `LIKE` | `$regex` |
| `MATCHES` | Custom terminology lookup |

## Path Resolution

### Alias Resolution

```aql
SELECT o/data/events/data/items[at0004]/value/magnitude
FROM EHR e
CONTAINS COMPOSITION c
  CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.blood_pressure.v2]
```

Resolution steps:
1. `o` → `OBSERVATION` with archetype `blood_pressure.v2`
2. Lookup archetype code: `42`
3. Encode path: `o/data/events/data/items[at0004]/value/magnitude`
4. Result: `n.11.-4.13.-3.12.-6.13.-1.15.42.v.m`

### Wildcard Paths

```aql
SELECT c/content[*]/name/value
```

Uses `$objectToArray` and `$filter`:

```javascript
[
  { $project: { entries: { $objectToArray: "$n" } } },
  { $unwind: "$entries" },
  { $match: { "entries.k": { $regex: "^15\\." } } }
]
```

## Complete Example

### AQL Query

```aql
SELECT
  e/ehr_id/value AS patient_id,
  c/uid/value AS composition_id,
  o/data/events/data/items[at0004]/value/magnitude AS systolic,
  o/data/events/data/items[at0005]/value/magnitude AS diastolic
FROM EHR e
CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.encounter.v1]
  CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.blood_pressure.v2]
WHERE o/data/events/data/items[at0004]/value/magnitude > 140
ORDER BY o/data/events/data/items[at0004]/value/magnitude DESC
LIMIT 100
```

### Generated MQL

```javascript
[
  // Cross-patient search
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
  },
  // Template filter
  { $match: { "tid": 15 } },
  // Sort
  { $sort: { "n.11.-4.13.-3.12.-6.13.-1.15.42.v.m": -1 } },
  // Limit
  { $limit: 100 },
  // Project selected fields
  {
    $project: {
      patient_id: "$ehr_id",
      composition_id: "$_id",
      systolic: "$n.11.-4.13.-3.12.-6.13.-1.15.42.v.m",
      diastolic: "$n.11.-5.13.-3.12.-6.13.-1.15.42.v.m"
    }
  }
]
```

## Debug Tools

### View AST

```bash
curl -X POST "http://localhost:8000/api/domains/openehr/query/aql/parse" \
  -H "Content-Type: text/plain" \
  -d "SELECT c/uid/value FROM EHR e CONTAINS COMPOSITION c"
```

### View Pipeline

```bash
curl -X POST "http://localhost:8000/api/domains/openehr/query/aql/mql" \
  -H "Content-Type: text/plain" \
  -d "SELECT c/uid/value FROM EHR e CONTAINS COMPOSITION c"
```

### Check Strategy

```bash
curl "http://localhost:8000/api/domains/openehr/query/strategy/info?ehr_id=patient-001"
```

## Related

- [Reversed Paths](/docs/concepts/reversed-paths) - Path encoding
- [Dual Collection](/docs/concepts/dual-collection) - Query routing
- [Query Translation](/docs/strategies/openehr-rps-dual/query-translation) - Strategy details

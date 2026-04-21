---
sidebar_position: 2
---

# Data Model

RPS Dual materializes two related MongoDB document shapes from one canonical openEHR composition:

- a primary composition document in `compositions_rps`
- an optional slim search document in `compositions_search`

The exact field names are configurable in the strategy, but the examples below use the current defaults from `defaults.json`.

## Primary Store (`compositions_rps`)

Representative document:

```json
{
  "_id": "composition-uuid::kehrnel::1",
  "ehr_id": "patient-001",
  "comp_id": "composition-uuid::kehrnel::1",
  "v": "1",
  "time_c": "2025-01-15T10:30:00Z",
  "tid": "PO_Obstetric_process_v0.8_FORMULARIS",
  "cn": [
    {
      "p": "1",
      "data": {
        "T": "C",
        "ani": 1,
        "uid": { "T": "OVI", "v": "composition-uuid::kehrnel::1" },
        "ad": {
          "ai": 1,
          "rv": "1.0.4",
          "ti": { "v": "PO_Obstetric_process_v0.8_FORMULARIS" }
        }
      }
    },
    {
      "p": "-4.8.-3.-2.-1.2.1",
      "kp": ["i"],
      "pi": [1, 0, -1, 0, -1, 0, -1],
      "data": {
        "T": "U",
        "ani": -4,
        "n": { "T": "dt", "v": "Data d'inici" },
        "v": { "T": "ddt", "v": "2025-01-15T10:30:00Z" }
      }
    }
  ]
}
```

## Search Store (`compositions_search`)

The search sidecar is only written when:

- `collections.search.enabled=true`, and
- the active analytics or mapping configuration selects fields for that template

Representative document:

```json
{
  "_id": "composition-uuid::kehrnel::1",
  "ehr_id": "patient-001",
  "comp_id": "composition-uuid::kehrnel::1",
  "sort_time": "2025-01-15T10:30:00Z",
  "tid": "PO_Obstetric_process_v0.8_FORMULARIS",
  "sn": [
    {
      "p": "-4.8.-3.-2.-1.2.1",
      "data": {
        "ani": -4,
        "v": { "v": "2025-01-15T10:30:00Z" }
      }
    }
  ]
}
```

## Root Fields

Identifier field storage is governed by `ids.ehr_id` and
`ids.composition_id`. The examples below use string values for readability,
but the physical stored type may also be `ObjectId` or UUID-backed binary,
depending on the active strategy config.

| Field | Store(s) | Type | Description |
|-------|----------|------|-------------|
| `_id` | both | string | Version UID (`uuid::system::version`) |
| `ehr_id` | both | string | EHR identifier |
| `comp_id` | both | string | Composition identifier stored explicitly for filters and joins |
| `v` | `compositions_rps` | string | Version number extracted from the version UID |
| `time_c` | `compositions_rps` | date | Commit time for the persisted version |
| `tid` | both | string | Template identifier by default |
| `cn` | `compositions_rps` | array | Full node array used for patient-scoped matching and reconstruction |
| `sort_time` | `compositions_search` | date | Search-side sort helper field |
| `sn` | `compositions_search` | array | Search node array for Atlas Search |

## Node Arrays

### Composition Nodes (`cn`)

Each `cn` entry stores:

- `p`: reversed and encoded path
- `data`: the local clinical subtree for that node
- optional positional helpers such as `kp` and `pi`

The path and `data.ani` values can be code-encoded, while the template id stays a string by default.

### Search Nodes (`sn`)

The `sn` array keeps only the fields required by the active analytics/search mappings:

```json
{
  "sn": [
    {
      "p": "<encoded_path>",
      "data": { "...": "mapped search payload" }
    }
  ]
}
```

## Path Encoding

`p` stores the reversed path. Depending on strategy config, path segments may be compacted with:

| Component | Current default |
|-----------|-----------------|
| archetype IDs | sequential integer code |
| at-codes | negative integers |
| RM attribute keys in search docs | shortcut encoding when `apply_shortcuts=true` |

## Value Encoding

Node payloads keep openEHR value structure, but shortcut keys can be applied in search projections. For example:

- `data.ad.ti.v` for `archetype_details.template_id.value`
- `data.v.v` for simple DV values
- `data.v.df.cs` for coded text `defining_code.code_string`

## Code Dictionaries

### `_codes`

Maintains mappings between human-readable archetype and at-code identifiers and the compact codes used during ingestion and query compilation.

### `_shortcuts`

Maintains the RM key shortcut map used in search-side projections when `transform.apply_shortcuts=true`.

## Indexes

### `compositions_rps`

```javascript
db.compositions_rps.createIndex({ "ehr_id": 1, "v": 1 })
db.compositions_rps.createIndex({ "ehr_id": 1, "tid": 1, "time_c": 1, "comp_id": 1 })
db.compositions_rps.createIndex({ "ehr_id": 1, "cn.p": 1, "time_c": 1 })
```

### `compositions_search`

```javascript
db.compositions_search.createIndex({ "ehr_id": 1, "sort_time": 1 })
```

Atlas Search definitions should be generated from the active mappings instead of hand-maintained static JSON:

```bash
kehrnel strategy build-search-index \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual \
  --out .kehrnel/search-index.json
```

## Related

- [Reversed Paths](/docs/concepts/reversed-paths) - Path encoding explained
- [Flattening](/docs/concepts/flattening) - Transformation process
- [Query Translation](/docs/strategies/openehr/rps-dual/query-translation) - How queries use this model

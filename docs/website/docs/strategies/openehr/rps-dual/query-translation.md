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

Last audited: 2026-07-10, against the RPS Dual compiler, the unit test suite,
and the sibling `ehrbase-mongodb` parity workspace.

\{kehrnel\} has two AQL compatibility tracks:

- `parity` is the default public behavior. It intentionally follows upstream
  EHRbase behavior, including rejecting features that EHRbase rejects.
- `extended` is opt-in with `X-AQL-Feature-Mode: extended`. It exposes
  Mongo-native capabilities that are useful in Kehrnel but are not part of the
  upstream-compatible default.

The `ehrbase-mongodb` Java facade does not implement AQL semantics itself. Its
standard endpoint, `POST /rest/openehr/v1/query/aql`, forwards to Kehrnel's
runtime contract and passes the feature mode through. Its parity repository uses
upstream EHRbase as the black-box reference target.

### Compatibility Matrix

Status legend:

- <span className="badge badge--success">Supported</span> implemented and validated for the stated scope
- <span className="badge badge--warning">Partially supported</span> implemented for useful shapes, with documented scope limits
- <span className="badge badge--danger">Not supported</span> not available as a public AQL feature today

| AQL capability | Kehrnel status | EHRbase-MongoDB / upstream comparison | Notes |
| --- | --- | --- | --- |
| Public AQL execution | <span className="badge badge--success">Supported</span> | EHRbase-MongoDB forwards to Kehrnel | Direct Kehrnel endpoints, the internal runtime contract, and the Java facade all route to the same Kehrnel AQL engine. |
| `SELECT` path projections | <span className="badge badge--success">Supported</span> | Parity-covered for laboratory cases | Supports EHR paths, composition metadata, version metadata, and archetype paths through encoded node lookup. Current immunization projection parity cases in the sibling workspace remain a visible growth area. |
| `AS` aliases | <span className="badge badge--success">Supported</span> | Supported | Output columns use explicit aliases. Kehrnel can generate fallback names, but public examples should prefer `AS`. |
| `SELECT DISTINCT` | <span className="badge badge--success">Supported</span> | Parity-covered | Implemented after projection with `$group` and `$replaceRoot`. |
| `COUNT(...)` | <span className="badge badge--success">Supported</span> | Parity-covered | Supports row-count aggregates such as `COUNT(*)`, `COUNT(e)`, `COUNT(c)`, and version-alias count forms. Count-plus-row projections are supported by grouping on the selected non-aggregate columns. |
| `MIN(...)`, `MAX(...)`, `SUM(...)`, `AVG(...)` | <span className="badge badge--success">Supported</span> | Parity-covered for current laboratory aggregate cases | Supported for aggregate result sets over full AQL path arguments. Multiple value aggregates are supported when they resolve to the same source path. |
| Mixed aggregate + non-aggregate projections | <span className="badge badge--success">Supported</span> | EHRbase-MongoDB records upstream behavior as partial | Supports grouped `COUNT(*)`, `COUNT(e)`, `COUNT(c)`, and `COUNT(v)` projections on both match and search pipelines. The selected non-aggregate columns define the grouping key. Mixed path-based aggregates such as `MIN(path) + c/name/value` remain the next expansion. |
| `FROM EHR e CONTAINS COMPOSITION c` | <span className="badge badge--success">Supported</span> | Supported | Standard root shape for direct Kehrnel and facade AQL. |
| `CONTAINS VERSION v CONTAINS COMPOSITION c` | <span className="badge badge--success">Supported</span> | Used by Kehrnel beyond the main sibling parity lane | Supports `v/commit_audit/time_committed/value` filtering, projection, and ordering. |
| Linear nested `CONTAINS` chains | <span className="badge badge--success">Supported</span> | Upstream supports this class | Optimized structural matching is strongest for archetype-predicate chains such as `COMPOSITION -> SECTION -> EVALUATION -> CLUSTER`. |
| Compound sibling `CONTAINS` aliases | <span className="badge badge--success">Supported</span> | Kehrnel extension area | Supports parsed sibling aliases such as `EVALUATION ar AND EVALUATION ar2`, alias-aware projection, and correlated row filtering. Keep predicates archetype-based for best results. |
| Archetype predicates in `CONTAINS` | <span className="badge badge--success">Supported</span> | Supported | Shortened-path execution resolves archetype IDs through the codes dictionary. |
| `CONTAINS` name predicates | <span className="badge badge--warning">Partially supported</span> | Not parity-covered | The parser accepts forms such as `[openEHR-... and name/value='...']`; current structural filtering is based on the archetype predicate. |
| Basic `NOT CONTAINS` | <span className="badge badge--success">Supported</span> | Upstream/default parity rejects it | Kehrnel supports one negated linear archetype-predicate edge in `extended` mode over shortened-path nodes. Compound or concatenated `NOT CONTAINS` chains remain a future expansion. |
| Patient-scoped queries | <span className="badge badge--success">Supported</span> | Parity-covered | An explicit EHR id uses the composition collection and `$match`. |
| Cohort / cross-patient queries | <span className="badge badge--success">Supported</span> | Parity-covered for laboratory cases | Uses the `compositions_search` sidecar and Atlas Search when needed. Match-friendly cohort shapes can use the base composition collection. |
| Search table / sidecar execution | <span className="badge badge--success">Supported</span> | EHRbase-MongoDB depends on the same Kehrnel sidecar | This is the core cohort-query path. It requires the RPS Dual search collection and Atlas Search index for the fully indexed execution path. |
| Query parameters (`$name`) | <span className="badge badge--success">Supported</span> | Parity-covered | Missing parameters fail before execution. |
| Literal comparisons (`=`, `!=`, `>`, `<`, `>=`, `<=`) | <span className="badge badge--success">Supported</span> | Parity-covered | Handles EHR-level, composition metadata, version commit-time, and node-value predicates. |
| Path-to-path comparisons | <span className="badge badge--success">Supported</span> | Kehrnel extension; not upstream-compatible default behavior | Public APIs require `X-AQL-Feature-Mode: extended`. The search pipeline reduces candidates with Atlas Search and applies exact correlated comparison afterward. |
| Adverse-reaction sibling comparison query | <span className="badge badge--success">Supported</span> | Kehrnel extension; upstream EHRbase rejects this `WHERE` shape | Validated for `SELECT DISTINCT`, `EVALUATION ar AND EVALUATION ar2`, and `ar/.../code_string != ar2/.../code_string`; compiles to `$search`, exact `$expr` match, projection, distinct grouping, and limit. |
| `AND` / `OR` | <span className="badge badge--success">Supported</span> | Parity-covered | Includes mixed EHR-level and composition-level `OR` branches. |
| `LIKE` | <span className="badge badge--success">Supported</span> | Parity-covered | Uses AQL wildcards: `*` for zero-or-more characters and `?` for one character. |
| `MATCHES {...}` | <span className="badge badge--success">Supported</span> | Parity-covered | Current scope is literal value-set matching, not terminology-service expansion. |
| `EXISTS` / `NOT EXISTS` | <span className="badge badge--success">Supported</span> | Upstream/default parity rejects it | Available in `extended` mode for match and search execution. |
| `ORDER BY` full AQL paths | <span className="badge badge--success">Supported</span> | Parity-covered | Supports multiple order expressions and `ASC`/`DESC`. Projection aliases are intentionally rejected to match upstream behavior. |
| `LIMIT` / `OFFSET` | <span className="badge badge--success">Supported</span> | Parity-covered | Runtime pagination may cap very large limits to the configured page size. Offset-only runtime requests require a fetch/limit. Deprecated `TOP` is intentionally rejected; use `LIMIT`. |
| `LET` expressions | <span className="badge badge--warning">Partially supported</span> | Not parity-covered | Internal transformer scaffolding exists, but this is not yet a recommended public AQL feature. |
| Deterministic string functions | <span className="badge badge--success">Supported</span> | Upstream support is implementation-dependent | Supports `LENGTH(path_or_literal)`, `SUBSTRING(path_or_literal, position, length)`, and `CONCAT(...)` in projections on match and search pipelines. `SUBSTRING` uses openEHR's 1-based position wording and compiles to MongoDB's zero-based `$substrCP`. |
| Date/time and terminology functions | <span className="badge badge--danger">Not supported</span> | Upstream support is implementation-dependent | Examples not currently targeted: `NOW`, `CURRENT_DATE_TIME`, and terminology expansion helpers. |

### Validated Kehrnel Extension

Kehrnel now supports the adverse-reaction query shape where two sibling
`EVALUATION` aliases are compared by path in the `WHERE` clause:

```aql
SELECT DISTINCT
  c/uid/value AS composition_uid,
  e/ehr_id/value AS ehr_id,
  ar/data[at0001]/items[at0002]/value/defining_code/code_string AS substance_code
FROM EHR e
CONTAINS VERSION v
CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.encounter.v1]
CONTAINS (
  EVALUATION ar[openEHR-EHR-EVALUATION.adverse_reaction_risk.v2]
  AND EVALUATION ar2[openEHR-EHR-EVALUATION.adverse_reaction_risk.v2]
)
WHERE
  c/archetype_details/template_id/value = 'air_adverse_reaction_record_v1'
  AND ar/data[at0001]/items[at0002]/value/defining_code/code_string
      != ar2/data[at0001]/items[at0002]/value/defining_code/code_string
```

This is an opt-in Kehrnel extension for public APIs because upstream EHRbase
rejects this class of path-to-path `WHERE` condition.

### Current Parity Snapshot

The sibling `ehrbase-mongodb` parity suite currently demonstrates passing AQL
parity for:

- patient-scoped and cross-patient laboratory queries
- cohort queries using the search sidecar
- query parameters
- `DISTINCT`
- `COUNT`, `MIN`, `MAX`, `SUM`, and `AVG` aggregate-only result sets
- `LIKE` and `MATCHES`
- mixed EHR-level/composition-level `OR`
- negative compatibility cases for `TOP` and `ORDER BY` alias

The Kehrnel unit suite now additionally covers:

- grouped `COUNT` result sets with selected non-aggregate columns
- deterministic string projection functions: `LENGTH`, `SUBSTRING`, and `CONCAT`

Growth areas visible in that parity workspace:

- close the remaining immunization cross-patient projection gaps
- broaden mixed aggregate result sets from grouped row counts to path-based value aggregates
- expand `NOT CONTAINS` from one linear negated edge to compound/chained shapes
- add date/time and terminology-aware built-in functions once their runtime semantics are explicit
- keep Kehrnel extensions (`EXISTS`, `NOT EXISTS`, `NOT CONTAINS`, and
  path-to-path comparisons) explicitly opt-in while expanding their test cases

### Recommended Support Roadmap

1. **Promote extension coverage into the product parity suite.** Add the
   adverse-reaction sibling-comparison query as an `extended` case in
   `ehrbase-mongodb`, with a default-mode rejection assertion and an
   extended-mode success assertion.
2. **Broaden `NOT CONTAINS`.** Move from one linear negated edge to multiple
   negated edges, then to compound sibling negation shapes.
3. **Finish path-based mixed aggregate projections.** Grouped row counts are
   supported now; the next step is `MIN(path)`, `MAX(path)`, `SUM(path)`, and
   `AVG(path)` combined with selected grouping columns.
4. **Add date/time and terminology function lanes.** Deterministic string
   helpers are supported now; move to current-time and terminology-aware
   functions once the expansion source and reproducibility semantics are
   explicit.
5. **Generate more compound-containment cases.** Use the same seed packs to
   generate sibling-alias, repeated-archetype, and path-to-path comparisons so
   regressions are caught before customers find them.

## Debug Endpoints

### View Generated Pipeline

```bash
RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

curl -X POST "${RUNTIME_URL}/api/domains/openehr/query/aql/mql" \
  -H "Content-Type: text/plain" \
  -d "SELECT c/uid/value FROM EHR e CONTAINS COMPOSITION c"
```

Response includes:
- Original AQL
- Parsed AST
- Generated MQL pipeline

### View Strategy Decision

```bash
RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

curl "${RUNTIME_URL}/api/domains/openehr/query/strategy/info?ehr_id=patient-001"
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
- [Data Model](/docs/strategies/openehr/rps-dual/data-model) - Storage structure
- [Query API](/docs/api/overview) - API reference (see ReDoc)

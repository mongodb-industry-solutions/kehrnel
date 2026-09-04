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

Last audited: **2026-09-01**, against the RPS Dual and RPS Dual IBM compilers,
the focused unit/contract suite, the sibling `ehrbase-mongodb` parity
workspace, and live execution checks of both the patient-scoped `$match` route
and the cross-patient `$search` route.

\{kehrnel\} has two AQL compatibility tracks:

- `parity` is the default public behavior. It intentionally follows upstream
  EHRbase behavior, including rejecting features that EHRbase rejects.
- `extended` is opt-in with `X-AQL-Feature-Mode: extended`. It exposes
  Mongo-native capabilities that are useful in Kehrnel but are not part of the
  upstream-compatible default.

The public domain AQL endpoints enforce that feature-mode boundary. Direct
strategy-runtime callers currently do not enforce it consistently; an
integration must not assume that successful strategy compilation proves public
parity-mode acceptance.

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
| `SELECT DISTINCT` | <span className="badge badge--warning">Partially supported</span> | Parity-covered for selective shapes | Implemented after projection with `$group` and `$replaceRoot`. Constrained queries execute successfully. An unconstrained cross-patient IBM query currently fails because the match-friendly route requires `$match` before projection. |
| `COUNT(...)` | <span className="badge badge--success">Supported</span> | Parity-covered | Supports row-count aggregates such as `COUNT(*)`, `COUNT(e)`, `COUNT(c)`, and version-alias count forms. Count-plus-row projections are supported by grouping on the selected non-aggregate columns. A template-constrained clinical cohort `COUNT(c)` has also been validated on the `$search` route. |
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
| Cohort / cross-patient queries | <span className="badge badge--warning">Partially supported</span> | Parity-covered for laboratory cases | Uses the `compositions_search` sidecar and Atlas Search when needed. Template-constrained numeric filters, result projection, `DISTINCT`, and cohort row counts are live-validated `$search` shapes. Match-friendly metadata and structural shapes can use the base collection. Results depend on sidecar coverage, path correlation, and compatible Atlas mappings. |
| Search table / sidecar execution | <span className="badge badge--warning">Partially supported</span> | EHRbase-MongoDB depends on the same Kehrnel sidecar | Requires the search collection, a ready Atlas Search index, mappings compatible with emitted operators, and complete sidecar materialization. Deployments must monitor sidecar coverage by template rather than assuming that index readiness proves completeness. |
| Deep-path predicate correlation on `$search` | <span className="badge badge--warning">Partially supported</span> | Not fully parity-covered | Projection resolves encoded `sn.p` paths after lookup, but the current Atlas candidate predicate can target a value field without also constraining the encoded node path. Template-constrained shapes can be validated against known data; broader path-precise cohort predicates remain a gap. |
| Query parameters (`$name`) | <span className="badge badge--success">Supported</span> | Parity-covered | Missing parameters fail before execution. HealthcareDataLab currently accepts parameters during compile but does not preserve them in the Query Lab to Sandbox execution hand-off, so use literals for that UI flow. |
| Literal comparisons (`=`, `!=`, `>`, `<`, `>=`, `<=`) | <span className="badge badge--success">Supported</span> | Parity-covered | Handles EHR-level, composition metadata, version commit-time, and node-value predicates. |
| Path-to-path comparisons | <span className="badge badge--success">Supported</span> | Kehrnel extension; not upstream-compatible default behavior | Public APIs require `X-AQL-Feature-Mode: extended`. The search pipeline reduces candidates with Atlas Search and applies exact correlated comparison afterward. |
| Adverse-reaction sibling comparison query | <span className="badge badge--success">Supported</span> | Kehrnel extension; upstream EHRbase rejects this `WHERE` shape | Validated for `SELECT DISTINCT`, `EVALUATION ar AND EVALUATION ar2`, and `ar/.../code_string != ar2/.../code_string`; compiles to `$search`, exact `$expr` match, projection, distinct grouping, and limit. |
| `AND` / `OR` | <span className="badge badge--warning">Partially supported</span> | Parity-covered for selected shapes | `AND` and patient-scoped mixed-path `OR` execute. Repeated equality branches on the same path are rejected, and a cross-patient template `OR` returned zero rows incorrectly in the audited IBM environment. |
| `LIKE` | <span className="badge badge--warning">Partially supported</span> | Parity-covered for laboratory cases | Uses AQL wildcards: `*` and `?`. Patient-scoped execution works. Cross-patient deep-path execution fails when the compiler emits Atlas regex against fields mapped as analyzed tokens. |
| `MATCHES {...}` | <span className="badge badge--warning">Partially supported</span> | Parity-covered for laboratory cases | Literal value-set matching, not terminology-service expansion. Patient-scoped execution works. Cross-patient execution has the same Atlas regex/analyzer compatibility requirement as `LIKE`. |
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

### Current IBM-Profile Execution Findings

The 2026-08-30 IBM-profile check positively executed projections, aliases,
patient parameters, `VERSION` commit-time ranges, deep clinical projections,
nested `CONTAINS`, constrained `DISTINCT`, `COUNT`, all four numeric value
aggregates, deterministic string functions, comparisons, patient-scoped mixed
`OR`, patient-scoped `LIKE` and `MATCHES`, multi-column `ORDER BY`, `LIMIT`,
`OFFSET`, `EXISTS`, and one-edge `NOT CONTAINS`.

The combined IBM-profile and 2026-09-01 live RPS Dual checks found these
defects or operational gaps:

- cross-patient regex-backed `LIKE` and `MATCHES` conflict with the active
  Atlas token mappings for `template` and coded-value fields
- Atlas candidate generation for a deep value predicate does not always carry
  the encoded `sn.p` constraint into the same embedded-document clause
- unconstrained cross-patient `DISTINCT` fails the match-friendly stage-order
  invariant
- repeated same-path equality `OR` is not supported, and cross-patient template
  `OR` produced an incorrect empty result
- the search sidecar was incomplete relative to the base composition collection
- extension-mode enforcement differs between public AQL and direct strategy
  runtime paths
- HealthcareDataLab does not carry compile-time parameter values into Sandbox
  execution

These are product findings, not limitations of the AQL specification.

### Engineering Test Status At Audit Time

The 2026-09-01 RPS Dual/AQL suite completed with **98 passed, 2 failed,
7 expected failures, and 1 unexpected pass**. The two failures are contract drift in
`test_compiler_engine.py`: the tests expect builder class labels
(`pipeline_builder` and `search_pipeline_builder`) while the current plan
contract reports execution engines (`mongo_pipeline` and `text_search_dual`).
They do not indicate a failed query pipeline, but the suite is not fully green
until the expected contract is made consistent.

The expected failures continue to track Atlas embedded-document/lookup parity,
patient regex grouping, and patient/cross-patient vaccination projection
compatibility. Treat those as known parity gaps even when adjacent compiler
tests pass.

### Recommended Support Roadmap

1. **Correlate Atlas node path and value predicates.** Emit the encoded `sn.p`
   constraint in the same embedded-document clause as each deep value predicate,
   then add live regressions covering two nodes with the same value.
2. **Align Atlas mappings and emitted operators.** Generate keyword-compatible
   mappings or use Atlas operators compatible with analyzed token fields for
   cross-patient `LIKE` and `MATCHES`.
3. **Correct `OR` and unconstrained `DISTINCT` lowering.** Add regression cases
   for repeated same-path predicates, cross-patient template alternatives, and
   projection-first cohort plans.
4. **Make sidecar completeness observable.** Report coverage by template and
   prevent incomplete cohort indexes from looking authoritative.
5. **Unify feature-mode enforcement and parameter hand-off.** Apply the same
   parity/extended gate to every runtime path and carry Query Lab parameters
   into Sandbox execution.
6. **Promote extension coverage into the product parity suite.** Add the
   adverse-reaction sibling-comparison query as an `extended` case in
   `ehrbase-mongodb`, with a default-mode rejection assertion and an
   extended-mode success assertion.
7. **Broaden `NOT CONTAINS`.** Move from one linear negated edge to multiple
   negated edges, then to compound sibling negation shapes.
8. **Finish path-based mixed aggregate projections.** Grouped row counts are
   supported now; the next step is `MIN(path)`, `MAX(path)`, `SUM(path)`, and
   `AVG(path)` combined with selected grouping columns.
9. **Add date/time and terminology function lanes.** Deterministic string
   helpers are supported now; move to current-time and terminology-aware
   functions once the expansion source and reproducibility semantics are
   explicit.
10. **Generate more compound-containment cases.** Use the same seed packs to
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
- Measure latency in the target environment; document count, projections, and
  network placement materially affect it.

### Cross-Patient

- Index: Atlas Search
- Complexity: O(log n) for search
- Measure latency and verify sidecar completeness in the target environment.
  A ready index does not by itself prove that every base composition was
  materialized into the sidecar.

## Related

- [AQL to MQL Concepts](/docs/concepts/aql-to-mql) - Detailed translation rules
- [Data Model](/docs/strategies/openehr/rps-dual/data-model) - Storage structure
- [Query API](/docs/api/overview) - API reference (see ReDoc)

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

## Governed OPT-Semantic Execution

Raw AQL remains the general query surface. For clinical questions that begin
with terminology and OPT meaning, Kehrnel also provides a narrower, fail-closed
path:

```text
typed semantic intent
  -> bounded terminology expansion
  -> exact OPT/catalog binding
  -> deterministic AQL
  -> projection-readiness preflight
  -> exact-correlation AQL-to-MQL compilation
  -> execution and evidence receipt
```

The relevant endpoints are:

- `GET /api/domains/openehr/semantics/templates/{templateId}/catalog`
- `POST /api/domains/openehr/semantics/templates/{templateId}/query-plans`
- `POST /api/domains/openehr/semantics/templates/{templateId}/terminology-query-plans`
- `POST /api/domains/openehr/semantics/templates/{templateId}/terminology-query-executions`
- the same paths below `/v1`

The execution endpoint accepts a typed semantic intent, not natural language,
raw AQL, or raw MQL. It resolves terminology through the authenticated
environment, pins the selected OPT/catalog and active strategy activation,
pins the credential-free physical data target, requires chain-valued
path-instance correlation, and verifies the exact AQL, parameters, output shape,
and compiled pipeline before returning rows. Results use deterministic
composition-UID ordering, bounded offset windows, and a one-row look-ahead that
reports `hasMore` and `nextOffset`. It currently supports `openehr.rps_dual`
only.

Before query dispatch, the endpoint requires a projection-readiness receipt. The
receipt binds required AQL paths to the active mappings, projection algorithm,
projection revision, bidirectional source/sidecar reconciliation, completed
rebuild checkpoint, and Atlas Search definition. The live index must report the
exact definition as READY and queryable. Kehrnel checks the mutation sequence
again after query execution and discards rows if it changed. A missing path,
stale or incomplete rebuild, active writer, unverified index, activation or data
target change, input-evidence mismatch, missing exact-correlation attestation,
or missing compiled pipeline fails the request instead of returning an
authoritative empty result.

The active mapping must also carry per-template `templateSourceDigest` and
`catalogDigest` provenance matching the selected OPT catalog. The packaged
laboratory mapping now carries those exact values, and its generator refreshes
them from the OPT-derived catalog. A stale or missing value fails preflight.

RPS Dual's rebuild operation is bounded, resumable by opaque cursor, and
idempotent by source identity. It detects orphan and unmapped sidecars, verifies
mapped source-to-sidecar coverage, and rejects mapped records that produce no
searchable nodes. The operation also accepts `template_ids`, allowing the
Analytics fields selected for one template to be materialized and certified in
the same shared search collection without unrelated templates blocking its
readiness. Semantic preflight prefers that exact template-scoped receipt and
falls back to a complete global receipt for compatibility. Governed writes use
a mutation fence, invalidate scoped receipts, and rebuild publication uses
compare-and-set. It does not yet prune stale records or atomically swap a
complete generation; deployments need an explicit, auditable reconciliation
step before production qualification, and external writers must participate in
the invalidation protocol.

### Evidence boundary

Unit and contract tests currently prove catalog binding, terminology receipt
validation, generated AQL, compiled-pipeline structure, strict correlation,
activation drift rejection, and projection-preflight failure modes. The packaged
heart-failure cohort provides deterministic OPT-valid cases and an excluded
invalid repeated-instance fixture.

Those tests alone do **not** prove a deployed Atlas environment. A reference
heart-failure probe has also completed against a live reference deployment: a
template-scoped rebuild materialized the sample laboratory compositions, the
live Atlas index was verified, and SNOMED CT expansion plus governed AQL
returned the expected patients. That is a deployment-scoped functional
qualification, not a blanket production or performance claim.

## Supported AQL Features

General AQL matrix last audited: **2026-09-01**, against the RPS Dual compiler,
the focused unit/contract suite, the `ehrbase-mongodb` parity workspace, and
live execution checks of both the patient-scoped `$match` route and the
cross-patient `$search` route. The governed semantic lane above was reviewed on
**2026-10-01** from its unit, contract and live Atlas evidence.

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
| `SELECT DISTINCT` | <span className="badge badge--warning">Partially supported</span> | Parity-covered for selective shapes | Implemented after projection with `$group` and `$replaceRoot`. Constrained queries execute successfully. |
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
| Cohort / cross-patient queries | <span className="badge badge--warning">Partially supported</span> | Parity-covered for laboratory cases | Uses the `compositions_search` sidecar and Atlas Search when needed. Template-constrained numeric filters, result projection, `DISTINCT`, and cohort row counts are live-validated `$search` shapes. Match-friendly metadata and structural shapes can use the base collection. Governed semantic execution adds strict correlation and readiness checks; general AQL retains its broader historical behavior. |
| Search table / sidecar execution | <span className="badge badge--warning">Partially supported</span> | EHRbase-MongoDB depends on the same Kehrnel sidecar | Requires the search collection, mappings compatible with emitted operators, complete materialization, and a ready Atlas Search index. The governed lane checks a versioned rebuild receipt plus the live index definition/state. The rebuild is resumable and detects unreconciled sidecars, but does not yet prune or atomically swap them. |
| Deep-path predicate correlation on `$search` | <span className="badge badge--warning">Partially supported</span> | Not fully parity-covered | Ordinary AQL keeps the historical permissive behavior. Governed semantic plans set `require_exact_path_correlation`: each Atlas value predicate is then paired with its encoded `sn.p` path inside one embedded search node, and same-archetype conjunctions are rechecked against one stored path-instance chain. Missing dictionaries, instance metadata, or unsupported Boolean/correlation shapes fail closed. Live coverage still depends on a sidecar and Atlas mapping generated from the same pinned OPT catalog. |
| Query parameters (`$name`) | <span className="badge badge--success">Supported</span> | Parity-covered | Missing parameters fail before execution. HealthcareDataLab currently accepts parameters during compile but does not preserve them in the Query Lab to Sandbox execution hand-off, so use literals for that UI flow. |
| Literal comparisons (`=`, `!=`, `>`, `<`, `>=`, `<=`) | <span className="badge badge--success">Supported</span> | Parity-covered | Handles EHR-level, composition metadata, version commit-time, and node-value predicates. |
| Path-to-path comparisons | <span className="badge badge--success">Supported</span> | Kehrnel extension; not upstream-compatible default behavior | Public APIs require `X-AQL-Feature-Mode: extended`. The search pipeline reduces candidates with Atlas Search and applies exact correlated comparison afterward. |
| Adverse-reaction sibling comparison query | <span className="badge badge--success">Supported</span> | Kehrnel extension; upstream EHRbase rejects this `WHERE` shape | Validated for `SELECT DISTINCT`, `EVALUATION ar AND EVALUATION ar2`, and `ar/.../code_string != ar2/.../code_string`; compiles to `$search`, exact `$expr` match, projection, distinct grouping, and limit. |
| `AND` / `OR` | <span className="badge badge--warning">Partially supported</span> | Parity-covered for selected shapes | `AND` and patient-scoped mixed-path `OR` execute. Repeated equality branches on the same path are rejected. |
| `LIKE` | <span className="badge badge--warning">Partially supported</span> | Parity-covered for laboratory cases | Uses AQL wildcards: `*` and `?`. Patient-scoped execution works. Cross-patient deep-path execution fails when the compiler emits Atlas regex against fields mapped as analyzed tokens. |
| `MATCHES {...}` | <span className="badge badge--warning">Partially supported</span> | Parity-covered for laboratory cases | Raw AQL provides literal value-set matching, not terminology-service expansion. For fields declared as Atlas `token`, the current compiler emits exact `equals`/`in`; analyzed string fields retain regex semantics. The governed endpoint resolves terminology before rendering the explicit `MATCHES` set. The SNOMED heart-failure exact-token lane was live-qualified on the reference tenant on 2026-10-01. |
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

### Status

Current support levels and what is planned next are kept on the
[status and roadmap](../../status-and-roadmap) page.

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

- [AQL Query Engine](/docs/domains/openehr/aql-query-engine) - Compiler components and collection routing
- [Query Translation (concept)](/docs/domains/openehr/concepts/aql-to-mql)
- [Data Model](/docs/strategies/openehr/rps-dual/data-model) - Storage structure
- [Query API](/docs/api/overview) - API reference (see ReDoc)


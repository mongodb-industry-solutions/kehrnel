# Healthcare Data Lab and openEHR semantic-query assessment

Status reviewed: 2026-10-01

Repositories reviewed:

- `HealthcareDataLab-datalab-ui`
- `kehrnel`

## Executive verdict

The new Healthcare Data Lab is already a strong product narrative and a credible
interactive specification. Its public healthcare and pharmaceutical entrances,
outcome-first Builder, maturity labels, versioned evidence contracts, and
separation between public discovery and authenticated operation are the right
shape.

It is not yet a live end-to-end product. The public Ask journey is a deliberately
labelled deterministic simulation. Its FHIR/SNOMED path still replays a generated
fixture, and its openEHR option is still a hard-coded planned trace. Neither UI
path currently calls Kehrnel, MongoDB, MCP, an LLM, or MongoDB Agentic Engine.

Kehrnel itself has moved beyond that UI state. It now has two narrow, typed,
authenticated execution slices: SNOMED expansion into a bounded FHIR Condition
cohort, and terminology expansion plus OPT-bound AQL execution through
`openehr.rps_dual`. Their failure rules, activation pinning, compiler evidence,
and response contracts are covered by unit and contract tests. They have not yet
been proven as one deployed Data Lab-to-terminology-to-database run over a named
snapshot, including live Atlas Search evidence for openEHR. "Implemented in
Core" and "live in the product" must therefore remain separate maturity claims.

The important conclusion remains that this is not primarily an AQL-to-MQL
problem. Kehrnel already has a substantial AQL parser, compiler,
patient-versus-population route selection, and MongoDB execution path in
`openehr.rps_dual`. The deterministic Core seam now exists; the remaining product
seam is to connect it to governed question interpretation, deployment, and the
Data Lab evidence experience:

```text
question
  -> governed clinical intent
  -> terminology expansion
  -> OPT/archetype semantic binding
  -> deterministic AQL
  -> existing AQL-to-MQL compiler
  -> MongoDB execution and evidence receipt
```

OPT and archetype metadata must be the authority for *where and how* a clinical
meaning is represented. A terminology service determines *what a coded concept
means*. Neither can replace the other.

## Current product status

| Area | Current status | Assessment |
| --- | --- | --- |
| Public healthcare/pharma entrances | Working | Clear industry-specific language over a shared platform is the correct model. |
| Outcome and application discovery | Working from versioned fixtures; Core-backed portfolio when connected | Strong storytelling surface; most items remain demonstrators or planned concepts. |
| Architecture Builder | Working as a guided, canvas-first experience | A good interactive specification. It compiles recorded/fixture projects when Core is unavailable and calls the bounded Core proxy when configured. |
| Builder persistence | Server foundation exists; current client draft cannot be saved reliably | Immutable revisions, optimistic concurrency, workspace scope, and server-side canonicalization are promising. However, the client now writes `sources[].profileRef` while the server rejects that key, and several blueprint concepts are lost during draft conversion. |
| Strategy operation workspace | Substantial existing implementation | The private/dark workspace contains real openEHR, FHIR, CDISC, mapping, synthetic-data, terminology, and environment tooling, but it remains a heterogeneous enablement application rather than one coherent product journey. |
| Ask in clinical meaning | Working simulation | Excellent explanation and evidence UX; not a live data query. |
| FHIR + SNOMED execution | Governed Kehrnel slice implemented; public Ask remains simulated | The authenticated Core endpoint resolves one pinned, bounded SNOMED selector, verifies that all Condition resources use the current search projection, and executes exact `system|code` tokens through `fhir.resource_store`. It is unit-tested, not yet demonstrated by a live Data Lab-to-MongoDB run. |
| openEHR AQL execution | Real inside Kehrnel | Raw AQL compilation and execution exist and have meaningful regression coverage. |
| OPT-derived semantic query planning | First governed Kehrnel slice implemented | Native OPTs now compile into immutable archetype-scoped catalogs, and a typed intent can produce validated AQL. Natural-language interpretation and Data Lab integration are still absent. |
| Terminology-aware AQL execution | Governed Kehrnel slice implemented; deployment proof remains | The authenticated endpoint resolves bounded, version-pinned expansions, validates the pinned OPT/catalog and projection, compiles with exact path-instance correlation, executes through `openehr.rps_dual`, and returns evidence. It is not yet called by Ask or proven against a provisioned live Atlas snapshot. |
| Agentic orchestration | Foundation only | Modeler contracts and policy boundaries exist. The Ask demonstrator does not call MongoDB Agentic Engine or MCP. |
| Production evidence | Contract shape implemented; live evidence not yet captured | Kehrnel returns activation, plan, terminology, projection, and compiled-query evidence. The public journey still has no receipt from an actual end-to-end run. |

### Engineering health observed

- At the UI assessment point, the Data Lab suite reported 419 passed and 4
  skipped, and its production build succeeded.
- At that same point, production Playwright reported 76 passed and 3 skipped.
  The skipped cases were live Core portfolio hand-offs, so this verified the UI
  and fixture paths rather than a live Kehrnel journey.
- Data Lab lint succeeded with warnings. The warnings included missing React Hook
  dependencies, unoptimized images, two missing image `alt` attributes, and
  anonymous default exports. These are not release blockers for the semantic
  slice, but hook warnings should be removed before calling the whole UI
  production-ready.
- The final Core foundation gate reports 214 passed, and the full unit suite
  reports 501 passed. The final supported-strategy gate reports 548 passed and
  37 skipped across `openehr.rps_dual`, the IBM
  exact-model compatibility profile, `fhir.resource_store`, and `cdisc.sdr`.
- Focused unit and contract tests exercise both governed execution slices,
  including truncation, activation and physical-target drift, missing projection
  paths, concurrent writes and rebuilds, unverified Atlas readiness, exact path
  correlation, and compiled-query evidence. These are simulated-adapter tests;
  they are not evidence of a live Atlas deployment.
- The generated strategy, Blueprint, Reference Architecture, and outcome catalog
  digests have been refreshed and the catalog-integrity check is clean.
- The openEHR validator emits thousands of `Element` truthiness deprecation
  warnings. This is technical debt rather than a semantic-query blocker, but it
  should be fixed before a Python upgrade turns the behavior into a defect.
- Both repositories contain a large uncommitted body of work. A release boundary,
  changelog, and reproducible tagged baseline are required before external reuse.
- The Data Lab production dependency audit currently reports 33 affected package
  nodes (4 critical, 15 high, 13 moderate, and 1 low). The direct roots include
  Next.js, Auth.js/NextAuth, Stoplight Elements, PostCSS, React Router, and
  Swagger UI. These need deliberate upgrades and regression testing.

### Product maturity

| Dimension | Assessment |
| --- | ---: |
| Narrative and visual experience | 8/10 |
| Builder/canvas abstraction | 8/10 |
| Core contracts and backend design | 7/10 |
| Builder authoring capability | 4/10 |
| Live runtime integration | 3/10 |
| Production/release readiness | 2/10 |

The scores are intentionally asymmetric: this is already an excellent way to
explain the future product, but it is not yet safe to present the visual paths as
operational product paths.

### Immediate Data Lab blockers

1. Establish a committed, reproducible baseline. The reviewed branch contains a
   very large uncommitted/untracked implementation surface.
2. Remediate the production dependency vulnerabilities with targeted upgrades.
3. Fix the `profileRef` persistence-contract mismatch.
4. Make draft conversion lossless. It currently collapses multiple consumptions
   of the same kind, reduces context contracts to `kind`, and merges distinct
   source instances that share a source profile. That cannot represent several
   hospitals, sites, or tenants running the same system.
5. Complete the public-to-private handoff. Save currently tells the user the
   design is not carried into the workspace; share links serialize the whole
   draft into the URL fragment.
6. Add real connector/schema discovery, per-source tenant/site/region settings,
   editable topology, field mapping, and bulk operations. The existing Mapping
   Studio is inspect/simulate only.
7. Make execution labels provider-aware. FHIR Search is currently labelled
   “live” even when the default fixture provider is replaying a simulation.

## What the current Ask demo really does

The current question, “Which patients have any form of heart failure recorded
since January 2025?”, follows this displayed sequence:

1. Resolve a curated intent.
2. Apply a read-only, bounded policy.
3. Select an allowlisted tool.
4. Expand SNOMED CT `<< 84114007` to eight concepts.
5. Show an ECL-to-MQL terminology query and an index choice.
6. Express the request as FHIR Search.
7. Show FHIR-Search-to-MQL compilation and a MongoDB index choice.
8. Return a bounded pseudonymous answer with citations.

The interaction architecture is worth keeping: versioned packs, composition
locks, policy decisions, ordered events, plan/run separation, evidence badges,
failure scenarios, replay, and citations are all reusable for openEHR.

The current implementation classifies the three supported intents using regular
expressions and replays an in-memory fixture. The data set, expansion, timings,
index use, documents, and answer are predetermined. This is honestly labelled in
the UI and contracts.

The secondary openEHR plan shown by the current UI is not equivalent to either
new Kehrnel execution slice:

- it is not generated from an OPT;
- it hard-codes one problem/diagnosis archetype and two node paths;
- the displayed onset path uses `at0077`, whereas the bundled laboratory OPT
  identifies “Date/time clinically recognised” as `at0003`;
- its terminology URI is not resolved by Kehrnel;
- it applies `MATCHES` to the structured `defining_code` object instead of to
  `defining_code/code_string` with a correlated terminology-ID predicate;
- the displayed MongoDB plan uses an illustrative value path that is not the
  RPS Dual flattened field model and omits the onset-date predicate; and
- it has no execution or `explain` receipt.

These are limitations of the current Data Lab fixture, not of the new Kehrnel
contract. The correct integration is to replace that fixture with the typed
Core response rather than reproduce its handwritten AQL in the UI.

## Kehrnel implementation completed in this pass

The first governed backend slices now exist in Kehrnel; they were deliberately
implemented outside the Data Lab UI so there is one domain authority.

### Native OPT semantic catalog

`openehr-opt-semantic-catalog/v1` is compiled directly from ADL 1.4 OPT XML. It
contains immutable template/source digests, archetype scopes, revision-scoped
field IDs, migration-oriented semantic keys, queryable fields,
archetype-relative and composition-relative AQL paths, RM value variants,
translations, node term bindings, allowed-code/value-set constraints,
governed ordinal-to-code options, annotations, multiplicity, containment, and
correlation anchors. Revision IDs
include the exact OPT digest while semantic keys remain stable across
non-structural revisions; reused `at` codes cannot collide.

All eight OPT files currently bundled with Kehrnel compile to non-empty,
digest-verifiable catalogs. Invalid and oversized XML is rejected.

### Governed semantic intent and deterministic AQL

`openehr-semantic-query/v1` now accepts only semantic-unit IDs from the exact
pinned catalog. It validates field facets, RM variants, operators, scalar types,
bounded request/result cardinalities, expansion references, and repeated-node
correlation declarations. The first safe slice permits cross-patient execution
over a COMPOSITION and one unambiguous contained archetype. Patient-scoped,
multi-archetype, nested-repetition, negative, and unit-less quantity queries are
rejected until their exact semantics can be proved end to end.

A terminology expansion receipt records the provider, code-system URI, openEHR
terminology ID, release, expression, sorted bounded code set, truncation state,
and content digest. Truncated or modified receipts fail closed. The authenticated
terminology-planning endpoint now obtains the expansion through the active
tenant terminology gateway and mints that receipt server-side. The sibling
compile-only endpoint still accepts caller-supplied receipts and therefore must
not be treated as a trusted execution boundary by itself. A coded predicate
checks both `terminology_id/value` and `code_string`, preventing an identical code
string from another code system from matching silently.

The generated execution request turns on exact path correlation. RPS Dual now
binds each Atlas Search value predicate to its resolved semantic path inside the
same flattened search node and, for several predicates under one repeated
archetype alias, adds an exact post-search comparison over the stored
path-instance chain. If the active path dictionary or instance metadata cannot
prove that relationship, the strict semantic path fails instead of weakening
the query. This strict mode is deliberately narrower than the historical raw-AQL
surface.

The path dictionary and flattened instance metadata used by an active RPS Dual
environment must be compatible with the selected projection. The execution
receipt pins the plan, catalog, activation, configuration, bindings, compiled
pipeline, projection revision, mapping, and Atlas index definition. Unit tests
exercise that verification with adapters; only a live deployment can attest the
actual Atlas index and stored sidecar contents.

OPT compilation is bounded to 16 MiB, 250,000 XML elements, a depth of 256, and
50,000 emitted fields. Sibling disambiguation is linear in the number of sibling
constraints, and the HTTP endpoints run the synchronous XML/catalog work outside
the asynchronous request thread. These limits protect the service boundary; they
do not replace catalog caching and precompilation for production deployments.

### Authenticated execution and evidence

The openEHR execution route accepts typed semantic intent, never raw caller-
supplied AQL or MQL. On the server it:

1. snapshots and constrains the active `openehr.rps_dual` activation;
2. resolves the bounded terminology request through the same environment;
3. verifies the OPT, catalog, semantic-query, and plan digests;
4. proves that the active projection covers every required path, has reconciled
   source and sidecar records in both directions, has no active governed writer,
   and that the exact Atlas index definition is live and queryable;
5. pins the credential-free physical data-target identity used to load the OPT,
   preflight the projection, and execute the query;
6. compiles and executes with exact path-instance correlation, then verifies the
   exact AQL, parameters, output aliases, engine, collection, stage-zero Atlas
   Search contract, deterministic sort, look-ahead limit, offset, and complete
   compiled-pipeline evidence;
7. rechecks the projection mutation sequence after execution and discards rows
   if the projection changed during the query; and
8. returns explicit paging (`offset`, `hasMore`, `nextOffset`) plus the complete
   execution evidence.

Activation and resolved data-target preconditions are checked again inside
runtime dispatch, closing configuration-switch and resolver-redirection gaps.
Semantic pages use stable composition-UID ordering and a one-row look-ahead;
storage and index errors cannot be converted into a legitimate empty result.
The public semantic-execution limit is 99 rows so that the strategy's bounded
100-row query allowance always retains the look-ahead row.

The IBM compatibility strategy is intentionally excluded from this semantic
execution route. Its current scalar lineage field cannot prove the repeated
archetype-instance relationship required by the strict contract. Its existing
query surface remains available, but semantic execution requires a migration or
backfill to chain-valued path-instance metadata.

### Projection lifecycle and reference cohort

RPS Dual now derives the slim-search projection and Atlas Search definition from
the active mapping model. Its maintenance operation uses bounded, opaque-cursor
batches and idempotent upserts, records the projection revision and checkpoint,
and defers readiness until materialization and index verification are complete.
The generated index includes the composition identifier used for deterministic
semantic-result ordering.
The semantic preflight fails closed for missing paths, stale revisions,
incomplete rebuilds, or an Atlas index whose live definition and READY/queryable
state cannot be verified.

The packaged reference cohort contains a pinned laboratory OPT, five valid
heart-failure cases, projection/index assets, and a deterministic cross-patient
AQL example. Two records should match; the controls cover a different concept,
a different terminology, and a clinical-recognition date before 2025. A separate
invalid repeated-EVALUATION fixture tests split-instance correlation and is not
part of the ingestable data set. The two bundled heart-failure codes are an
illustrative fixture subset, not an authoritative SNOMED expansion.

The preflight also requires the active mapping to declare the exact template
source and semantic-catalog digests. The packaged sample projection mapping does
carry those exact digests for the laboratory template. Regeneration refreshes
the mapping provenance from the same OPT-derived catalog, so drift causes
preflight to fail rather than silently using a path-compatible older mapping.

The rebuild proves mapped source-to-sidecar and sidecar-to-source coverage,
rejects mapped records that produce empty projections, and detects orphan or
unmapped sidecars. It acquires one exclusive rebuild lease, publishes `building`
before sidecar writes, binds the projection algorithm version into the revision,
and uses a monotonic mutation fence plus compare-and-set publication. A resumed
cursor is rejected when a governed write or another rebuild changed its
checkpoint. The strategy ingest path, synthetic generation, and the legacy
openEHR HTTP writers all participate in this fence.

The implementation does not delete stale records or atomically swap a complete
generation. Production lifecycle work still needs an explicit, auditable prune
or generation-swap operation, and writers outside Kehrnel must participate in
the fence or be quiesced.

### FHIR comparison slice

Kehrnel also exposes a deliberately narrow FHIR resource-search equivalent for
`Condition`. It
resolves one pinned SNOMED selector through the tenant terminology gateway,
checks Resource Store statistics under the same activation precondition, passes
exact `system|code` tokens into `fhir.resource_store`, executes under that pinned
activation, verifies the complete compiled query and execution target, and
returns canonical resources plus a receipt. It does not claim that page-local
subject references are a distinct-patient cohort. The contract is bounded to
200 resources and 256 expanded codes.

The readiness check requires the physical collection to exist, the Condition
document count to equal the current-contract count, and zero mismatches between
canonical `code.coding` and `_search.code_systemCode`. A digested readiness
receipt binds those facts to the database and collection. The required generic
search and exact-code indexes must be present, but their full definitions are
not yet attested; this is correctness readiness, not performance qualification.

The terminology release is pinned for expansion, but current FHIR persistence
does not require `Coding.version` on every stored resource. The evidence contract
therefore explicitly reports `storedCodingVersionEnforced: false`; system-and-
code identity is proven, while per-resource release provenance remains future
work.

FHIR readiness now establishes a credential-free physical-target digest. The
query dispatch must resolve to that same cluster authority and database, and the
runtime evidence must return the same digest. Rotating credentials remains safe;
redirecting the authorized request to another database fails closed.

### Request and supported-profile isolation

The active legacy openEHR HTTP surface no longer copies an environment's
database, collections, mappings, Atlas index, or flattener into process-global
settings or application state. Composition, EHR, EHR status, directory,
contribution, ingest, synthetic, internal, and AQL routes carry one resolved
request context end to end. Missing request-scoped collection contracts fail
closed, and activation lookup is strict by domain. A concurrent two-tenant test
uses different databases, base/search/dictionary collections, and mappings and
proves that the resolved state is not crossed.

The compatibility ingest endpoint's optional `strategy_id` route now treats the
identifier as an assertion against that request's active openEHR strategy and
delegates the complete write to the strategy runtime. The runtime owns
transformation, persistence, projection fencing, and dictionary flush; the
legacy repository cannot perform a second write. Per-request configuration,
conflicting selectors, and non-openEHR protocols are rejected on this path, and
server routing metadata is excluded from the clinical composition.

The IBM exact-model compatibility profile is also a complete supported pack at
the runtime boundary: it declares MongoDB and Atlas adapters, requires a
strategy-owned database, and publishes the same inherited index, projection
preflight, and rebuild operations as RPS Dual. Its `_id`/ObjectId and scalar
`li` representation are preserved. It remains deliberately excluded from the
strict semantic-query endpoint until it can provide chain-valued instance
lineage.

### API boundary

Kehrnel now exposes the deterministic portion through:

- `GET /api/domains/openehr/semantics/templates/{templateId}/catalog`
- `POST /api/domains/openehr/semantics/templates/{templateId}/query-plans`
- `POST /api/domains/openehr/semantics/templates/{templateId}/terminology-query-plans`
- `POST /api/domains/openehr/semantics/templates/{templateId}/terminology-query-executions`
- `POST /api/domains/fhir/semantic/terminology-query-executions`
- equivalent `/v1/...` routes

The openEHR endpoints reuse stored ADL 1.4 templates, return camelCase contracts,
and detect template/catalog drift. The catalog and compile-only plan endpoints do
not perform terminology lookup. The terminology plan endpoint resolves exactly
matched expansion requests but does not execute. The terminology execution
endpoint owns the complete server-side handoff into RPS Dual and does access the
configured projection and database. The FHIR endpoint performs the analogous
bounded terminology-and-execution handoff into Resource Store. None performs
natural-language interpretation.

### What remains before the product journey is live

- natural-language question to governed semantic intent;
- candidate retrieval/ranking across several templates and archetypes;
- persistence/signing or later verification of terminology receipts outside the
  authenticated server-orchestrated request;
- safe multi-archetype joins and richer Boolean/temporal logic;
- unit/range-aware `DV_QUANTITY` planning and complete ordinal predicate
  validation (the catalog now preserves ordinal options, but the planner remains
  conservative);
- a provisioned environment containing compatible terminology and openEHR/FHIR
  activations, the pinned templates, reference data, current FHIR search
  projections, and the required live openEHR Atlas index;
- execution of the implemented, redacted
  [semantic qualification harness](./semantic-execution-qualification.md)
  against a named Atlas environment, plus a separate performance run that
  captures explain metrics without placing clinical rows in the retained
  qualification artifact;
- an auditable prune or generation-swap phase for orphan sidecars, followed by a
  full restart/resume and reconciliation test;
- opaque continuation tokens or a pinned read snapshot for stable pagination
  across separate requests while clinical records continue to change (current
  pages have deterministic ordering but use numeric offsets);
- migration of the IBM compatibility profile from scalar lineage to the
  chain-valued instance metadata required for strict semantic execution;
- stored FHIR `Coding.version` enforcement if release-level provenance is a
  requirement for the deployed cohort;
- a pre-JSON request-body byte limit at the API edge (typed list/string
  cardinalities are already bounded);
- the Data Lab facade/events that replace both fixture-backed Ask traces;
- MongoDB Agentic Engine integration for supervised query modelling; and
- realistic cross-patient correctness and performance benchmarks.

Two legacy management boundaries should be redesigned rather than treated as
the future tenant API. `POST /api/strategies/openehr/rps_dual/config` still
updates process-level preview/startup state, although active environment routes
no longer consume it. Stored AQL queries and templates also use fixed collection
names inside the selected database; environments intentionally sharing one
database therefore share those namespaces unless a later workspace policy
separates them.

Consequently, this is now an executable backend foundation with fail-closed
contracts, not yet a live natural-language Data Lab demo.

Two distinctions are release-critical. First, only the authenticated
terminology-planning and execution routes establish provider trust by invoking
the configured tenant gateway; a digest from the compile-only route establishes
content integrity, not issuer trust. Second, a successful unit or compiler test
does not prove that a particular deployment has materialized the matching
sidecar or has a queryable Atlas index. The execution route is designed to reject
that state, and the first live qualification run still needs to demonstrate it.

## Assets that should be kept

### From the new Data Lab

- Keep the industry-specific entrances and shared backend.
- Keep the outcome-first and system-first ways into one Builder canvas.
- Keep every capability labelled independently by execution state, runtime
  availability, evidence quality, and data provenance.
- Keep the interaction timeline and evidence UI as the universal way to explain
  deterministic and agent-assisted work.
- Keep anonymous discovery and simulation separate from authenticated persistence
  and operation.
- Keep application, architecture, strategy, semantic module, and source profile
  as distinct catalog concepts.

### From the legacy/private Data Lab and Context Studio

- Keep its native model inspection, semantic units, terminology bindings, query
  contracts, query-shape concepts, mapping tools, synthetic generation, and
  operational strategy tooling.
- Promote reusable contracts and deterministic compilers into Kehrnel.
- Treat the UI implementation as a design/reference client, not as the authority
  for execution semantics.

The existing JavaScript openEHR semantic-artifact compiler is a useful prototype:
it derives semantic units, labels, paths, datatypes, units, coded options,
containment chains, terminology entries, query contracts, and query shapes from a
preserved web template. Its resolver can turn a small class of English questions
into AQL. It should not simply become the production engine because:

- its tested input is an openEHR web-template JSON document, not a native OPT;
- it lives in the UI repository and duplicates domain behavior that belongs in
  Kehrnel;
- matching is lexical and rule-based, with a small fixed vocabulary;
- generated AQL covers only a subset of contract kinds;
- external terminology expansion and release pinning are not part of the flow;
- repeated-node and cross-archetype correlation are not proven; and
- the output is not passed through the active Kehrnel strategy/compiler.

## Target openEHR architecture

### 1. Template semantic catalog

Compile every pinned OPT into an immutable, versioned semantic catalog. The
catalog is a derived access artifact; the OPT remains authoritative.

Each catalog records:

- template ID, template version, composition archetype ID, source digest, and
  compiler version;
- every included archetype ID and RM type;
- every queryable semantic unit with a stable ID;
- absolute AQL path and path relative to its containing archetype;
- node code, label, description, translations, annotations, and term bindings;
- RM datatype, value path, supported operators, units, coded constraints,
  occurrences, and cardinality;
- containing archetype and containment chain;
- temporal paths and subject paths that may qualify the unit; and
- a correlation anchor identifying the repeated archetype/event instance in which
  predicates must agree.

Stable semantic-unit identity must include the template digest, archetype ID,
path, and RM type. An `at0001` code by itself is never an identifier because it is
local to an archetype and is commonly reused.

### 2. Governed semantic-query intent

The agent or deterministic resolver produces a constrained intent, never raw MQL
and preferably not raw AQL:

```json
{
  "contract": "openehr-semantic-query/v1",
  "scope": "cross_patient",
  "templateRef": {
    "templateId": "...",
    "sourceDigest": "sha256:...",
    "catalogDigest": "sha256:..."
  },
  "select": ["ehr_id", "composition_uid"],
  "predicates": [
    {
      "semanticUnitId": "...",
      "operator": "matches",
      "values": ["84114007", "..."],
      "terminologyExpansionRef": "...",
      "correlationGroup": "problem-instance"
    },
    {
      "semanticUnitId": "...",
      "operator": "gte",
      "value": "2025-01-01",
      "correlationGroup": "problem-instance"
    }
  ],
  "limit": 50
}
```

Kehrnel validates every reference, datatype, operator, unit, value set, temporal
constraint, correlation group, policy limit, and pinned digest. Unknown or
ambiguous bindings produce candidates for human confirmation; they do not become
queries.

### 3. Terminology resolution

Terminology is a pluggable service boundary. A terminology expression such as
SNOMED CT descendant-or-self is resolved against an explicitly pinned provider,
edition, version, and language. The result is a bounded, explicit code set plus a
receipt.

The generated AQL should contain explicit, parameter-bound codes (or a reference
that Kehrnel itself resolves before compilation). It must not send an opaque
`terminology://...` literal to the current AQL compiler.

### 4. Deterministic AQL planner

The planner binds semantic-unit IDs to validated archetype-relative paths and
generates standard AQL. For a single archetype instance the desired shape is:

```aql
SELECT
  e/ehr_id/value AS ehrId,
  c/uid/value AS compositionId
FROM EHR e
  CONTAINS COMPOSITION c[<composition archetype id>]
  CONTAINS EVALUATION problem[<problem archetype id>]
WHERE c/archetype_details/template_id/value = $templateId
  AND problem/<coded path>/defining_code/terminology_id/value = $terminology0
  AND problem/<coded path>/defining_code/code_string MATCHES {'195111005','84114007'}
  AND problem/<onset path>/value >= $from
LIMIT 50
```

Aliases and relative paths are important: they make the intended archetype
instance explicit. For predicates over repeated nodes, events, or multiple
archetypes, Kehrnel must either prove correlation in the generated MQL or reject
the query as unsupported. Independent array predicates that can match different
instances are clinically unsafe.

### 5. Existing RPS Dual compiler and execution

The resulting AQL goes through the existing strategy-level `compile_query` path.
That remains the single authority for:

- parsing and validation;
- patient-scoped versus cross-patient route selection;
- dictionary and shortcut resolution;
- `$match` versus Atlas Search pipelines;
- projection and pagination;
- active environment configuration; and
- execution against the configured collections.

No second AQL-to-MQL implementation should be added to the Data Lab.

### 6. Evidence contract

Every answer should retain references to:

- raw question retention policy and normalized intent;
- policy and tool-selection decision;
- terminology expression, release, provider, expansion digest, and bounded codes;
- OPT, archetype, semantic-catalog, mapping, strategy, compiler, and configuration
  digests;
- selected semantic units and rejected/alternative candidates;
- generated/effective AQL and parameters;
- compiled MQL, route, index requirements, and warnings;
- actual execution metrics and `explain` evidence when permitted; and
- result-document citations and answer bounds.

The existing Data Lab plan/run/event/evidence contracts are the correct UI-facing
envelope for these receipts.

## Agent boundary

MongoDB Agentic Engine should orchestrate the modeler/query assistant, but the
model must not invent executable clinical paths.

Allowed agent work:

- interpret the user's goal;
- retrieve candidate templates, archetypes, semantic units, and query contracts;
- ask for missing scope, unit, time, subject, or correlation decisions;
- propose a structured semantic-query intent;
- explain the proposed AQL/MongoDB route; and
- use outcome feedback as governed, scoped memory.

Deterministic Kehrnel work:

- parse OPTs;
- bind IDs and paths;
- resolve or verify terminology receipts;
- validate values, operators, cardinality, and correlation;
- render and validate AQL;
- compile AQL to MQL;
- enforce policy, limits, environment, and strategy versions; and
- execute and record evidence.

MCP can expose the same small set of allowlisted operations. It is a delivery
interface, not the semantic or execution authority.

## Product integration

The public Ask workbench should keep one interaction model for FHIR and openEHR.
The protocol selector changes the binding and compiler stages, not the product
experience.

For openEHR, add visible steps for:

1. intent interpretation;
2. terminology expansion;
3. template and archetype candidate selection;
4. semantic-unit/path binding;
5. ambiguity and policy approval;
6. deterministic AQL generation;
7. AQL-to-MQL compilation and route selection;
8. real execution and `explain` evidence; and
9. bounded answer with composition citations.

The Builder should reuse the same catalog at design time. Selecting an openEHR
source or AQL consumption should cause Core to identify the required templates,
semantic artifacts, terminology bindings, correlation capabilities, projections,
and indexes. A query learned in Ask can become a governed interaction contract;
a recurring query whose orchestration will not scale can become input to a new
optimized strategy proposal.

## Delivery sequence

### Slice 1 — deterministic semantic foundation: implemented in Core

- Native OPT semantic catalogs and archetype-qualified identities are
  implemented.
- The typed `openehr-semantic-query/v1` planner renders the bounded,
  single-archetype query slice to deterministic AQL.
- Exact path/value and repeated-instance correlation are covered in the strict
  RPS Dual compiler lane.
- The reference heart-failure pack, projection mappings, Atlas definition, and
  fixed cohort query are included.

Exit evidence today: unit and contract tests prove the deterministic artifacts
and generated MongoDB pipeline. This is code-level proof, not live deployment
proof.

### Slice 2 — governed execution foundation: implemented in Core, not deployed end to end

- Authenticated openEHR and FHIR terminology-backed execution endpoints are
  implemented.
- Activation and physical-target preconditions, bounded terminology expansion,
  projection readiness, exact compiled-query evidence, request-scoped tenant
  configuration, and fail-closed runtime errors are implemented.
- RPS Dual has a resumable, idempotent sidecar rebuild with a versioned readiness
  receipt, an exclusive lease, mutation fencing, and bidirectional reconciliation.

Still required for the exit criterion:

- provision a clean environment with both the selected domain strategy and its
  terminology provider;
- load the pinned templates and reference data, complete the sidecar rebuild, and
  wait for the exact Atlas index definition to become READY/queryable;
- exercise each endpoint over the network and retain the real result, citations,
  timings, index state, and explain receipt;
- define an auditable orphan-prune or generation-swap operation; and
- wire those responses into the Data Lab event/evidence envelope.

Exit criterion: the exact displayed AQL or FHIR filter, MongoDB query, result
count, citations, and live index evidence all come from one named run.

### Slice 3 — supervised agentic query modelling

- Integrate MongoDB Agentic Engine behind the allowlisted semantic-query tool.
- Retrieve semantic candidates rather than placing an entire catalog in a prompt.
- Require confirmation for ambiguity, missing units, policy-sensitive intent,
  unsupported correlation, or estimated expensive execution.
- Store scoped decision traces and feedback separately from canonical clinical
  records.

Exit criterion: free text can produce a candidate intent, but no unvalidated path,
code, AQL, or MQL reaches execution.

### Slice 4 — complex clinical questions at scale

- Support multiple predicates within the same repeated archetype/event instance.
- Add tested multi-archetype containment and temporal relationships.
- Add template-family/version compatibility and migration rules.
- Add cost/cardinality estimates and index recommendations to Builder proposals.
- Benchmark patient and cross-patient workloads with realistic distributions.
- Migrate and validate IBM-profile path-instance lineage before enabling the
  governed semantic execution route for `openehr.rps_dual_ibm`.

Exit criterion: complex cohort questions have semantic-equivalence tests,
correlation guarantees, performance budgets, and reproducible evidence.

## Immediate product readiness gates

The public openEHR Ask option must remain planned until all of these are true.
Gates 1–6 are enforced or exercised by current Core contracts; gates 7–10 still
require the provisioned Data Lab deployment:

1. The selected OPT and every referenced archetype are pinned by digest.
2. The semantic field bindings are generated from that OPT, not handwritten in a
   demo fixture.
3. Terminology expansion returns a pinned, bounded receipt and explicit codes.
4. Generated AQL passes syntax, binding, datatype, policy, and correlation checks.
5. Kehrnel compiles the exact displayed AQL through the active `rps_dual`
   configuration.
6. Every clinical and temporal predicate is present in the compiled MQL.
7. Execution occurs against a named synthetic data snapshot.
8. The selected route and index are supported by actual plan/explain evidence.
9. Results cite the authoritative compositions used to produce the answer.
10. Failure, ambiguity, outage, and unsupported-capability paths are tested.

## Recommendation

Do not build a separate openEHR conversational query engine in the Data Lab.
Promote OPT-derived semantics into Kehrnel, expose a small governed semantic-query
contract, and let the Data Lab remain the best possible visual client for it.

The existing public experience is good enough to keep. The next credibility jump
comes from replacing one planned trace with one narrow, real, fully evidenced
vertical slice—not from adding more simulated applications.

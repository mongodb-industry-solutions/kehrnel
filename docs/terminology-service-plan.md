# Kehrnel Terminology Services and SNOMED CT Strategy Plan

**Status:** Controlled-pilot delivery candidate; licensed-release/browser acceptance and production hardening remain
**Date:** 2026-09-05
**Primary runtime:** Kehrnel
**Customer workbench:** Healthcare Data Lab (HDL)
**Semantic consumer and governance plane:** Context Studio
**Reference native provider:** `snomedct.mongodb`

## Delivery snapshot

The current branch contains the pilot vertical slice across Kehrnel and HDL:

- dedicated native SNOMED strategy activation with no automatic licensed-data ingest;
- staged-release list, inspect, diff, dry-run/ingest, sidecar rebuild, indexes, and readiness through strategy operations, explicit domain APIs, CLI, and HDL;
- bounded search/typeahead, concept history, hierarchy, relationships, ECL-to-MQL, named ValueSets, code validation, subsumption, grounding review, reviewed-corpus query, and retrieval benchmarks;
- provider-neutral routing to native SNOMED or a customer FHIR terminology server, with encrypted HDL secret references and real provider verification;
- live native/FHIR provider capability discovery with declared-versus-advertised operation evidence;
- FHIR terminology endpoints and optional advisory/required import validation, including active-profile ValueSet bindings;
- IG compilation that inventories terminology bindings and invariants, plus an immutable activation-level terminology lock;
- mandatory tenant licence acknowledgement before a staged SNOMED release can be persisted;
- an HDL learning/operational journey and Context Studio validation evidence that can be pinned to a tenant semantic artifact.

The remaining work is deliberately classified as production hardening or a
later semantic/conformance increment: executing complete profile/invariant
validation through the customer's configured validator, terminology release
drift workflows, advanced external-provider auth and capability caching/health
policy, Atlas Search/vector retrieval, model-based extraction,
asynchronous benchmark jobs, complete ECL/post-coordination, and large-release
operational evidence.

This wording is intentional: the branch is suitable for a controlled
accelerator demonstration and fresh-tenant acceptance run, but it is not yet a
claim that Kehrnel is a production-complete terminology server. Automated
Kehrnel/HDL verification and a read-only compatibility check against the
published MongoDB SNOMED model have passed. A real tenant activation with the
customer-selected licensed release, external provider, restart/recovery, and
browser journey must still be recorded before calling a deployment
pilot-ready. A fresh empty environment has now been activated and reloaded
successfully against a dedicated database; it created only the declared empty
collections and indexes. The final deployment rehearsal still needs the
customer-selected licensed release and an authenticated HDL browser session.

## 1. Executive decision

This program has **two first-class deliverables**, not one primary service and
one implementation detail:

1. **SNOMED CT on MongoDB accelerator** — a complete persistence, release,
   indexing, search, ECL, hierarchy, grounding, benchmark, and API strategy,
   with the same learn → configure → use your data → query → inspect MQL →
   integrate journey that HDL provides for openEHR and FHIR.
2. **Shared terminology service** — a tenant-configurable server/gateway that
   exposes the native SNOMED strategy as one provider and lets customers route
   SNOMED, ICD, LOINC, UCUM, NCI, MedDRA, and other systems to their own
   terminology services.

Both products share the same Kehrnel execution. The complete SNOMED journey
must not be reduced to a generic `$validate-code` adapter, and the transversal
terminology service must not become coupled to the SNOMED document model.

Kehrnel should provide one tenant-governed terminology gateway that can route requests to:

1. the MongoDB-native `snomedct.mongodb` strategy;
2. a customer-selected remote FHIR terminology server;
3. later provider adapters for ICD, LOINC, UCUM, NCI controlled terminology,
   MedDRA, RxNorm, and other licensed or public code systems.

The existing SNOMED CT strategy must remain a first-class product and reference
implementation. It is not replaced by a generic proxy. The gateway gives all
Kehrnel strategies a stable contract; providers retain their specialized
capabilities underneath it.

Healthcare Data Lab owns configuration, learning, inspection, testing, and
operational enablement. Context Studio uses the same contract to propose,
review, pin, publish, and monitor terminology bindings in semantic maps.
Kehrnel remains the only runtime authority for terminology execution.

The target product statement is:

> A tenant-configurable terminology layer for healthcare on MongoDB: use the
> native MongoDB SNOMED CT accelerator or connect an existing terminology
> server; validate, search, expand, navigate, and bind terminology consistently
> across FHIR, openEHR, CDISC, Healthcare Data Lab, and Context Studio.

The companion SNOMED-specific statement is:

> A complete SNOMED CT persistence and query accelerator for MongoDB: load and
> govern licensed releases, understand the document model, search and navigate
> clinical meaning, compile ECL to MQL, ground and review clinical text, prove
> performance and accuracy, and expose the same capabilities through APIs.

### 1.1 Reference implementation located

The local repository corresponding to the published MongoDB Solution Library
implementation is:

```text
/Users/francesc.mateu/Documents/GitHub/snomed-ct-on-mongodb
```

Its configured Git remote is:

```text
https://github.com/mongodb-industry-solutions/snomed-ct-on-mongodb.git
```

The current local branch is `docs/solution-overview`. Treat that repository as
the behavior and experience reference. Kehrnel must own the reusable runtime
implementation, while HDL incorporates the journey rather than embedding or
calling the standalone Next.js application.

### 1.2 Existing published SNOMED database

The already-populated SNOMED CT database used by the standalone solution should
be registered as a controlled acceptance environment for the Kehrnel strategy.
It is valuable for realistic hierarchy, release, search, index, and grounding
verification, but it must not become an implicit application dependency.

- Connection details remain secret bindings.
- The database is attached through a normal `snomedct.mongodb` activation.
- Kehrnel readiness verifies the expected model, release, collections, and
  indexes before declaring it usable.
- HDL displays its configured release and capabilities, not credentials.
- Licensed content is never copied into Git fixtures, core metadata, support
  bundles, or public screenshots.
- CI continues to use a minimal license-safe fixture; the published full
  database is used for explicit acceptance/performance runs.

## 2. Why this is a platform capability

Terminology is not a FHIR-only feature:

- FHIR profiles bind coded elements to `ValueSet` resources.
- openEHR templates and AQL contain external terminology bindings and
  terminology-aware query semantics.
- CDISC relies on versioned controlled terminology, principally NCI and other
  domain-specific packages. SNOMED mappings may be useful but must never
  replace the authoritative CDISC terminology.
- Context Studio needs stable codes, displays, synonyms, hierarchy, value-set
  membership, and provenance to build a governed semantic map.
- Clinical note grounding needs terminology search and hierarchy, but is not
  itself terminology validation.

Making the terminology contract transversal prevents every strategy from
implementing its own remote-server client, cache, release policy, credentials,
error semantics, and audit trail.

## 3. Current-state assessment

### 3.1 What already exists in Kehrnel

The `snomedct.mongodb` strategy is an implemented preview with approximately
three thousand lines across the strategy, domain API, model/sidecar helpers,
configuration, manifest, specification, and contract tests.

Implemented capabilities include:

- canonical concept documents by release;
- a term-level sidecar by release and language;
- release discovery, inspection, comparison, and ingestion;
- sidecar rebuild and MongoDB index management;
- concept lookup;
- lexical terminology search;
- MongoDB-filtered search described as hybrid, with hierarchy and semantic
  facets;
- parent, child, ancestor, and descendant navigation;
- an explicitly bounded ECL parser/compiler/executor;
- relationship search;
- ECL/focus-concept expansion;
- clinical mention grounding to bounded candidates;
- readiness reporting;
- dedicated `/api/domains/snomedct/*` routes;
- an HDL SNOMED Terminology Explorer.

The canonical/source model and term-sidecar model align with the published
MongoDB reference architecture:

- concept-centered canonical documents keep descriptions, relationships,
  parents, children, ancestors, status, modules, refsets, and release metadata;
- term-level projections support language-aware retrieval and can evolve
  independently of the source terminology model;
- ancestor arrays enable indexed subsumption and descendant queries;
- reviewed grounding output can carry the original evidence and selected
  meaning.

### 3.2 What is not complete

The original implementation was a SNOMED application surface rather than a
shared terminology service. The first implementation slice now provides:

- a provider-neutral terminology contract and deterministic tenant routing;
- native SNOMED and external FHIR terminology providers;
- shared lookup, validation, bounded batch validation, subsumption, expansion,
  search, ECL, grounding, and translation operations;
- FHIR `CodeSystem`, `ValueSet`, and `ConceptMap` operation facades;
- an immutable native ValueSet registry with release and digest provenance;
- optional advisory or fail-closed terminology validation during FHIR import;
- active-profile binding enforcement against canonical ValueSet/version routes;
- immutable activation terminology locks with package/profile/binding/invariant digests;
- native and external-provider capability discovery;
- explicit licence acknowledgement before non-dry-run SNOMED ingestion;
- encrypted, write-only external-provider credentials managed by HDL;
- Context Studio validation through the same tenant gateway;
- an expanded HDL SNOMED journey covering its model, release operations,
  ECL-to-MQL, MongoDB extensions, terminology APIs, and named ValueSets.

Important production-hardening gaps remain:

- complete FHIRPath invariant execution still depends on a configured official
  or compatible validation engine; Kehrnel deliberately does not approximate it;
- complex FHIR `ValueSet.compose` ingestion and native ConceptMap authoring;
- pinned expansion artifacts for Context Studio;
- shared cache, circuit-breaker, durable audit-retention, and multi-instance
  provider-health policy (live capability and validation probes are implemented);
- no full Atlas Search/Vector Search implementation in the Kehrnel strategy;
  the current deterministic lexical pipeline uses normalized regular
  expressions;
- no full ECL or post-coordination claim;
- no complete production release-ingestion workflow for arbitrary RF2 input;
  the current accelerator expects the authorized JSON concept model.

These gaps should be closed through composition around the existing strategy,
not by rewriting it.

### 3.3 Parity with the published standalone solution

The standalone `snomed-ct-on-mongodb` project is the acceptance reference for
the complete accelerator experience. Its useful behavior must be classified as
Kehrnel runtime, HDL presentation, Context Studio integration, or intentionally
out of scope.

| Capability | Standalone solution | Kehrnel today | Target owner |
| --- | --- | --- | --- |
| Overview, architecture, model, licensing | Implemented | Manifest/spec and docs exist | HDL renders Kehrnel manifest/spec |
| Canonical concept model | Implemented | Implemented | Kehrnel SNOMED strategy |
| Term search sidecar | Implemented | Implemented | Kehrnel SNOMED strategy |
| Release hardening/stamping/diff | Implemented scripts | Partial ingest/inspect/diff | Kehrnel release operations |
| B-tree indexes | Implemented | Implemented | Kehrnel strategy apply/maintenance |
| MongoDB Search/autocomplete | Implemented | Not implemented; regex search | Kehrnel search adapter |
| Vector Search/auto-embedding | Implemented/configurable | Not implemented | Kehrnel optional search adapter |
| Hybrid fusion/reranking | Implemented/configurable | Current “hybrid” is filtered lexical | Kehrnel optional search/rerank adapters |
| Typeahead suggestions | Implemented | Implemented | Kehrnel API + HDL |
| Concept detail and hierarchy | Implemented | Implemented | Kehrnel API + HDL |
| Concept release history | Implemented | Implemented | Kehrnel release-aware query + HDL |
| ECL subset | Implemented | Implemented subset | Kehrnel compiler/executor |
| ECL → MQL explanation | Present | Compiler returns pipeline | HDL dedicated Query Lab |
| Clinical-note grounding | Rich staged workflow | Basic mention splitting + search | Kehrnel grounding workflow |
| Reviewer confirmation | Implemented | Implemented with source-text-off default | Kehrnel persistence + HDL review |
| Grounded-corpus query | Implemented | Implemented with ancestor-path index | Kehrnel query/API + HDL |
| Grounding benchmark | Implemented | Bounded retrieval benchmark implemented; extraction benchmark pending | Kehrnel evidence + HDL results |
| Contextual API examples/OpenAPI | Implemented | Implemented for domain, gateway, FHIR facade, and release lifecycle | Kehrnel OpenAPI + HDL |
| Generic terminology validation | Not its primary purpose | Implemented provider-neutral gateway | Kehrnel terminology gateway |
| External terminology providers | Not its primary purpose | FHIR terminology adapter, capability discovery, and HDL setup/test implemented | Kehrnel terminology gateway |

“Parity” does not mean mechanically copying every standalone route or UI
component. It means preserving the customer-visible behaviors and evidence in
the correct product layer.

## 4. Product boundaries

### 4.1 In scope

- Tenant configuration of one or more terminology providers.
- The MongoDB-native SNOMED strategy as a provider.
- Customer-owned external terminology servers.
- Routing by canonical code-system/value-set URI and optional operation.
- Versioned lookup, validation, expansion, subsumption, translation, and
  terminology search contracts.
- FHIR-standard terminology endpoints backed by the gateway.
- FHIR profile validation using the gateway for bindings.
- HDL configuration, validation, exploration, and operational evidence.
- Context Studio candidate discovery, human confirmation, version pinning,
  publication, and drift evidence.
- Secure provider secrets and tenant isolation.
- Truthful capabilities and explicit degradation.

### 4.2 Not initially in scope

- Reimplementing the complete HL7 FHIR validator in Kehrnel.
- Claiming complete ECL, SNOMED post-coordination, or description-logic
  classification.
- Redistributing licensed SNOMED CT, ICD, MedDRA, or other protected content.
- Replacing authoritative CDISC/NCI terminology with SNOMED.
- Silently merging answers from providers with different releases.
- Allowing an LLM to invent or approve a clinical code.
- Building a universal native database model for every terminology before a
  customer requirement exists.

## 5. Target architecture

```text
                         CONTROL / GOVERNANCE

 Healthcare Data Lab                                Context Studio
 provider setup, release ingest,                    candidates, bindings,
 exploration, validation, evidence                  semantic-map publication
                |                                            |
                +------------------+-------------------------+
                                   |
                         Kehrnel Terminology API
                         and provider gateway
                                   |
                 +-----------------+------------------+
                 |                                    |
       Kehrnel-native providers              External providers
       snomedct.mongodb                      FHIR terminology API
       future package providers             Snowstorm/native adapter
       local value-set artifacts            customer/vendor endpoints
                 |                                    |
        dedicated strategy DB                  customer authority

                            RUNTIME CONSUMERS

        FHIR validator / API    openEHR    CDISC    Context Runtime
```

### 5.1 Separation of responsibilities

| Component | Responsibility |
| --- | --- |
| Provider | Executes terminology-specific operations against one authoritative source. |
| Gateway | Authorizes, routes, normalizes, bounds, caches, audits, and reports provenance. |
| FHIR facade | Translates standard FHIR `Parameters` operations to/from the gateway contract. |
| FHIR profile validator | Applies StructureDefinitions and FHIRPath invariants, calling the gateway for terminology bindings. |
| HDL | Configures and tests providers and provides the terminology workbench. |
| Context Studio | Governs semantic bindings and publishes immutable references/expansions. |

Profile validation and terminology validation must remain separate contracts.
A FHIR validator understands `StructureDefinition`, cardinality, slicing,
FHIRPath constraints, and binding strengths. The terminology gateway answers
questions about codes, systems, versions, value sets, hierarchy, and maps.

## 6. Tenancy, activation, and database model

### 6.1 Initial deployment rule

Activate `snomedct.mongodb` in the same Kehrnel environment as its consumers,
with its own mandatory strategy database. FHIR and other strategy databases
must never contain SNOMED release collections.

This gives one clear authorization boundary and avoids premature cross-
environment sharing. Later, a tenant may publish a terminology activation as a
tenant-shared service, but cross-tenant use must remain prohibited.

### 6.2 Provider registry

The environment stores non-secret provider metadata:

- provider id and display name;
- provider type and adapter version;
- activation reference or remote base URL;
- supported code systems and operations;
- release/edition policy;
- languages;
- timeout and bounded-cache policy;
- health state and last capability refresh;
- enabled/disabled lifecycle;
- routing priority;
- provenance and audit metadata.

Credentials are secret bindings, never inline activation configuration.

### 6.3 Recommended provider types

1. `kehrnel-strategy`
   - dispatches to an activated Kehrnel strategy such as `snomedct.mongodb`;
   - inherits tenant authorization and strategy database isolation.
2. `fhir-terminology`
   - uses standard FHIR terminology operations;
   - suitable for HAPI, Ontoserver, Firely-compatible services, and other
     conformant endpoints.
3. `snowstorm`
   - optional specialized adapter for full SNOMED/ECL capabilities not exposed
     consistently through a generic server.
4. `package`
   - future adapter for pinned local terminology packages such as controlled
     terminology snapshots.

Do not create an unrestricted generic HTTP adapter. Provider types must define
an allowlisted protocol, bounded requests, and normalized output.

## 7. Provider-neutral contract

### 7.1 Core operations

| Operation | Purpose | MVP |
| --- | --- | --- |
| `capabilities` | Supported systems, releases, languages, operations, limits | Required |
| `health` | Connectivity and operational readiness | Required |
| `lookup` | Resolve a code to authoritative display/designations/properties | Required |
| `validate_code` | Validate code, display, system, version, active state, and optionally membership | Required |
| `expand` | Expand a canonical value set or bounded provider expression | Required |
| `subsumes` | Determine equivalent/subsumes/subsumed-by/not-subsumed | Required for SNOMED |
| `search` | Find bounded terminology candidates | Required for native SNOMED |
| `translate` | Translate codes through a named/versioned concept map | Provider capability |
| `ground` | Retrieve candidates for clinical mentions; never authoritative validation | Optional specialized capability |

### 7.2 Normalized request identity

Every request supports, where meaningful:

- tenant/environment id from authenticated context;
- provider hint, without allowing unauthorized provider selection;
- canonical `system` URI;
- `version` or edition URI;
- `code` and optional `display`;
- canonical `valueSet` URI/version;
- language/designation preferences;
- date or release policy;
- bounded paging/limit;
- purpose (`authoring`, `validation`, `query-expansion`, `grounding`);
- correlation id.

For SNOMED CT, version handling must support the canonical edition/version URI,
for example `http://snomed.info/sct/{moduleId}/version/{effectiveDate}`. A bare
local `releaseId` is not sufficient evidence at the public boundary.

### 7.3 Normalized result evidence

Every result must report:

- outcome and normalized issues;
- selected provider and adapter version;
- authoritative system and resolved version/release;
- matched code/display/designations;
- active/inactive status where available;
- value-set membership when requested;
- operation-specific rows or expansion;
- truncation/pagination state;
- cache status;
- execution time;
- request/result digests;
- warnings and explicitly used fallback;
- correlation id.

The gateway must never make a result look more conformant than the provider
reported.

### 7.4 Error contract

Normalize at least:

- provider unavailable;
- unsupported operation;
- unsupported system/version;
- unknown code;
- inactive code;
- display mismatch;
- not in value set;
- expansion too costly/truncated;
- ambiguous provider route;
- licensing/content unavailable;
- timeout/rate limit;
- invalid request.

Consumers need to distinguish an invalid code from a validator outage.

## 8. Routing and policy

### 8.1 Routing order

Route deterministically using:

1. exact canonical value-set override;
2. exact code-system URI and optional version/edition rule;
3. configured default provider only when that provider declares support;
4. otherwise fail with an unsupported-system result.

No provider may be selected solely because it returned a result first.

### 8.2 Fallback policy

Fallback is explicit per route:

- `none` — fail closed;
- `on_unavailable` — use a named secondary only for operational failure;
- `advisory_compare` — run a second provider for comparison without changing
  the authoritative answer.

Required validation should default to `none`. A different release on a fallback
server can produce a clinically different answer.

### 8.3 Validation modes

Consumers select:

- `disabled` — no terminology validation;
- `advisory` — accept data and attach/report findings;
- `required` — fail closed on invalid results or provider unavailability.

The FHIR profile validator additionally applies binding strength:

- `required`: code must be in the bound value set;
- `extensible`: report when no suitable code is present, according to policy;
- `preferred`: advisory;
- `example`: informational only.

## 9. MongoDB-native SNOMED provider

### 9.1 Preserve the existing model

Keep the current primary pattern:

- one canonical concept document per concept and release;
- one active term projection per description/language/release;
- indexed ancestor arrays for subsumption;
- relationship keys for common attribute refinements;
- separate reviewed grounding output and telemetry.

Do not store descendant arrays on every parent when the same query can use the
child's ancestor array. Do not mix telemetry or reviewed clinical text into the
canonical terminology collection.

### 9.2 Complete provider functionality

Add strategy operations for:

- `snomed_validate_code`;
- `snomed_subsumes`;
- standards-aligned lookup with designation/property selection;
- named value-set registration and versioned expansion;
- membership validation against refsets and ECL-backed value sets;
- inactive concept reporting and historical association metadata when present;
- edition/module/version URI normalization;
- bounded paging for search and expansions;
- a complete provider capability response.

The existing `snomed_expand_value_set` remains useful but must distinguish:

- an ad hoc ECL expansion;
- a focus-concept expansion;
- a named canonical ValueSet;
- a SNOMED reference set.

### 9.3 Search tiers

Declare search tiers truthfully:

1. `deterministic-lexical` — current normalized exact/prefix/substring search;
2. `atlas-lexical` — MongoDB Search autocomplete/fuzzy/scoring;
3. `semantic` — Vector Search over `embedText`;
4. `hybrid` — lexical/vector fusion and optional reranking.

Current Kehrnel search must not be labelled semantic or vector until those
indexes and execution paths are actually enabled and evidenced.

### 9.4 Grounding boundary

Grounding is a candidate-retrieval workflow:

1. extract mentions and context;
2. retrieve candidates from the authoritative terminology provider;
3. rank within the returned candidates;
4. require human or governed application confirmation;
5. persist confirmed coding, evidence span, assertion, subject, provider,
   release, and reviewer provenance.

An LLM may extract text and rank candidates. It must not invent concept ids,
silently persist a coding, or override terminology validation.

## 10. External terminology providers

### 10.1 FHIR terminology adapter

The first external adapter should use the standard FHIR operations:

- `CodeSystem/$lookup`;
- `CodeSystem/$validate-code`;
- `ValueSet/$validate-code`;
- `ValueSet/$expand`;
- `CodeSystem/$subsumes` where supported;
- `ConceptMap/$translate` where supported;
- `TerminologyCapabilities` discovery where supported.

Use POST with FHIR `Parameters` by default to avoid sensitive or large values in
URLs. Normalize vendor-specific failures without discarding the original
OperationOutcome evidence.

### 10.2 Provider onboarding

Provider creation must include:

- base URL and adapter type;
- secret reference/authentication method;
- TLS and hostname validation;
- explicit allowed host policy to prevent SSRF;
- connection test;
- capability discovery;
- administrator confirmation of systems/releases;
- route assignment;
- validation of timeout and result limits.

### 10.3 ICD and other terminologies

Support them through the same generic operations, but do not impose SNOMED
semantics:

- ICD hierarchy does not use ECL;
- UCUM validation has unit-specific rules;
- LOINC has part/property semantics;
- NCI controlled terminology is package/release governed;
- MedDRA has licensing and hierarchy rules;
- ConceptMap translation requires a named mapping authority and version.

External FHIR terminology support provides broad initial interoperability.
Native MongoDB providers should be added only when they deliver a distinct
MongoDB operational value proposition or a customer requires them.

## 11. Public API surfaces

### 11.1 Provider-neutral Kehrnel API

Add an authenticated tenant-scoped surface such as:

```text
GET  /api/terminology/capabilities
GET  /api/terminology/providers
POST /api/terminology/providers/{id}/test
POST /api/terminology/lookup
POST /api/terminology/validate-code
POST /api/terminology/expand
POST /api/terminology/subsumes
POST /api/terminology/search
POST /api/terminology/translate
POST /api/terminology/ground
```

Administrative provider creation/update remains under an admin/environment
route and requires stronger authorization than lookup/search.

### 11.2 FHIR terminology facade

Expose standard FHIR operations from the FHIR domain:

```text
POST /api/domains/fhir/CodeSystem/$lookup
POST /api/domains/fhir/CodeSystem/$validate-code
POST /api/domains/fhir/CodeSystem/$subsumes
POST /api/domains/fhir/ValueSet/$validate-code
POST /api/domains/fhir/ValueSet/$expand
POST /api/domains/fhir/ConceptMap/$translate
```

Inputs and responses are FHIR `Parameters`; failures are FHIR
`OperationOutcome`. The FHIR `CapabilityStatement` advertises only operations
that the active terminology routes can currently execute.

Keep `/api/domains/snomedct/*` as the specialized navigation, ECL, model,
release, and grounding surface. Do not force specialized SNOMED workflows into
generic FHIR operations.

### 11.3 CLI and SDK

Provide parity for:

- provider list/show/test;
- route list/set/validate;
- lookup/validate/expand/subsumes/search;
- SNOMED release inspect/diff/ingest/rebuild/readiness;
- export of capability and validation evidence.

All clients call Kehrnel. HDL and Context Studio must not contain competing
terminology execution logic.

## 12. FHIR integration

### 12.1 Activation

FHIR activation selects:

- profile-validation engine;
- terminology policy (`disabled`, `advisory`, `required`);
- provider routing profile;
- version policy;
- timeout and maximum expansion size;
- treatment of display mismatches, inactive concepts, and warnings.

It should reference provider ids, not duplicate endpoint credentials.

### 12.2 Write and import pipeline

Use one pipeline for create, update, transaction Bundle, regular import, and
synthetic persistence:

```text
parse
  -> base structural validation
  -> active-profile selection
  -> StructureDefinition validation
  -> FHIRPath invariant evaluation
  -> terminology binding validation through gateway
  -> reference/identity resolution
  -> search and compartment projection
  -> index verification
  -> persistence
```

Dry-run reports every layer separately. An unavailable terminology provider is
not reported as an invalid code.

### 12.3 Profile and terminology locks

IG compilation should produce a terminology dependency manifest containing:

- canonical ValueSet URL and version;
- binding strength and element path;
- referenced CodeSystems;
- selected provider route;
- resolved release/edition where known;
- whether expansion is materialized, deferred, or unsupported;
- content/expansion digest;
- compilation timestamp and tool version.

Do not eagerly expand unbounded value sets during activation. Large expansions
become bounded asynchronous jobs and immutable artifacts.

## 13. Healthcare Data Lab journey

SNOMED should become a complete terminology journey rather than one 1,500-line
explorer screen.

This journey is a peer of the openEHR and FHIR journeys. It is not merely a
settings page for a server used elsewhere. A terminology architect, clinician,
data engineer, or application developer should be able to understand and use
the SNOMED accelerator directly inside HDL.

### 13.0 Canonical SNOMED journey

```text
UNDERSTAND
  concept graph, descriptions, relationships, releases, collections, indexes
      ↓
ACTIVATE
  dedicated database, release/languages, licensed source, search capabilities
      ↓
LOAD OR UPDATE
  inspect → diff → dry run → ingest → build sidecar/indexes → promote release
      ↓
NAVIGATE
  find concept → inspect → parents/children/ancestors/descendants/relationships
      ↓
QUERY
  author ECL → parse AST → compile MQL → execute → inspect index evidence
      ↓
GROUND AND REVIEW
  clinical text → mentions/context → bounded candidates → confirm coding
      ↓
SAVE AND REUSE
  ECL/value sets, confirmed bindings, regression cases, grounded corpus
      ↓
INTEGRATE
  native SNOMED API + provider-neutral terminology API + FHIR terminology API
      ↓
OPERATE
  release readiness, search quality, benchmark, jobs, provider health, drift
```

SNOMED does not generate synthetic terminology. The licensed release is its
model/data source. HDL may provide synthetic, license-safe clinical notes for
grounding exercises and small public concept fixtures for demonstrations.

### 13.0.1 Model presentation parity

The Strategy Studio presentation should follow the same tab contract used by
the mature openEHR/FHIR experiences, adapted to SNOMED terminology:

1. **Overview** — problem, challenges, implemented scope, and product boundary.
2. **Data model** — concept, description, relationship, refset/member, release,
   and reviewed-coding metamodel.
3. **Collections** — canonical concepts, term sidecar, grounded notes, and
   telemetry as separate abstract roles; show configured physical names only
   after activation.
4. **Relationships & lineage** — RF2/authorized JSON → canonical concept → term
   projection → candidates → reviewed coding; include release lineage.
5. **Indexes & access patterns** — concept lookup, term search, ancestors,
   descendants, relationships, refsets, release filtering, and grounded-corpus
   queries.
6. **Queries** — ECL grammar subset, AST, generated MongoDB pipeline, execution,
   and unsupported constructs.
7. **Operations & APIs** — ingest, rebuild, readiness, navigation, grounding,
   validation, terminology gateway, and OpenAPI.
8. **Manifest/raw contract** — advanced view of the exact Kehrnel source of
   truth.

The abstract model always comes from the strategy manifest/spec. HDL must not
reverse-engineer it from whatever collections happen to exist in the tenant
database.

### 13.1 Navigation

Recommended sections:

1. **Overview & Capabilities**
   - what terminology provides;
   - native MongoDB model;
   - active providers, systems, releases, languages, and limitations.
2. **Providers**
   - activate MongoDB SNOMED;
   - connect external server;
   - test connection and capabilities;
   - configure routes and validation policy.
3. **Releases & Data**
   - stage/inspect licensed release;
   - dry-run/diff/ingest;
   - build sidecar and indexes;
   - readiness and release history.
4. **Terminology Explorer**
   - lexical/Atlas/semantic modes as supported;
   - typeahead suggestions and search provenance;
   - concept details, descriptions, hierarchy, relationships, raw document;
   - release history for the selected concept;
   - ECL parse, compile, MQL, execute;
   - pagination and truncation evidence.
5. **Validation & Value Sets**
   - validate code/display/version;
   - validate value-set membership;
   - expand named ValueSet/refset/ECL;
   - compare providers in advisory mode;
   - save governed value sets.
6. **Ground Clinical Text**
   - deterministic extraction or an explicitly configured model-assisted
     extraction path;
   - evidence spans, assertion, temporality, and subject context;
   - bounded candidates;
   - human review, accept/reject/swap/add;
   - confirmed coding provenance;
   - grounded-corpus query by concept or ancestor.
7. **API & Integration**
   - native Kehrnel terminology API;
   - standard FHIR terminology API;
   - executable OpenAPI console;
   - copyable application examples.
8. **Activity & Health**
   - ingest/rebuild/expansion jobs;
   - provider health and latency;
   - cache and release state;
   - bounded audit evidence;
   - search and grounding benchmark runs against governed fixtures.

### 13.2 Activation experience

The activation wizard should offer:

- **MongoDB SNOMED CT** — dedicated database, release, languages, source
  staging, sidecar/search tier, index plan, licensing confirmation;
- **Existing terminology server** — type, URL, secret, capability test, systems,
  releases, languages;
- **Both** — deterministic route table and optional advisory comparison.

Activation must preview the collections/indexes for native SNOMED and the exact
provider routing policy. No data ingestion should happen merely because the
strategy was activated.

### 13.3 Operational workbench requirements

Users must be able to return to their tenant and:

- see the active release and readiness;
- change a provider through a reviewed activation revision;
- ingest a new release and compare before promotion;
- preserve searches, ECL expressions, value sets, validation cases, and
  confirmed bindings;
- export stable API examples and evidence for production implementation.

### 13.4 API sandbox

HDL should expose an executable, tenant-bound console generated from Kehrnel's
OpenAPI. It must cover both intentions:

- the specialized SNOMED accelerator APIs for navigation, ECL, grounding,
  releases, and benchmark evidence;
- the provider-neutral and FHIR terminology operations used when the strategy
  acts as a server.

Contextual request examples should be available directly beside a selected
concept, ECL query, validation result, or confirmed coding. HDL proxies
authenticated requests; credentials are never rendered in generated examples.

### 13.5 Benchmark boundary

The standalone project's extraction benchmark is valuable and should be
preserved, but moved to the correct layers:

- Kehrnel owns benchmark cases, execution, scores, model/provider provenance,
  and stored evidence;
- HDL selects fixtures/models/search modes and visualizes results;
- Context Studio may consume the evidence when selecting a semantic
  extraction policy;
- custom gold cases require reviewer attribution and revision history;
- benchmark notes must be synthetic, de-identified, or explicitly approved.

## 14. Context Studio integration

### 14.1 Role of terminology

Context Studio uses terminology during semantic discovery and governance, not
as an unconstrained runtime guessing mechanism.

```text
source evidence
  -> extract terminology-bearing fields and candidate phrases
  -> query Kehrnel terminology gateway
  -> present bounded candidates with provider/release evidence
  -> reviewer confirms/rejects binding
  -> compile versioned semantic-map artifacts
  -> deterministic runtime after binding
```

### 14.2 Terminology binding artifact

A governed binding should include:

- stable binding id;
- semantic object/node and physical source paths;
- system URI, code, display, version;
- optional ValueSet canonical/version and binding strength;
- provider and resolved release;
- discovery evidence and alternative candidates;
- reviewer decision, identity, time, and rationale;
- validation result and digest;
- lifecycle (`draft`, `review`, `approved`, `deprecated`);
- supersession/replacement link;
- source and compilation versions.

### 14.3 Published semantic artifacts

For deterministic execution, publication pins:

- terminology routes or provider requirements;
- exact code-system/value-set versions;
- approved bindings;
- bounded expansion snapshots when runtime expansion is unnecessary;
- expansion digest and generation evidence;
- drift checks for newer releases.

A published semantic map should not silently change because the terminology
provider activated a new release. Release promotion creates a reviewed revision
and drift report.

### 14.4 Natural-language use

The model may propose semantic bindings and cohort intent. It must use
provider-returned candidates and produce a structured, reviewable plan.
Execution uses approved code ids, value sets, expansions, and physical
bindings. This preserves Context Studio's rule: exploratory before binding;
deterministic after binding.

## 15. Other strategy integrations

### 15.1 openEHR

- Resolve external terminology bindings during template review.
- Validate coded values during composition ingestion when configured.
- Compile terminology-aware AQL functions and value-set predicates through
  pinned expansions.
- Keep openEHR local terminology distinct from external code systems.

### 15.2 CDISC

- Use the gateway for provider-neutral validation, but retain NCI/CDISC
  packages as the authority where mandated.
- Pin terminology package/version in study artifacts and validation evidence.
- Treat SNOMED mappings as optional semantic projections, never replacements.

### 15.3 Future domains

Every domain declares required generic operations and any specialized provider
capabilities. A strategy must not hardcode a direct connection to a terminology
vendor.

## 16. Persistence model

### 16.1 Control-plane collections

Store provider configuration and policy outside clinical strategy databases:

- `terminology_providers` — non-secret provider metadata;
- `terminology_routes` — canonical system/value-set routing rules;
- `terminology_validation_cases` — user-authored regression cases;
- `terminology_artifacts` — saved expansions, locks, and digests;
- `terminology_audit` — bounded operational evidence.

Follow the existing tenant/environment ownership model. Secrets remain in the
configured secret provider.

### 16.2 Native provider collections

The `snomedct.mongodb` strategy owns its dedicated terminology database:

- canonical concepts;
- term search projection;
- optional named value-set/refset metadata;
- optional reviewed grounded output;
- strategy-owned operational metadata.

Collection names remain configurable. Their abstract roles must be declared in
the strategy manifest/spec so HDL can render the model without reading tenant
data.

### 16.3 Cache

Cache only deterministic, non-PHI terminology results by default. Cache keys
include provider, operation, system, version, value set, language, parameters,
and release. Invalidate or namespace on provider capability/release changes.

Clinical note grounding input and results must not enter a shared terminology
cache. Any persistence requires explicit workflow configuration and retention
policy.

## 17. Security, licensing, and operations

- Enforce tenant/environment authorization on every operation.
- Restrict remote provider hostnames and protocols; prevent SSRF and redirects
  to unapproved hosts.
- Store credentials in secret bindings and redact headers from logs.
- Bound request size, expansion size, search limits, ECL complexity, timeout,
  retries, and concurrency.
- Do not retry non-idempotent provider operations automatically.
- Record provider, release, latency, cache usage, outcome, and correlation id,
  but avoid recording clinical note text or patient payloads.
- Surface circuit-breaker/open state as provider unavailable, never invalid
  terminology.
- Require a licensing acknowledgement before native SNOMED ingestion.
- Do not package or export licensed terminology content in public fixtures,
  backups, logs, screenshots, or support bundles.
- Use synthetic/license-safe samples for CI and demonstrations.

## 18. Delivery phases and tickets

The phases below are sequenced by dependency rather than calendar estimate.

After Phase 0, work proceeds through two coordinated tracks:

```text
                         Phase 0 shared contracts
                                  |
              +-------------------+-------------------+
              |                                       |
    Track A: complete SNOMED                 Track B: terminology server
    persistence/query journey               gateway + external providers
              |                                       |
              +-------------------+-------------------+
                                  |
                    FHIR / HDL / Context Studio
```

Track A is independently deliverable as the MongoDB SNOMED accelerator. Track B
is independently useful to customers retaining their existing terminology
server. Neither should wait for every feature in the other, but both must use
the Phase 0 contracts.

### Phase 0 — Contract and ownership

**Goal:** Freeze the shared boundary before extending provider features.

#### T0.1 — Terminology contracts

- Define provider interface, request/result/error schemas, capability document,
  pagination/truncation, and provenance.
- Define generic operation semantics and specialized-capability extension
  points.
- Publish JSON Schema/OpenAPI and Python types.

**Acceptance:** a fake provider passes lookup, validation, expansion,
subsumption, unsupported-operation, timeout, and truncation contract tests.

#### T0.2 — Tenant provider registry and routing

- Persist provider metadata/routes.
- Add activation references and secret bindings.
- Implement deterministic route resolution and explicit fallback modes.
- Validate canonical system/version identifiers.

**Acceptance:** two tenants can route the same code system differently with no
cross-tenant access; ambiguous/missing routes fail explicitly.

#### T0.3 — Gateway API, authorization, and evidence

- [x] Implement provider-neutral endpoints.
- [x] Normalize errors and provenance.
- Add bounded cache, audit metadata, metrics, health, and circuit breaking.

**Acceptance:** all requests are tenant scoped, bounded, correlated, and report
the actual provider/release used.

### Phase 1 — MongoDB SNOMED as a complete gateway provider

**Goal:** Complete the existing persistence/query accelerator journey and make
the same strategy usable as a validation and expansion authority for its
declared scope.

#### T1.0 — Publish and activate the existing strategy cleanly

- Verify `snomedct.mongodb` discovery through the normal strategy catalog.
- Require a dedicated strategy database at activation.
- Make activation create configuration/index prerequisites but never ingest a
  licensed release automatically.
- Produce readiness and next-action evidence for an empty, partial, and ready
  database.

**Acceptance:** a new tenant can discover and activate the strategy through
CLI, API, and HDL, restart Kehrnel, and retain the activation without any
licensed terminology being copied into core/transversal storage.

#### T1.1 — Provider adapter over existing operations

- Adapt lookup, search, expansion, hierarchy, and readiness to the shared
  contract without duplicating their implementations.
- Normalize SNOMED canonical edition/version URIs.

#### T1.2 — Code validation and subsumption

- Validate active/inactive concept, system, version, display/designation.
- Add subsumption with deterministic ancestor queries.
- Report historical-association evidence where source data supplies it.

#### T1.3 — Value-set registry and membership

- Register named versioned value sets backed by refset, ECL, explicit concepts,
  or composed includes/excludes within the supported subset.
- Add membership validation and bounded expansion.
- Persist immutable expansion artifacts with digests when requested.

#### T1.4 — Search tiers

- Rename the current filtered lexical mode truthfully.
- Add Atlas Search lexical execution and evidence.
- Add optional Vector Search/auto-embedding and fusion only after indexes and
  capability checks prove availability.

#### T1.5 — Complete navigation and release-query parity

- [x] Add bounded typeahead suggestions.
- [x] Add concept history across releases.
- [x] Complete direct parent/child/ancestor/descendant and relationship evidence.
- [x] Add offset pagination/continuation metadata to search, hierarchy, relationship, and expansion
  results.
- [x] Preserve the ECL AST → MongoDB pipeline → execution-evidence path.

#### T1.6 — Complete grounding and reviewed-corpus parity

- Replace naive line splitting with a governed mention/context extraction
  contract.
- Support deterministic extraction and an optional configured model adapter.
- [x] Return bounded MongoDB-authoritative candidates.
- [x] Add reviewer confirmation/rejection and provenance. Replacement is expressed by selecting a different candidate before save.
- [x] Persist confirmed codings separately from source terminology, hashing and discarding source text by default.
- [x] Query confirmed codings by exact concept and ancestor.

#### T1.7 — Benchmark and golden evidence

- [x] Add bounded deterministic retrieval benchmarks with hit@k and mean reciprocal rank.
- Port the extraction/selection behavior of the standalone grounding benchmark into Kehrnel jobs.
- Support built-in license-safe fixtures and tenant-authored reviewed fixtures.
- Measure extraction, candidate retrieval, selection, exclusion/negation,
  latency, token usage, provider/release, and search mode separately.
- Keep evaluation evidence versioned and reproducible.

#### T1.8 — Strategy publication and tenant activation

- Bump manifest/spec/schema versions.
- Publish complete capability/readiness metadata.
- Add dedicated database enforcement and activation preview.
- Add release promotion and rollback-safe configuration revisions.

**Phase acceptance:** a tenant activates native SNOMED, ingests a licensed or
license-safe release, validates a code/display/value-set membership, expands a
bounded set, searches and navigates concepts, inspects ECL-to-MQL, grounds and
confirms a clinical note, queries the reviewed corpus, runs a benchmark, and
receives provider/release evidence through both specialized and generic APIs.

### Phase 2 — Customer terminology servers

**Goal:** Let the tenant use its existing terminology investment.

#### T2.1 — External FHIR terminology adapter

- [x] Implement standard FHIR Parameters operations.
- [x] Preserve OperationOutcome evidence.
- [x] Discover capabilities and compare them with declared operations.
- Cache discovery results only when invalidation and multi-instance behavior are defined.
- Add OAuth/API-key/basic/mTLS secret-binding patterns as platform-supported.

#### T2.2 — Provider administration

- [x] Add tenant-scoped configuration, create/update/remove, and real test flows.
- [x] Validate URL/TLS, secret references, systems, and declared operations.
- [x] Make configuration updates atomic so a provider cannot be removed while a retained route still references it.

#### T2.3 — Routing and comparison

- [x] Route deterministically by system/value-set/version, with no silent fallback.
- Add an explicit secondary provider only when an outage policy is designed.
- Add advisory comparison reports when a customer migration needs them.

**Phase acceptance:** the same gateway validation suite passes against native
SNOMED and a test FHIR terminology server; routing and evidence identify the
selected provider without leaking credentials.

### Phase 3 — FHIR terminology and profile conformance

**Goal:** Make terminology operational in the FHIR accelerator.

#### T3.1 — FHIR terminology facade

- Expose standard terminology operations and FHIR Parameters responses.
- Advertise only runtime-supported operations in CapabilityStatement.

#### T3.2 — Validator integration

- [x] Provide the fail-closed command-validator adapter boundary.
- [x] Pass active profiles/package sources and keep terminology gateway access separate.
- [x] Separate structure, profile/invariant, terminology, and reference findings.
- Rehearse the customer's selected official/compatible validator and IG before deployment acceptance.

#### T3.3 — IG terminology dependency compilation

- [x] Extract canonical terminology resources, profile bindings, strengths, and versions.
- [x] Produce an immutable activation terminology lock with package/profile/invariant evidence.
- Resolve every selected external release and record promotion/drift evidence when a customer deployment requires it.

#### T3.4 — Unified write/import enforcement

- [x] Apply the same policy to FHIR REST writes, Data Factory imports, and synthetic persistence through the shared importer.
- [x] Add advisory, required, provider-unavailable, and required-binding enforcement tests.
- Transaction Bundle execution remains outside the current accelerator interaction boundary.

**Phase acceptance:** a profile-bound FHIR resource is accepted/rejected
correctly against both native SNOMED and an external provider, with identical
normalized evidence and a standard FHIR outcome.

### Phase 4 — Healthcare Data Lab terminology journey

**Goal:** Make the capability understandable and operational for tenants.

#### T4.1 — Strategy Studio parity

- [x] Add SNOMED theme/order and a manifest-driven data-model visualization.
- [x] Show canonical collection, term sidecar, indexes, release lineage, and query
  patterns.

#### T4.2 — Provider Configuration Center

- [x] Native/external/both setup through the tenant terminology configuration.
- [x] Secret binding, real test, capability discovery, routes, releases, languages,
  validation mode, and activation review.

#### T4.3 — Refactor the SNOMED explorer

- [x] Present provider/release, search, concept, hierarchy/ECL,
  validation/value-set, grounding, and API workbenches.
- [x] Drive operational features from runtime capabilities and readiness.
- [x] Port the useful workflow and evidence patterns from the standalone reference
  application without copying its local runtime logic into HDL.

Splitting the current explorer component into smaller source files is a
maintainability improvement, not a pilot capability gate.

#### T4.4 — Terminology validation lab

- [x] Interactive lookup/validate/expand/subsumes through the Kehrnel boundary.
- [x] Provider selection/comparison evidence in the operational provider center.
- [x] Links from FHIR binding findings to profile/path evidence.
- Saved validation cases and regression replay remain a later operational enhancement.

#### T4.5 — Operational activity

- [x] Release inspect/diff/ingest and sidecar/index rebuild activity.
- [x] Explicit license acknowledgement and data-scope notices.
- Large expansion jobs and shared cache operations remain production hardening.

**Phase acceptance:** a new user can activate local SNOMED or an external
server, understand the model, ingest/test a release, validate and expand codes,
search and navigate its hierarchy, inspect ECL-to-MQL, ground and confirm a
clinical note, query the grounded corpus, execute the API, run a benchmark,
save evidence, and return later to the same tenant state.

### Phase 5 — Context Studio semantic-map integration

**Goal:** Use terminology as governed semantic evidence.

#### T5.1 — Kehrnel terminology client

- Add one typed client to Context Studio.
- Consume capabilities and normalized evidence; do not query SNOMED MongoDB
  collections directly.

#### T5.2 — Terminology source and discovery

- Register terminology providers as semantic evidence sources.
- Detect terminology-bearing fields and request bounded candidates.
- Store proposals separately from approved bindings.

#### T5.3 — Binding review

- Show candidates, displays, synonyms, hierarchy, provider, release,
  confidence, evidence, and alternatives.
- Confirm/reject with reviewer provenance.

#### T5.4 — Publication and drift

- Publish version-pinned binding and expansion artifacts.
- Add release-drift detection and reviewed promotion.
- Generate terminology-backed deterministic query/cohort plans.

**Phase acceptance:** Context Studio proposes a SNOMED binding from text,
retrieves only provider-backed candidates, requires confirmation, publishes a
pinned semantic-map artifact, and deterministically uses it in a query or
cohort plan.

### Phase 6 — Broader strategy adoption

- openEHR external terminology binding validation and terminology-aware AQL.
- CDISC/NCI package provider and validation evidence.
- External ICD/LOINC/UCUM coverage through the FHIR terminology adapter.
- Add native providers only when justified by customer value.

### Phase 7 — Production hardening when demanded

- Large release/expansion performance evidence.
- Shared cache and multi-instance invalidation.
- Advanced provider failover and regional deployment.
- Full ECL/post-coordination through a capable provider.
- RF2-native ingestion if the authorized customer source requires it.
- SLOs, capacity guidance, immutable audit integration, and disaster recovery.

## 19. Testing strategy

### 19.1 Provider contract suite

Run the same tests against every adapter:

- valid, invalid, inactive, unknown, and display-mismatch codes;
- explicit and missing versions;
- named value-set membership and non-membership;
- bounded/truncated expansion and paging;
- subsumption outcomes;
- unsupported operation;
- timeout, unavailable, and malformed provider response;
- tenant isolation and authorization;
- provenance completeness.

### 19.2 Native SNOMED fixtures

Use a small, license-safe graph containing:

- active/inactive concepts;
- preferred and acceptable designations in two languages;
- parent/ancestor relationships;
- one refset;
- one historical association;
- one relationship refinement;
- enough ambiguity to test candidate ranking.

### 19.3 FHIR integration fixtures

- one base-valid resource with valid required binding;
- invalid code;
- valid code outside required ValueSet;
- display mismatch;
- inactive code;
- external-provider outage in advisory and required modes;
- one profile FHIRPath invariant violation;
- one collection Bundle where required terminology validation prevents all writes.

### 19.4 Context Studio fixtures

- candidate proposal without automatic approval;
- confirmed binding with provider/release evidence;
- rejected candidate retained as governance evidence;
- pinned expansion replay;
- provider release drift requiring review;
- deterministic query/cohort output before and after approved revision.

## 20. Definition of pilot-ready

The terminology capability is ready for a supported pilot when all of the
following are true:

1. `snomedct.mongodb` is published and activates against a dedicated tenant
   database.
2. Activation does not ingest licensed data automatically.
3. The tenant can ingest/inspect/diff a release and build required indexes.
4. Native lookup, validate-code, expand, subsumes, search, hierarchy, and ECL
   work for the declared subset.
5. A customer can configure and test an external FHIR terminology server.
6. Deterministic route selection and no-silent-fallback policies are enforced.
7. FHIR terminology operations expose standard Parameters/OperationOutcome.
8. FHIR profile validation distinguishes profile, invariant, terminology,
   reference, and provider-availability findings.
9. HDL supports the full native/external provider journey using the active
   tenant configuration.
10. Context Studio can propose, confirm, pin, publish, and replay terminology
    bindings through Kehrnel.
11. Every result records provider/release provenance without leaking secrets or
    PHI.
12. A fresh-tenant acceptance run and restart/recovery rehearsal pass.

The current feature slice satisfies items 1-7, 9, and 11 in automated
verification. Item 8 has the fail-closed validator adapter, separate finding
categories, required ValueSet-binding enforcement, and immutable constraint
lock; the final proof still requires running the customer's selected validator
and IG. Item 10 has validation and pinned evidence but still needs the complete
propose/approve/publish/replay governance rehearsal. Item 12 has passed empty
fresh-environment activation and reload with zero licensed concepts copied, but
still needs the authenticated HDL journey and customer-selected licensed
release. These remain explicit acceptance gates; they must not be inferred from
successful base-schema or terminology validation.

## 21. Acceptance milestone

The implemented vertical slice proves the architecture end to end through the
following capabilities:

1. define the provider-neutral contract and registry;
2. adapt existing MongoDB SNOMED lookup/search/expansion;
3. implement native SNOMED `validate_code` and `subsumes`;
4. expose provider-neutral APIs;
5. add one external FHIR terminology adapter;
6. configure both in HDL and demonstrate deterministic routing;
7. connect one FHIR required binding to the gateway;
8. connect one Context Studio terminology binding to the gateway;
9. validate the Context Studio terminology surface and pin provider/release
   evidence to the tenant semantic artifact.

The acceptance story should be deliberately small and credible:

> Activate a licensed/sample SNOMED release in MongoDB, configure it as the
> authority for `http://snomed.info/sct`, validate a FHIR `Condition.code`, use
> an ECL-backed value set to query a broader clinical meaning, confirm the same
> concept in Context Studio, and publish a version-pinned semantic binding with
> complete provider and release evidence.

That vertical slice proves the strategic value without waiting for every code
system, complete ECL, or production-scale terminology operations.

The parity slice is now present: concept history/navigation, ECL-to-MQL,
governed grounding, reviewed-corpus query, retrieval evidence, and executable
API surfaces are implemented in Kehrnel and exposed in HDL. Work remaining
after this controlled-pilot delivery is narrower:

1. Atlas Search/vector retrieval and production-scale performance evidence;
2. complete Context Studio propose/approve/publish/replay and release-drift governance;
3. an authenticated fresh-tenant browser run with a customer-licensed release;
4. a real external terminology provider and official/compatible FHIR validator rehearsal;
5. multi-instance cache, circuit-breaker, auth, audit, SLO, and recovery hardening.

## 22. Decisions to preserve

- Kehrnel owns execution; HDL and Context Studio consume contracts.
- Native SNOMED is a reference accelerator and selectable provider, not a
  mandatory dependency.
- Customer terminology servers remain first-class and can be authoritative.
- Provider routes and releases are tenant-governed and versioned.
- No silent fallback or silent release change.
- Clinical terminology content remains licensed and customer supplied.
- Candidate generation is not validation, and validation is not human coding
  approval.
- Semantic exploration may use AI; approved runtime bindings remain
  deterministic, inspectable, and reproducible.

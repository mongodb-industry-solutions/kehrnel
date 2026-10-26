# Source-aware strategy compilation

**Status:** first Core contract and preview compiler implemented
**Date:** 2026-09-25
**Scope:** Kehrnel/Data Lab Core; no strategy activation or generated code

## 1. Product decision

Kehrnel must not assume that the customer's source is already a FHIR resource
or an openEHR composition. It must start with the estate the customer actually
has: EHR, LIS, PACS, MPI, billing, patient portal, devices, files, streams and
custom databases. It must also avoid implying that a healthcare organization
has to replace those systems.

The default architecture is additive:

> Existing applications remain authoritative. Kehrnel selectively materializes
> and continuously maintains the semantic, operational context required for
> cross-system workflows, efficient multi-record queries and governed AI.

MongoDB can become the system of record for a new or migrated domain, but this
is an explicit source-level choice. It is not the default hidden behind a
mapping wizard.

## 2. Do not mix sources and target strategies

The catalog has two primary axes with a semantic bridge between them.

### Source categories — the customer's operational landscape

The primary discovery catalog is category-based: EHR, MPI, scheduling, LIS,
PACS, pharmacy, revenue cycle, EDC, CTMS, pharmacovigilance, manufacturing and
other business capabilities. Categories describe product-neutral functional
interactions and semantic domains. They are the choices shown first in the
Builder.

Products do not define the architecture. OpenMRS, Bahmni and future commercial
systems are reference implementations used to learn real tables, documents,
identifiers, events and integration constraints for a category.

### Source profiles — how concrete implementations behave

A Source Profile describes a concrete origin and its change behavior:

- product or exchange profile and version;
- technology and structures such as databases, tables, collections, APIs,
  messages and files;
- user/application interactions that create meaningful data changes;
- supported extraction modes such as CDC, event stream, API, polling, batch
  or snapshot;
- identifiers, polymorphism and semantic bindings;
- available or proposed mappings to target models.

`app.openmrs.mysql` and `app.bahmni.reference` are examples of reference Source
Profiles behind one or more categories. `exchange.fhir-r4-json` is a Source Profile only when the origin
actually emits FHIR R4 JSON. FHIR itself is not assumed to be the customer's
source model.

### Target strategies — what MongoDB should provide

A Target Strategy solves a persistence and consumption problem on MongoDB:

- authoritative document shape and version behavior;
- transformation and mapping boundary;
- queries and canonical protocol behavior;
- indexes, Search, Vector Search and graph access;
- operational APIs and agent tools;
- maintenance, validation, evidence and lifecycle.

`fhir.resource_store`, `openehr.rps_dual`, `openehr.rps_dual_ibm` and
`cdisc.sdr` are target strategies. They make a FHIR store, openEHR CDR or CDISC
repository efficient; they do not assert that the source application stores
data in that standard.

### Semantic bridge — how meaning survives the transformation

Semantic bindings, identity resolution, terminology/value-set references,
mapping releases and lineage connect source profiles to target strategies.
Context Studio can propose these bindings from an unfamiliar database. Core
owns their typed representation, deterministic validation and approved runtime
release.

This produces a clear flow:

```text
source categories selected from business needs
        ↓  optionally match a known product or discover a customer system
source profiles and observed interactions
        ↓  semantics + identity + governed mappings
target strategies on MongoDB
        ↓  compose
data architectures and workflows
        ↓
applications and agent experiences
```

## 3. Catalog tree and provenance

The user should see an industry-specific tree, but the tree is a navigational
view over typed artifacts rather than the source-code hierarchy. One artifact
can appear in more than one industry without being duplicated.

```text
Healthcare
├── Source systems
│   ├── Apps
│   │   ├── EHR / OpenMRS
│   │   └── Hospital suite / Bahmni
│   └── Standards-native exchanges
│       ├── FHIR R4 JSON
│       └── openEHR JSON + OPT
├── Target strategies
│   └── Data standards
│       ├── FHIR Resource Store
│       └── openEHR RPS Dual
├── Semantic assets
│   ├── Terminology / SNOMED CT
│   └── Context and value-set packs
├── Data architectures
└── Applications

Pharmaceutical
├── Source systems
├── Target strategies
│   └── Data standards / CDISC SDR
├── Semantic assets
├── Data architectures
└── Applications
```

The backend records artifact role independently of placement:

- `source_profile`
- `target_strategy`
- `semantic_asset`
- `data_architecture`
- `application`

It also records lifecycle ownership independently:

- `mongodb_curated`
- `community`
- `tenant_generated`
- `tenant_extended` with an immutable `extendsRef`

This avoids folders such as `healthcare/apps/fhir` incorrectly making an
application and a target standard look like the same kind of component.

## 4. Integration posture: augmentation first

Each source instance declares one posture:

| Posture | Meaning |
| --- | --- |
| `federated_access` | Keep data at the source and access it through a governed connector/tool. |
| `selective_materialization` | Materialize only the records and context needed by declared workloads. This is the default. |
| `operational_replica` | Maintain a broader, continuously updated operational replica in MongoDB. |
| `mongodb_system_of_record` | MongoDB is authoritative for this selected domain or new workflow. |

A hospital can choose different postures for different domains. For example,
PACS binaries may remain external while study metadata is selectively
materialized; an EHR remains authoritative while patient/encounter context is
replicated; a new agent decision-trace domain can be born directly in MongoDB.

The sales narrative is therefore not database replacement. It is that meaning
and context become operational workloads over rich, hierarchical,
polymorphic, evolving data—the area where MongoDB is strongest—while source
authority and lineage remain explicit.

## 5. Implemented first slice

The following contracts are published as JSON Schema:

- `source-profile/v1`
- `source-profile-catalog/v1`
- `legacy-source-blueprint/internal` (retired prototype)
- `strategy-compilation-request/v1`
- `strategy-candidate/v1`

The category catalog establishes a first landscape of 31 product-neutral
categories: 15 Healthcare categories and 16 Pharmaceutical categories. The
reference-profile catalog initially contains:

- FHIR R4 JSON exchange;
- openEHR composition + OPT exchange;
- OpenMRS on MySQL;
- Bahmni reference distribution spanning clinical, laboratory, billing and
  imaging boundaries.

The first two have executable direct routes to the matching supported target
strategies. OpenMRS and Bahmni production routes are deliberately marked
`proposed`: the compiler exposes the intended route but blocks activation
until connector, mapping coverage and evidence exist. This prevents a polished
catalog card from being mistaken for a working integration.

One deliberately narrower artifact is executable in simulation:
`mapping.openmrs-to-openehr.lab-observation@1.0.0`. It covers one normalized
numeric laboratory observation, pins the relevant OpenMRS structures and
joins, and produces an openEHR `sample_laboratory_v0.4` composition with
per-field lineage. Equivalent batch input and a CDC after-image produce the
same document and idempotency key. Its `simulated` maturity is visible in the
contract and does not make the complete OpenMRS route production-ready.
When a Blueprint source selects `laboratory-result-observed`, the Strategy
Candidate carries the exact simulation Mapping Artifact reference and an
informational `MAPPING_SIMULATION_AVAILABLE` issue alongside the blocking
production-route issue. An unknown interaction is rejected rather than being
silently ignored.

Unknown customer repositories can be supplied as immutable discovered Source
Profiles. The compiler verifies their digests, semantic bindings and requested
ingestion modes, then emits a deterministic Strategy Candidate. Inferred
semantics and generated mappings always require approval. Multiple sources
carrying person identity are blocked until identity resolution is declared.

Available API contracts:

```text
GET  /v1/source-profiles
GET  /v1/source-categories
GET  /v1/mappings
POST /v1/mappings/{mapping_id}/simulate
POST /v1/strategy-candidates/compile
POST /v1/architecture-proposals/compile
GET  /v1/strategy-portfolio
```

Compilation is read-only. It neither installs generated code nor activates an
environment.

The architecture-proposal endpoint adds the first consumption-aware access
plan. A workload envelope can declare record, subject, population, corpus or
stream scope; query features; consistency; rate; result size; latency; and
freshness. Core then selects the lowest justified access mechanisms, reuses
them across compatible consumers, records rejected alternatives, and reports
an estimated projection budget. Every non-authoritative structure has a
rebuild source and retirement condition. This proposal is deterministic Core
output: a future Modeler Agent may propose typed input changes, but cannot
silently alter the compiled result.

## 6. What remains before production

This slice establishes the language and compiler boundary. It does not yet:

- connect to a customer's database or inspect credentials;
- run CDC, bulk ingestion or federated access;
- execute complete OpenMRS/Bahmni mappings or connect to their live databases;
- generate collections, indexes, shard keys, Search/Vector Search definitions
  or APIs;
- benchmark alternatives or prove horizontal scale;
- turn estimated access-plan costs into measured benchmark evidence;
- promote, activate, migrate or roll back a compiled candidate;
- provide the full source → context → consumption Builder experience.

The next vertical implementation should expand the laboratory proof into one
complete route: OpenMRS or Bahmni events → governed mapping →
`openehr.rps_dual` → AQL and a read-only agent tool. It must add connector
replay, join/reconciliation state, delete handling, drift detection and target
validation before the route can become available.

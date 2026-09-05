# Kehrnel strategy for a document-first CDISC Study Data Repository

**Status:** implemented preview; customer-discovery ready, not a regulatory-compliance claim
**Date:** 2 September 2026
**Proposed strategy ID:** `cdisc.sdr`
**Architectural category:** Study Data Repository (SDR)

## 1. Executive decision

Kehrnel implements CDISC as a **Study Data Repository strategy pack**, not as a clinical-record API and not as a metadata repository.

The strategy should preserve and govern versioned research-study snapshots, make them operationally queryable, validate them against pinned standards and rules, retain source-to-analysis lineage, generate coherent synthetic studies, and recreate regulatory exchange artifacts. It should not replace EDC, LIMS, pathology, statistical programming, or submission assembly systems.

The recommended design is a three-layer representation:

1. **Original artifact** - immutable bytes or an immutable object reference, checksum, media type, and acquisition provenance.
2. **Canonical study snapshot** - typed dataset and record documents that preserve CDISC names, values, ordering, keys, metadata, and lineage.
3. **Query projections** - deterministic, rebuildable facets and materialized views for clinical, nonclinical, analysis, and product-evidence workloads.

This adopts the strongest idea from Kehrnel's openEHR RPS strategy - one governed source representation plus workload-specific projections and deterministic query compilation - without copying openEHR's reversed-path encoding into a data model where it does not fit.

The implemented preview supports **SEND, SDTM, ADaM, and TIG** profiles over one repository kernel. Clinical SDTM and preclinical SEND examples prove the common intake, validation, publication, query, and export path. A production deployment must still pin its licensed standards, terminology, validation engine, and representative acceptance datasets.

Every persisted repository document carries `modelSchemaVersion`. This version describes the Kehrnel storage shape and evolves independently from the CDISC standard, implementation-guide, generator, and strategy-pack versions. Operational queries select the current model version; a shape change requires an explicit, idempotent migration.

## 2. What Kehrnel already provides

Kehrnel is a strategy runtime, not a domain-specific database. Its current execution path is:

```text
manifest discovery
  -> environment activation
  -> defaults + config validation + secure binding resolution
  -> adapter construction
  -> capability or named-op dispatch
  -> strategy transform/ingest/query/generation logic
  -> MongoDB and Atlas Search
```

The following existing capabilities are reused.

| Kehrnel capability | CDISC use |
|---|---|
| Strategy manifests and JSON Schema | Declare `cdisc.sdr`, profiles, dependencies, configuration, operations, and UI metadata. |
| Environment activation | Pin a tenant/environment to a strategy version, profile configuration, bindings, and manifest digest. |
| `bindings_ref` resolution | Keep database and object-store credentials outside requests and activation records. |
| `plan` / `apply` | Create collections, B-tree indexes, optional search indexes, and supporting infrastructure. |
| `transform` / `ingest` hooks | Host canonicalization and persistence for small synchronous payloads. |
| Named strategy operations | Host artifact ingestion, metadata synchronization, validation, publication, export, and maintenance workflows. |
| `compile_query` / `query` | Compile a CDISC Study Query IR into an explainable MongoDB aggregation plan and execute it. |
| Explain metadata | Record strategy, activation, manifest, config, engine, scope, warnings, and query-plan decisions. |
| Synthetic job API | Provide the initial asynchronous facade for synthetic CDISC generation. |
| Mongo storage, index, and Atlas Search adapters | Implement the first physical repository. |
| Strategy-pack specification | Describe logical models, physical stores, encoding profiles, indexes, and workloads. |
| ContextObjects and Con2L | Add an optional semantic discovery layer above CDISC records and query IR. |
| Shared mapping engine | Reuse expression, tracing, macro, and mapping concepts where applicable. |

Kehrnel therefore does not need a separate service framework for CDISC. It needs a CDISC domain library, a thin strategy pack, and a small set of general kernel extensions described below.

## 3. Lessons from openEHR RPS

### 3.1 What is genuinely reusable

The RPS paper and `openehr.rps_dual` implementation establish six useful patterns:

1. Keep a reconstruction-capable primary representation and derive search projections from it.
2. Select a query plan by workload scope instead of forcing every query through one physical access path.
3. Compile a domain query into a deterministic MongoDB plan and expose the decision in `explain`.
4. Derive indexes and projections from versioned model metadata and mappings.
5. Make derived search data disposable and rebuildable.
6. Use the same activated strategy for ingestion, transformation, querying, synthetic generation, and operational maintenance.

The strategy also demonstrates useful implementation mechanics: configuration normalization, dictionary caching, field aliases, mapping-driven search-index creation, parameter binding, pagination, golden query tests, plan/apply, reversible transformation, and batch generation.

### 3.2 What must not be copied

OpenEHR compositions are deeply nested, path-addressed documents. Reversed paths, archetype dictionaries, semi-flattened node arrays, AQL `CONTAINS`, and patient-versus-cross-patient routing are solutions to that shape.

CDISC submission datasets are primarily metadata-governed tables. A CDISC record already has a domain, named variables, dataset keys, subject or experimental-unit identifiers, sequence variables, and timing variables. Encoding every variable as a reversed path would add complexity, enlarge indexes, and make ordinary compound indexes harder to use.

For CDISC:

- use direct named fields in `data` and stable typed fields in `facets`;
- use compound B-tree indexes for high-value dataset, subject, group, timing, and finding access paths;
- use Atlas Search selectively for flexible text, terminology, product-evidence discovery, and high-dimensional ad hoc filters;
- use path indexing only for genuinely hierarchical artifacts such as Define-XML, ODM, and package manifests;
- define a CDISC query IR instead of misusing AQL, because CDISC has no standard general-purpose query language equivalent to AQL.

### 3.3 A fidelity issue the CDISC design must make explicit

The RPS paper describes canonical compositions stored as whole documents. The current Kehrnel implementation ingests a reconstruction-oriented semi-flattened base document and provides a best-effort unflattener; normal ingest does not separately retain the original input artifact.

That distinction matters more for regulatory study data. A CDISC SDR must define three different guarantees:

| Guarantee | Meaning |
|---|---|
| Artifact replay | Return the exact original bytes by checksum. |
| Semantic round trip | Export an equivalent dataset with the same variables, types, values, order, keys, labels, and relevant metadata. |
| Regulatory conformance | The exported package passes the pinned applicable rule sets; this is not the same as byte equality. |

Original XPT or XML bytes must be retained if byte-identical replay is required. A regenerated XPT file may be semantically equivalent without being byte-identical.

## 4. CDISC repository scope

### 4.1 Repository anchor and identities

The repository anchor is the **study snapshot**, not the patient.

Identity is hierarchical:

```text
tenant
  -> study
    -> submission or evidence package
      -> snapshot/version
        -> dataset
          -> record
```

A subject is typed and optional. Supported types include human subject, animal subject, pooled animals, experimental unit, specimen, product, batch, and an assay with no subject at all.

Every published snapshot is immutable. Corrections create a new snapshot and lineage edge; they do not silently mutate a regulatory evidence state.

### 4.2 Profiles on one kernel

| Profile | Navigation and principal entities | Representative workloads |
|---|---|---|
| SEND | Study, test article, treatment group, animal, specimen, organ, finding | Dose response, lesion incidence, target-organ review, cross-study toxicology, treatment consistency. |
| SDTM | Study, subject, arm, epoch, visit, intervention, event, specimen | Subject timeline, cohorts, exposure, safety review, deviations, cross-domain review. |
| ADaM | Study, population, analysis parameter, analysis record, method | Endpoint reproducibility, population derivation, analysis review, traceability to SDTM/source. |
| TIG | Product, batch, constituent, test article, study, evidence item | Product description, nonclinical evidence, individual health, population health, connected evidence packages. |

TIG should be an overlay that pins and extends CDASH, SDTM, ADaM, Define-XML, terminology, and TIG conformance assets. It should not fork the repository kernel.

### 4.3 Product boundaries

The strategy will not claim to:

- replace collection, laboratory, pathology, statistical, or submission-authoring systems;
- infer a compliant CDISC mapping from arbitrary source data without governed mappings and review;
- make a dataset compliant merely because it was ingested;
- provide a universal raw-science model for images, omics files, instrument traces, or every assay format;
- run heavy biostatistical analysis inside the operational repository;
- expose a standard CDISC query language where none exists.

## 5. Target architecture

### 5.1 Component boundary

The implementation should separate shared CDISC logic from the strategy facade.

```text
src/kehrnel/engine/domains/cdisc/
  artifacts/       # media detection, checksums, object references
  formats/         # Dataset-JSON, XPT, Define-XML, ODM readers/writers
  metadata/        # standards packages, terminology, Define metadata
  model/           # canonical study, dataset, record, lineage types
  projection/      # profile-driven facets and materialized views
  query/           # Study Query IR, validator, planner, Mongo compiler
  validation/      # engine adapter contract and finding normalization
  synthetic/       # recipe model, constraint graph, generators
  export/          # Dataset-JSON, XPT, Define-XML/package exporters

src/kehrnel/engine/strategies/cdisc/sdr/
  manifest.json
  spec.json
  schema.json
  defaults.json
  profiles/
    send/
    sdtm/
    adam/
    tig/
  strategy.py      # thin StrategyPlugin facade
```

The domain library must not depend on MongoDB. Storage, object storage, validation engines, and queues enter through adapters. The `cdisc.sdr` pack selects the MongoDB implementation in version one.

### 5.2 Processing lifecycle

```text
RECEIVED
  -> artifact checksum, malware/content policy, manifest registration
PARSED
  -> structural parsing and normalized metadata
CANONICALIZED
  -> immutable dataset/record snapshot in staging
VALIDATED
  -> structural, metadata, terminology, conformance, sponsor findings
PUBLISHED or QUARANTINED
  -> atomic visibility switch; query projections become visible only on publish
SUPERSEDED
  -> retained with lineage to the replacing snapshot
```

Parsing and validation failures must never create partially published datasets. Large imports should write to a staging snapshot and commit visibility atomically after counts, hashes, and required gates agree.

### 5.3 Physical collections

| Collection | Purpose | Mutability |
|---|---|---|
| `cdisc_studies` | Stable study identity and portfolio metadata. | Versioned updates. |
| `cdisc_snapshots` | Submission/evidence package identity and immutable snapshot state. | Append-only snapshots. |
| `cdisc_datasets` | Dataset metadata, variable metadata, keys, order, counts, hashes, and standard pins. | Immutable per snapshot. |
| `cdisc_records` | One lossless logical CDISC row per document, plus deterministic facets and lineage. | Immutable per snapshot. |
| `cdisc_entities` | Normalized study entities and explicit cross-dataset references. | Rebuildable from snapshot. |
| `cdisc_materializations` | Subject timelines, nonclinical findings, analysis traceability, and product-evidence views. | Rebuildable from snapshot. |
| `cdisc_artifacts` | Artifact identity, checksum, media type, size, storage URI, and provenance. | Append-only. |
| `cdisc_standards` | Pinned standards, implementation guides, terminology, and rules manifests. | Append-only versions. |
| `cdisc_validation_runs` | Engine/rule versions, inputs, configuration, status, and summary. | Append-only. |
| `cdisc_validation_findings` | Normalized findings linked to dataset, record, variable, and rule. | Append-only per run. |
| `cdisc_validation_waivers` | Scoped, expiring decisions retained with validation evidence. | Append-only. |
| `cdisc_transformations` | Mapping/export/derivation executions and lineage edges. | Append-only. |

Do not split every CDISC domain into its own collection in the first implementation. A common record collection gives one governance and query contract. Add materialized views or dedicated collections only after measured workloads justify them.

### 5.4 Canonical record contract

```json
{
  "_id": "sha256:<deterministic-record-identity>",
  "tenantId": "sponsor-a",
  "studyId": "NC-TOX-001",
  "packageId": "package-2026-08",
  "snapshotId": "snapshot-v3",
  "datasetId": "NC-TOX-001-MI-v3",
  "standard": {
    "family": "SEND",
    "modelVersion": "pinned-by-package",
    "implementationGuide": "SENDIG",
    "implementationGuideVersion": "pinned-by-package",
    "terminologyPackage": "pinned-by-package"
  },
  "domain": "MI",
  "row": {
    "ordinal": 887,
    "key": {
      "STUDYID": "NC-TOX-001",
      "USUBJID": "NC-TOX-001-0042",
      "MISEQ": 7
    }
  },
  "entityRefs": [
    {"type": "animalSubject", "id": "NC-TOX-001-0042"},
    {"type": "treatmentGroup", "id": "HIGH-DOSE"},
    {"type": "testArticle", "id": "PRODUCT-A"},
    {"type": "specimen", "id": "LIVER-0042"}
  ],
  "facets": {
    "subjectType": "animal",
    "species": "RAT",
    "sex": "M",
    "doseLevel": {"value": 100, "unit": "mg/kg/day"},
    "organ": {"code": null, "display": "LIVER"},
    "finding": {"code": null, "display": "HEPATOCELLULAR HYPERTROPHY"},
    "severity": "MINIMAL",
    "studyDay": 29
  },
  "data": {
    "STUDYID": "NC-TOX-001",
    "DOMAIN": "MI",
    "USUBJID": "NC-TOX-001-0042",
    "MISEQ": 7,
    "MISPEC": "LIVER",
    "MISTRESC": "HEPATOCELLULAR HYPERTROPHY",
    "MISEV": "MINIMAL"
  },
  "lineage": {
    "sourceArtifactId": "artifact-123",
    "sourceDataset": "MI",
    "sourceRow": 887,
    "mappingId": null,
    "mappingVersion": null,
    "recordHash": "sha256:..."
  }
}
```

Rules for this contract:

- `data` keeps original CDISC variable names and logical values. It is not renamed into a universal ontology.
- `facets` is deterministic, typed, versioned, and rebuildable from `data` plus pinned metadata.
- variable metadata, column order, labels, lengths, display formats, origins, and codelist references live on the dataset document rather than being repeated per row;
- the record key comes from Define-XML or pinned standard metadata, with row ordinal only as a controlled fallback;
- missing, blank, null, unknown, and not-applicable states must not be silently collapsed;
- raw lexical XML representations are retained when they are needed for exact semantics;
- records include `tenantId` and `snapshotId` in every access path so isolation and publication state can be enforced.

### 5.5 Standards package contract

Every snapshot points to one immutable standards-package manifest containing:

- standard family and model version;
- implementation guide and version;
- controlled terminology packages and dates;
- Define-XML, Dataset-JSON, and other exchange versions;
- applicable regulatory authority and supported-version context;
- validation engine and rule-package versions;
- sponsor overlays;
- source URI, license class, redistribution policy, checksum, and acquisition time for each asset.

The latest CDISC version and the regulator-supported version are different dimensions and must never be represented by one `latest` flag.

## 6. Ingestion and transformation

### 6.1 Artifact adapters

Implement in this order:

1. Dataset-JSON v1.1 reader and writer.
2. Define-XML reader sufficient to bind dataset/variable/key/codelist/origin metadata.
3. SAS XPT reader and writer.
4. ODM reader for collection/source lineage where a use case requires it.
5. Standards-metadata synchronizer for CDISC Library and licensed/offline packages.

Input should be an artifact reference, not arbitrary server paths in a public API:

```json
{
  "artifactRef": {
    "provider": "object_store",
    "uri": "tenant://incoming/package-123/dm.json",
    "sha256": "...",
    "mediaType": "application/vnd.cdisc.dataset+json"
  },
  "profile": "sdtm",
  "standardsPackageId": "pkg-sdtm-pilot-1",
  "mode": "stage"
}
```

CLI local-path input may remain an explicitly enabled development feature, following Kehrnel's existing local-file security controls.

### 6.2 Ingestion stages

Each adapter produces the same staged contract:

1. Detect and register the artifact; verify checksum and media type.
2. Parse headers and structural metadata without publishing records.
3. Resolve explicit Define metadata, embedded metadata, and pinned standard metadata with documented precedence.
4. Type values without changing the preserved logical `data` contract.
5. Calculate dataset keys, record identities, row hashes, counts, and dataset content hash.
6. Generate profile facets and entity references.
7. Persist records in batches to the staging snapshot using idempotent upserts.
8. Run structural and configured validation gates.
9. Reconcile source count, stored count, duplicate count, rejected count, and aggregate hash.
10. Publish atomically or quarantine with findings.

Every stage creates an execution record with input/output hashes, code version, configuration digest, counts, timings, warnings, and actor/service identity.

### 6.3 Transformations and lineage

Source-to-CDISC mappings and SDTM-to-ADaM derivations are explicit versioned assets. A transformation is a DAG:

```text
source artifact/record
  -> mapping execution
    -> CDISC record
      -> derivation execution
        -> analysis record
```

Lineage edges must support dataset-level and record-level references. ADaM needs links to source SDTM records and method metadata; a prose derivation description alone is not sufficient for computational traceability.

Kehrnel's current mapping tracing utilities can contribute expression evaluation and traces, but CDISC mappings require a richer typed contract for keys, terminology, timing, units, multi-row derivations, and reviewer approvals.

## 7. Validation

Validation is a first-class data workflow, not the strategy's `validate_config` method.

Define a storage-neutral adapter:

```python
class CdiscValidationEngine:
    async def capabilities(self) -> dict: ...
    async def validate(self, snapshot_ref: dict, rule_package: dict, options: dict) -> dict: ...
```

Normalize all engine results into one finding contract:

```json
{
  "runId": "validation-run-42",
  "ruleId": "rule-package:rule-id",
  "severity": "error",
  "category": "controlled-terminology",
  "message": "...",
  "location": {
    "studyId": "...",
    "datasetId": "...",
    "recordId": "...",
    "row": 887,
    "variable": "MISEV"
  },
  "engine": {"name": "...", "version": "..."},
  "rules": {"packageId": "...", "version": "..."},
  "waiver": null
}
```

Validation layers are:

1. artifact and parser integrity;
2. dataset structure and metadata consistency;
3. data types, keys, uniqueness, order, and required variables;
4. controlled terminology;
5. cross-record and cross-domain rules;
6. profile rules for SEND, SDTM, ADaM, or TIG;
7. regulator-specific and sponsor-specific overlays.

CDISC CORE should be an adapter where its supported rules and formats apply, not a hard dependency or a claim of complete regulatory compliance. Validation runs must remain reproducible even as engines and rules evolve.

## 8. Query strategy

### 8.1 CDISC Study Query IR

Expose a versioned JSON intermediate representation through Kehrnel's existing compile/query capabilities.

```json
{
  "version": "cdisc-query/v1",
  "scope": {
    "studies": ["NC-TOX-001"],
    "snapshots": "published"
  },
  "from": {
    "profile": "send",
    "domains": ["MI"]
  },
  "where": {
    "and": [
      {"path": "facets.organ.display", "op": "eq", "value": "LIVER"},
      {"path": "facets.severity", "op": "in", "value": ["MINIMAL", "MILD"]},
      {"path": "facets.studyDay", "op": "gte", "value": 28}
    ]
  },
  "select": [
    "studyId",
    "data.USUBJID",
    "facets.doseLevel",
    "facets.finding",
    "facets.severity"
  ],
  "orderBy": [{"path": "facets.studyDay", "direction": "asc"}],
  "page": {"limit": 100, "token": null}
}
```

The compiler must allow only catalogued paths and operators, enforce tenant/snapshot predicates, bind parameters safely, select indexes and collections, and return an explainable plan. It must not accept arbitrary MongoDB operators from clients.

### 8.2 Planner modes

| Mode | Primary access path | Use |
|---|---|---|
| `dataset_key` | B-tree on tenant, snapshot, dataset, and record key | Exact record retrieval and dataset reconstruction. |
| `study_subject` | B-tree on tenant, study, subject/entity, domain, and time/sequence | Timelines and within-study review. |
| `portfolio_facets` | Compound/wildcard facet indexes; Atlas Search only when justified | Cross-study clinical or nonclinical discovery. |
| `traceability` | Indexed lineage edges and bounded lookups | ADaM-to-SDTM/source navigation. |
| `product_evidence` | Materialized evidence projection plus optional text/vector search | TIG evidence exploration. |

Cross-domain correlation should use typed keys and bounded relationships. Frequently used multi-domain reviews should become profile-owned materialized views rather than unbounded `$lookup` pipelines over the full portfolio.

Heavy statistical computation and large analytical scans should be exported to an analytical engine. The SDR remains the governed operational source and reproducible extraction point.

### 8.3 Query catalogue

The query catalogue is an engineering input, not demo decoration. Each entry contains:

- user role and question;
- query IR and parameters;
- expected columns and result semantics;
- expected dataset/profile coverage;
- scope and selectivity class;
- required materialization/index;
- golden fixture and expected result;
- latency and scale target.

Schema and index decisions should be accepted only when they serve catalogue queries or measured operational requirements.

## 9. Synthetic study generation

CDISC synthetic generation must build a coherent study, not independently randomize rows.

The generation graph is:

```text
protocol/study design
  -> arms or treatment groups
  -> subjects/animals/experimental units
  -> visits, epochs, study days, and scheduled events
  -> exposure and interventions
  -> specimens and observations/findings
  -> cross-domain relationships
  -> analysis populations, parameters, and derivations
  -> controlled validation anomalies (optional)
```

A recipe pins:

- seed and generator version;
- profile and standards package;
- study design and population sizes;
- terminology distributions;
- temporal and biological constraints;
- effect/dose-response models;
- missingness and dropout models;
- requested domains;
- anomaly scenarios and expected validation findings;
- scale factor and target storage profile.

Generation must be deterministic for the same recipe, seed, generator version, and standards package. Every generated artifact is watermarked as synthetic and includes its recipe digest.

The SEND `safety-signal` scenario is a solution-testing contract rather than a random-data preset. It requires DM, TX, MI, and LB and creates a known, reproducible evidence path across five dose groups: treatment assignment, a treated-only thymus finding with dose-related incidence/severity, longitudinal lymphocyte measurements, and a background finding for contrast. The response declares `expectedSignals`, including the intended domains and expected control/treated pattern, so a consuming application can test its governed queries and visualizations without duplicating toxicology rules.

Recommended demonstration paths:

1. The official SDTM pilot, fetched from its authoritative repository at a pinned revision, checksum-verified, and used according to its terms.
2. The public PhUSE SEND study, fetched from its authoritative repository at a pinned revision, checksum-verified, and used according to its license.
3. A synthetic repeat-dose SEND study with TS, TX, DM, EX, BW, CL, LB, OM, MA, MI, and optional TF; control/low/mid/high groups; both sexes; scheduled sacrifices; and internally consistent dose-response findings.
4. A small synthetic TIG overlay linking product and batch data to a test article, nonclinical study, and optional clinical/population evidence.

Customer or licensed examples should be used for verification when available, but must not become silently redistributable fixtures.

The checked-in `examples/catalog.json` is the delivery contract for the first two journeys. It stores attribution, terms/license links, immutable source revisions, and SHA-256 digests—not the datasets. `cdisc_list_examples` exposes this catalog and `cdisc_ingest_example` performs the same artifact retention, XPT/Define ingestion, validation, publication, and projection workflow used for customer data. It accepts only entries in the curated catalog and therefore does not become an arbitrary URL fetcher.

## 10. Export and round-trip verification

Implement export as an asynchronous, versioned operation over an immutable snapshot.

Initial formats:

1. Dataset-JSON;
2. XPT;
3. Define-XML;
4. a package manifest linking datasets, metadata, validation evidence, and checksums.

An export report compares:

- source and output dataset/variable order;
- logical data types and format-specific representations;
- record counts and keys;
- value-level hashes after documented canonical normalization;
- labels, lengths, codelists, origins, and metadata references;
- validation results under the same pinned rules;
- unresolved or format-induced differences.

The API must state whether it returned original bytes, a semantic regeneration, or a newly conformed package.

## 11. Strategy operations and API surface

Use universal runtime endpoints first. Add CDISC-specific HTTP routes only after the operation contracts stabilize.

| Capability/op | Purpose |
|---|---|
| `plan` / `apply` | Provision collections and indexes. |
| `cdisc_register_standards` | Register or synchronize a standards-package manifest. |
| `cdisc_store_artifact` | Retain bounded input bytes by SHA-256 through the activated artifact adapter. |
| `cdisc_replay_artifact` | Return retained bytes after digest and size verification. |
| `cdisc_list_examples` | List curated, revision-pinned clinical and preclinical starter datasets with attribution and terms. |
| `cdisc_ingest_example` | Fetch, checksum-verify, retain, ingest, validate, and optionally publish one curated example. |
| `cdisc_inspect_dataset_json` | Validate Dataset-JSON structure without persistence. |
| `cdisc_ingest_dataset_json` | Stage, canonicalize, reconcile, and optionally publish Dataset-JSON. |
| `cdisc_validate_snapshot` | Execute one or more pinned validation adapters. |
| `cdisc_publish_snapshot` | Atomically expose an accepted staged snapshot. |
| `cdisc_rebuild_projections` | Recreate facets/entities/materialized views from canonical records. |
| `compile_query` / `query` | Compile and execute CDISC Study Query IR. |
| `cdisc_export_dataset_json` | Generate Dataset-JSON, verify semantic equivalence, retain it optionally, and write an execution record. |
| `cdisc_export_package` | Generate a deterministic multi-dataset exchange package and manifest. |
| `cdisc_export_solution_evidence` | Emit a checksum-protected, deployment-neutral evidence handoff for a self-contained business solution. |
| `cdisc_generate_synthetic_study` | Generate a deterministic, linked study from a governed recipe and optionally ingest it. |
| `cdisc_run_analysis` | Execute bounded grouped metrics over governed records. |
| `cdisc_semantic_search` | Run lexical, semantic, or hybrid retrieval inside the published study scope. |
| `cdisc_ask_assistant` | Answer a bounded set of read-only study questions with evidence citations. |
| `cdisc_get_lineage` | Traverse source, tabulation, and analysis lineage. |

Long-running ingestion, validation, projection rebuild, export, generation, and a future benchmark harness should use the generalized durable job API. Recovery, lease, cancellation, and resumability behavior still require deployment-level acceptance testing before regulated use.

## 12. Required Kehrnel kernel work

### 12.1 Must be addressed before production

| Current limitation | Required change |
|---|---|
| `BundleStore` validates only `slim_search_definition` and disk seeding is hard-coded to openEHR. | Generalize asset bundles by `kind`, domain schema, pack-relative discovery, digest, license metadata, and immutable version. |
| Checksum-addressed artifacts, bounded inline transfer, and direct object-store upload/download are implemented. | Exercise direct transfer with production object-store limits, malware scanning, retention policy, and tenant credentials. |
| `TransformResult` assumes only `base` and `search`. | Add backward-compatible named outputs or make multi-artifact workflows first-class named ops. |
| Synthetic jobs are in-process; cancellation and execution do not recover across process restarts. | Generalize to durable jobs with leases, heartbeats, retry policy, cancellation, resumable phases, and worker identity. |
| Core `validate` dispatch means configuration validation. | Preserve `validate_config`, but add an unambiguous data-validation capability/job contract. |
| Tenant isolation partly relies on environment/API-key scoping. | Enforce tenant and environment predicates in storage/query adapters and test cross-tenant denial. |
| Operational audit is domain-specific, especially in openEHR APIs. | Add strategy-operation audit events with actor, input/output refs, activation, code/config digests, and outcome. |
| Filesystem, S3-compatible, and Azure Blob artifact adapters are available. | Complete deployment wiring and acceptance tests for the selected validation engine and durable queue/worker implementation. |

### 12.2 Correctness requirements for the CDISC pack

The new pack itself must:

- use idempotent upserts keyed by deterministic snapshot/record identity, not blind inserts;
- fail query execution when the database fails, rather than returning an empty result with only an embedded error;
- use staging and an atomic publication marker;
- reconcile counts and hashes before publish;
- keep standards, terminology, mappings, rules, and generator versions in every execution record;
- never permit a query compiler to omit tenant and published-snapshot constraints;
- make all projections disposable and reproducible;
- cap and paginate portfolio queries;
- redact sensitive values from logs, explain payloads, and validation summaries.

### 12.3 Useful but not blocking initially

- ContextObject definitions for study, dataset, subject, treatment group, finding, analysis result, product, and evidence item;
- Con2L negotiation that produces CDISC Study Query IR;
- vector search over governed metadata and narrative evidence;
- non-MongoDB record-store adapters;
- streaming change events for newly published snapshots.

## 13. Security, governance, and regulated operation

The production design requires:

- environment- and tenant-scoped credentials through `bindings_ref`;
- encryption in transit and at rest, explicit key ownership, and artifact-store encryption;
- least-privilege roles for ingest, validate, publish, query, export, standards administration, and waiver approval;
- immutable audit events and retained execution evidence;
- separation of duties between data preparation and snapshot approval;
- retention, legal hold, supersession, and defensible deletion policies;
- integrity checks at artifact, dataset, and record levels;
- controlled software/configuration changes and reproducible deployments;
- backup, restore, and disaster-recovery tests;
- de-identification and disclosure controls for clinical data;
- license and redistribution enforcement for standards packages and sample data.

Kehrnel can provide technical controls and evidence, but the accelerator must not claim GxP validation, 21 CFR Part 11 compliance, or regulatory acceptance without the customer's validated process and deployment controls.

## 14. Original delivery plan and remaining acceptance gates

This roadmap is retained to show how the preview was built. Implemented status
is recorded in section 18; any gate without deployment evidence remains a
release gate rather than an implied capability gap in the preview.

### Phase 0 - evidence and contracts

**Work**

- Obtain and classify standards, terminology, rule packages, samples, and licenses.
- Interview SEND, SDTM, ADaM, validation, and submission users.
- Build the versioned query catalogue and sample expected results.
- Freeze canonical dataset/record, artifact, standards-package, finding, lineage, and query-IR contracts.

**Gate**

- Legal/redistribution matrix approved.
- At least 20 prioritized queries across nonclinical, clinical, analysis, and governance roles.
- Contract examples reviewed by domain practitioners.

### Phase 1 - JSON-native vertical slice

**Work**

- Scaffold `engine/domains/cdisc` and `cdisc.sdr`.
- Register one offline standards package.
- Ingest Dataset-JSON into an immutable SDTM snapshot.
- Generate deterministic facets for DM, AE, EX, LB, and VS.
- Compile and run study/subject/portfolio query IR.
- Export Dataset-JSON and produce a semantic equivalence report.

**Gate**

- Repeat ingest is idempotent.
- Source/stored/export counts and value hashes agree under documented normalization.
- Golden query suite passes.
- A failed import cannot expose partial records.

### Phase 2 - metadata, XPT, and validation

**Work**

- Add Define-XML binding, XPT read/write, terminology packages, and artifact storage.
- Add validation adapter and normalized findings.
- Retain staged publication and complete generalized durable-job recovery for every long-running workflow.
- Keep the official SDTM pilot reproducible through the curated, revision-pinned external catalog.

**Gate**

- Original artifacts replay by checksum.
- XPT semantic round trip is documented and tested.
- Validation run is reproducible from stored engine/rule/config versions.
- Tenant isolation and audit tests pass.

### Phase 3 - SEND profile and synthetic study

**Work**

- Implement SEND facets, entities, domain relationships, and query catalogue.
- Extend the implemented deterministic SEND generator when a customer journey needs additional domains or biological models.
- Add nonclinical finding materialization and dose-response queries.
- Verify against licensed/customer examples where permitted.

**Gate**

- Cross-domain subject/group/finding relationships reconcile.
- Synthetic study passes all intended rules and triggers every deliberately injected anomaly.
- Nonclinical golden queries return reviewed expected results.

### Phase 4 - ADaM traceability and governed transformation

**Work**

- Add mapping and derivation assets, record-level lineage, analysis metadata, and traceability queries.
- Add analysis-result materialization and reproducible extraction packages.

**Gate**

- Selected ADaM values trace to SDTM/source inputs and method versions.
- Re-execution with pinned inputs produces equivalent outputs.

### Phase 5 - TIG overlay and publication accelerator

**Work**

- Pin TIG dependencies and conformance assets.
- Add product, batch, constituent, test-article, and evidence relationships.
- Build the Product Evidence Explorer and a small synthetic overlay.
- Publish benchmark harness, sizing guidance, and solution architecture.

**Gate**

- Four TIG v1.0 use-case areas are represented without changing the repository kernel.
- TIG rules and dependencies are versioned and reproducible.

## 15. Benchmark plan

Measure independently:

- artifact parse and canonical ingest throughput;
- validation throughput and finding volume;
- exact dataset reconstruction;
- study/subject or study/animal retrieval;
- cross-domain within-study review;
- cross-study facet queries at low, medium, and high selectivity;
- lineage traversal;
- export throughput and equivalence verification;
- projection rebuild and publication delay;
- concurrent ingest/query behavior and index catch-up;
- storage amplification for artifact, canonical, indexes, and materializations.

Every result must state dataset shape, record count, profile/domain mix, indexes, cluster topology, cache state, concurrency, percentile, result size, and whether latency includes API and deserialization. Do not generalize the openEHR billion-document results to CDISC without a CDISC-specific benchmark.

## 16. Testing strategy

| Layer | Required tests |
|---|---|
| Formats | Golden Dataset-JSON, XPT, Define-XML, malformed files, missing values, encodings, long labels/values, date/time precision. |
| Canonical model | Deterministic IDs/hashes, key resolution, type preservation, order, duplicate handling, immutability. |
| Profiles | Facet and entity derivation for each supported domain and standards package. |
| Query | IR schema rejection, tenant predicate enforcement, golden pipelines, golden results, pagination, selectivity plans. |
| Validation | Finding normalization, engine failure, rule-version pinning, waivers, rerun reproducibility. |
| Lineage | Dataset and record DAGs, ADaM-to-SDTM links, cycle rejection. |
| Synthetic | Seed determinism, cross-domain constraints, expected anomalies, watermarking. |
| Export | Original replay, semantic equality, metadata equality, conformance rerun. |
| Operations | Idempotence, crash/retry, staging cleanup, atomic publish, cancellation, job recovery. |
| Security | Cross-tenant denial, role boundaries, redaction, malicious artifact names/paths, decompression limits. |

## 17. Main risks and mitigations

| Risk | Mitigation |
|---|---|
| Standards licensing or redistribution is misunderstood. | Asset manifest, legal classification, fetch-at-setup design, no silent rebundling. |
| A generic schema erases profile semantics. | Preserve `data`; derive versioned profile facets and entities. |
| Facets become an uncontrolled second truth. | Deterministic derivation, digest, rebuild operation, and parity tests. |
| “Round trip” is overclaimed. | Separate byte replay, semantic equivalence, and conformance guarantees. |
| Query design follows the schema rather than user work. | Query catalogue precedes index/materialization acceptance. |
| Synthetic data is internally inconsistent. | Constraint graph, deterministic recipes, validation, and expected-anomaly tests. |
| CORE is treated as complete compliance. | Pluggable engines, capability declarations, pinned rules, explicit coverage report. |
| Heavy analytics harms operational performance. | Bounded operational queries, workload isolation, reproducible analytical export. |
| Generic kernel changes become CDISC-specific. | Keep models/formats in `engine/domains/cdisc`; generalize only artifacts, assets, jobs, audit, and named outputs. |

## 18. First implementation increments

Implementation status on 4 September 2026: the pack scaffold, canonical contracts,
Dataset-JSON 1.1 parser/canonicalizer/exporter, XPT conversion, secure Define-XML
metadata extraction, deterministic SDTM facets,
immutable Mongo upserts, staged publication marker, governed Mongo query
compiler, checksum-addressed artifact replay, and semantic-equivalence reports
are implemented with unit and contract coverage. Structural and referential
validation, external command-validator normalization, scoped waivers,
validation-gated publication, and deterministic linked SDTM/SEND/ADaM/TIG
synthetic generators are also implemented. Profile entities and workload
materializations are rebuildable, semantic retrieval has portable lexical plus
optional Atlas vector/hybrid modes, XPT v5/v8 export is verified by readback,
and portable multi-dataset packages can be emitted. Filesystem, S3-compatible,
and Azure Blob artifact adapters are available, and persisted strategy jobs
have explicit fail-or-retry restart recovery.

The deterministic SEND generator also provides a versioned `safety-signal`
scenario with explicit expected truth across TX, DM, MI, and LB. This is the
seed path for the AI-Ready Nonclinical Safety solution library while keeping
business-specific signal ranking, language, and review workflow outside the
kernel.

That solution boundary is an export/import handoff. A deployed business
solution owns its database, APIs, retrieval indexes, and agent runtime; it does
not require a live Kehrnel connection. Kehrnel remains the reusable environment
for data creation, CDISC validation, model evolution, and query-pattern
learning. `cdisc_export_solution_evidence` formalizes that boundary as
`kehrnel.dev/cdisc-solution-evidence/v1`: one published-snapshot package with
dataset metamodels, canonical records, generic entity/materialization
projections, source-artifact metadata, validation evidence, and transformation
lineage. Deployment-specific tenant scope is removed while source identities
are retained explicitly for traceability; the package payload is protected by
a cross-runtime canonical JSON SHA-256 digest.
The machine-readable contract is shipped beside the package service as
`packages/solution-evidence.schema.json` and is validated in the contract test
suite.

The preview also includes an external example-data catalog with no vendored
study bytes. The official clinical CDISC Pilot DM data and the public PhUSE
SEND study are revision-pinned, SHA-256 verified, and exercised through the
normal repository workflow. Real clinical and preclinical fixtures cover XPT
and Define-XML intake, validation, publication, governed query, and export.
Every persisted document is stamped with `modelSchemaVersion`, and governed
operational query and analysis plans inject that version together with tenant
and snapshot scope.

Licensed standards/rule content, a deployment's selected CDISC CORE or
Pinnacle 21 executable, regulator-specific acceptance testing, and production
infrastructure credentials remain deployment inputs rather than
redistributable repository assets.

| Readiness evidence | Current result |
|---|---|
| Strategy manifest/spec/config | Preview versioned, cross-validated, and documented |
| Operation contracts | 39 operations with typed, runtime-validated top-level responses |
| Clinical example | Official CDISC Pilot XPT/Define journey passes |
| Preclinical example | Public PhUSE SEND XPT/Define journey passes across DM, TX, MI, and LB |
| Schema evolution | `modelSchemaVersion` stored on every document and injected into governed reads |
| Tenant isolation | Cross-tenant query and repository-discovery contract tests pass |
| Real MongoDB | Isolated plan/apply/ingest/query/export test exists; it must pass against the target release tenant |
| Regulated production use | Not claimed; deployment validation and licensed rule/terminology assets are required |

The original delivery increments were:

1. **Contracts and pack scaffold** - `cdisc.sdr` manifest/spec/schema/defaults, canonical Pydantic models, Study Query IR schema, representative Dataset-JSON fixture, and contract tests. No database persistence yet.
2. **Dataset-JSON vertical slice** - reader, canonicalizer, Mongo repositories, idempotent staged ingest/publish, SDTM facet profile, query compiler/executor, and golden results.
3. **Artifact and equivalence foundation** - object artifact adapter, checksum/replay, Dataset-JSON exporter, semantic comparison report, execution/audit records, and failure/retry tests.

Those increments, XPT, Define-XML, SEND breadth, and synthetic generation are
now implemented in the preview. Release publication still requires a fresh
real-MongoDB activation/ingest/query/export run in the target tenant, explicit
real-MongoDB cross-tenant denial evidence, and deployment-specific validation
acceptance.

## 19. Success criteria

The accelerator is credible when it can demonstrate all of the following:

- one activated Kehrnel strategy ingests, validates, publishes, queries, generates, and exports CDISC study snapshots;
- the same kernel supports SEND, SDTM, ADaM, and TIG without four separate persistence products;
- original artifacts and semantic canonical data have distinct, testable fidelity guarantees;
- standards, terminology, mappings, rules, and code are pinned in every result;
- profile projections are fast, disposable, and reproducible;
- query plans are deterministic, tenant-safe, explainable, and benchmarked against reviewed questions;
- synthetic studies are coherent across domains and reproducible by recipe;
- lineage connects source, tabulation, analysis, validation, and export;
- regulatory claims remain bounded to evidence the system can actually produce.

## 20. Sources reviewed

Repository sources:

- `README.md` and the runtime/activation implementation under `src/kehrnel/engine/core/`.
- `src/kehrnel/engine/strategies/openehr/rps_dual/`, including ingest, reverse transform, AQL compilation, query execution, plan/apply, bundles, and synthetic jobs.
- `src/kehrnel/engine/strategies/openehr/rps_dual/assets/rps-paper.pdf`, *A Document-First openEHR Persistence Layer for Operational Patient and Cross-Patient Workloads*.
- `src/kehrnel/engine/strategies/contextobjects/rps/` and the ContextObjects/Con2L documentation.
- `src/kehrnel/engine/strategies/fhir/clinical_cdr/` for reusable query and generation packaging patterns.
- The supplied *Document-First Study Data Repository for CDISC - Research and accelerator strategy*.

Official sources checked on 21 August 2026:

- [CDISC Standards](https://www.cdisc.org/standards)
- [CDISC Library](https://www.cdisc.org/cdisc-library)
- [Dataset-JSON](https://www.cdisc.org/standards/data-exchange/dataset-json)
- [SEND](https://www.cdisc.org/standards/foundational/send)
- [Tobacco Implementation Guide](https://www.cdisc.org/standards/foundational/tobacco-implementation-guide)
- [CDISC SDTM/ADaM Pilot Project](https://github.com/cdisc-org/sdtm-adam-pilot-project)
- [CDISC Library API documentation](https://www.cdisc.org/cdisc-library/api-documentation/oas3)

The official CDISC site currently describes Dataset-JSON v1.1 as a JSON exchange standard for tabular data and API/regulatory scenarios, CDISC Library as the authoritative versioned metadata source exposed through REST and multiple formats, SEND as an SDTM implementation for nonclinical studies and an FDA submission standard, and TIG v1.0 as covering product description, nonclinical, individual-health, and population-health use cases. Exact supported versions for a deployment must still be pinned from the applicable regulator catalog and licensed standards package rather than inferred from a website's latest-version label.

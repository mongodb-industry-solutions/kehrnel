---
sidebar_position: 1
---

# CDISC Study Data Repository Strategy

The **Document-First CDISC Study Data Repository** strategy (`cdisc.sdr`) is \{kehrnel\}'s strategy for governed CDISC study data on MongoDB.

It keeps the CDISC study snapshot as the unit of governance. Original Dataset-JSON, XPT, and Define-XML artifacts stay checksum-addressed and replayable. Each dataset is described by a variable-level metamodel, and every CDISC row is stored once, under its original variable names, in one polymorphic record collection. Typed facets, entity references, workload views, and an optional semantic projection are derived from that canonical data and can be rebuilt at any time.

## Overview

| Aspect | What the strategy does |
|--------|------------------------|
| Profiles | SEND, SDTM, ADaM, TIG (`send`, `sdtm`, `adam`, `tig`) |
| Intake | Dataset-JSON 1.1 (inline or as a multi-dataset package), SAS XPORT from a retained artifact, Define-XML metadata |
| Governance unit | Immutable study snapshot with an explicit state and a single publication marker |
| Storage | Twelve MongoDB collections: canonical, evidence, and rebuildable projection collections |
| Query | Governed `cdisc-query/v1` compiled to a MongoDB aggregation pipeline |
| Analysis | Governed grouped metrics through `cdisc-analysis/v1` |
| Search | Lexical, semantic-vector, or hybrid (reciprocal-rank fusion) retrieval over the semantic projection |
| Export | Dataset-JSON with semantic equivalence check, XPT v5 or v8 with read-back verification, deterministic packages, solution evidence packages |
| Validation | Built-in structural and cross-dataset rules, an optional external validation command, scoped waivers, and a publication gate |
| Lineage | Recorded transformations, lineage inspection, and snapshot supersession without deleting history |

## Why Document-First

Study data arrives as many related tabular datasets whose variables differ by CDISC domain and standard version. A separate rigid schema per domain creates operational sprawl. Treating the data as a patient record loses the study and submission context.

`cdisc.sdr` stores one record document per CDISC row with a stable governance envelope (tenant, study, snapshot, dataset, profile, domain, row order, key) and a `data` object that holds the original variables. The dataset metamodel says which variables can appear in `data`. Stable fields such as `facets.subjectId`, `facets.testCode`, or `facets.severity` make clinical, nonclinical, analysis, and product records queryable through one contract.

## Snapshot Lifecycle

Snapshot states declared in the code: `received`, `parsed`, `canonicalized`, `validated`, `published`, `quarantined`, `superseded`.

| Step | Resulting state |
|------|-----------------|
| Ingest with `publicationState: staged` (the default) | `canonicalized` |
| `cdisc_validate_snapshot` passes | `validated` |
| `cdisc_validate_snapshot` fails on a blocking severity | `quarantined` |
| `cdisc_publish_snapshot` | `published` (projections are rebuilt when `projections.rebuild_on_publish` is true) |
| `cdisc_supersede_snapshot` (both snapshots published) | old snapshot becomes `superseded` |

With `validation.require_before_publish: true` (the default), only a `validated` snapshot can be published, and direct ingestion with `publicationState: published` is rejected with `CDISC_VALIDATION_REQUIRED`. Ingesting changed content into a published dataset identity is rejected with `CDISC_SNAPSHOT_IMMUTABLE`; a correction creates a new snapshot.

`cdisc_semantic_search` reads published snapshots only. `cdisc-query/v1` and `cdisc-analysis/v1` default to `scope.snapshots: "published"`, which adds a join on the snapshot collection and keeps only published snapshots. If you pass an explicit list of snapshot ids instead, the query filters on those ids and does not add the publication constraint. The explain output reports this as `governance.publicationConstraintInjected`.

## Interaction Contracts

| Contract id | Mode | Contract | Description |
|-------------|------|----------|-------------|
| `cdisc.dataset-exchange` | native | Dataset-JSON v1.1 API v1.0 and SAS XPORT | Preserves and exchanges standards-conformant datasets and metadata |
| `cdisc.study-query` | compiled | `cdisc-query/v1` | Compiles study questions into bounded MongoDB plans (builder `cdisc_study_query_v1`) |
| `cdisc.study-analysis` | compiled | `cdisc-analysis/v1` | Compiles governed groupings and metrics into an inspectable aggregation (`cdisc_run_analysis`) |
| `cdisc.operational-evidence` | hybrid | CDISC Solution Evidence Package | Combines canonical evidence with rebuildable timelines, findings, analyses, and lineage |

## Capabilities

Manifest capabilities: `transform`, `reverse_transform`, `ingest`, `query`, `validate`, `generate`, `plan`, `apply`, `study_data`, `artifact_replay`, `semantic_equivalence`, `semantic_ready`, `operational_objects`.

Strategy operations, grouped by kind:

| Area | Operations |
|------|------------|
| Artifacts | `cdisc_store_artifact`, `cdisc_initiate_upload`, `cdisc_finalize_upload`, `cdisc_register_external_artifact`, `cdisc_prepare_download`, `cdisc_replay_artifact` |
| Intake | `cdisc_inspect_dataset_json`, `cdisc_ingest_dataset_json`, `cdisc_inspect_define_xml`, `cdisc_ingest_xpt`, `cdisc_ingest_package`, `cdisc_list_examples`, `cdisc_ingest_example` |
| Standards | `cdisc_register_standards`, `cdisc_get_standards_package`, `cdisc_list_standards` |
| Validation and publication | `cdisc_validate_snapshot`, `cdisc_get_validation_run`, `cdisc_register_validation_waiver`, `cdisc_publish_snapshot` |
| Discovery | `cdisc_list_studies`, `cdisc_list_snapshots`, `cdisc_snapshot_summary`, `cdisc_list_datasets`, `cdisc_list_validation_runs`, `cdisc_list_artifacts` |
| Projections | `cdisc_rebuild_projections`, `cdisc_browse_projections` |
| Query and analysis | `cdisc_semantic_search`, `cdisc_run_analysis`, `cdisc_ask_assistant` |
| Export | `cdisc_export_dataset_json`, `cdisc_export_xpt`, `cdisc_export_package`, `cdisc_export_solution_evidence` |
| Lineage | `cdisc_get_lineage`, `cdisc_record_transformation`, `cdisc_supersede_snapshot` |
| Synthetic data | `cdisc_generate_synthetic_study` |

Every operation has an input and an output JSON Schema in the manifest. The runtime validates the input; the strategy validates its own response against the output schema and fails with `CDISC_OUTPUT_CONTRACT_VIOLATION` if it does not match. Use `kehrnel op schema <operation> --strategy cdisc.sdr` to read a schema.

`cdisc_ask_assistant` is read-only. It routes a question to one of the intents `summary`, `validation`, `lineage`, `search`, or `analysis` (by keyword, or explicitly through `intent`) and answers from the governed services with evidence citations.

## API Placement

| Layer | Responsibility |
|-------|----------------|
| Core runtime | Activation, plan and apply, `query` and `compile_query`, operation dispatch, background jobs |
| Domain | No CDISC domain router; see the [CDISC Domain](/docs/domains/cdisc/) page |
| Strategy | Ingestion, validation, publication, projections, query compilation, export, lineage, synthetic generation |
| MongoDB | Strategy-owned database, twelve collections, B-tree indexes, optional Atlas Search and vector indexes |
| Artifact store | Filesystem, S3-compatible, or Azure Blob storage for immutable artifact bytes |

## Adapters and Dependencies

| Kind | Providers |
|------|-----------|
| Storage | `mongodb` (required) |
| Artifact | `filesystem`, `s3`, `azure-blob` |
| Search | `mongodb` (Atlas Search and Atlas Vector Search with Automated Embedding, optional) |
| Validation | Optional external command (`validation.provider: command` in bindings) |

Python extras: `kehrnel-core[cdisc]` installs `pyreadstat` and `pandas` for XPT import and export; `kehrnel-core[cdisc-cloud]` adds the S3 and Azure Blob clients.

## Maturity

Manifest maturity: **preview** (`0.2.0`). Persisted document contract: `modelSchemaVersion` `1.1.0`.

The manifest scores the strategy at 1 out of 5 for submission authoring. It does not author regulatory submissions, and passing the built-in rules is not regulatory conformance. Regulator acceptance and validation of the deployed system remain the deployment owner's responsibility.

Heavy biostatistical scans are out of scope for governed queries; the spec recommends governed extracts or analytical systems for that work.

## Next Steps

- [Configuration](./configuration.md)
- [Workflows](./workflows.md)
- [Data model](./data-model.md)
- [CDISC Domain](/docs/domains/cdisc/)

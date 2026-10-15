---
sidebar_position: 4
---

# CDISC Data Model

`cdisc.sdr` separates three kinds of data:

- **Canonical** documents: studies, snapshots, dataset metamodels, and records. They are the source of truth.
- **Evidence** documents: artifacts, standards packages, validation runs, findings, waivers, and transformations.
- **Rebuildable projections**: record facets, entity references, the semantic text, entity documents, and workload views. They can be recreated from canonical data and pinned metadata.

Every persisted document carries `modelSchemaVersion` (currently `1.1.0`) and `tenantId`. `modelSchemaVersion` identifies the \{kehrnel\} document shape. It is independent of the strategy release and of the SEND, SDTM, ADaM, TIG, and exchange standard versions. Governed record queries add the current version to their filter.

## Collections

Collection names are configured under `collections` and must be distinct. The defaults are:

| Config key | Default collection | Purpose |
|------------|--------------------|---------|
| `studies` | `cdisc_studies` | Study identity and study-level discovery |
| `snapshots` | `cdisc_snapshots` | Immutable version and publication boundary |
| `datasets` | `cdisc_datasets` | Variable definitions, keys, order, counts, standard pins, content hashes |
| `records` | `cdisc_records` | One canonical CDISC row per document, with facets, entity references, lineage, and optional semantic text |
| `entities` | `cdisc_entities` | Subject, animal, treatment group, specimen, product, batch, and parameter navigation |
| `materializations` | `cdisc_materializations` | Subject timelines, nonclinical findings, analysis traceability, and product evidence views |
| `standards` | `cdisc_standards` | Pinned model, implementation guide, terminology, exchange, and validation-rule manifests |
| `artifacts` | `cdisc_artifacts` | Checksums, media types, object-store references, and acquisition provenance |
| `validation_runs` | `cdisc_validation_runs` | Validation engine, status, summary, coverage, and input digest |
| `validation_findings` | `cdisc_validation_findings` | Normalized rule findings linked to datasets, records, and variables |
| `validation_waivers` | `cdisc_validation_waivers` | Immutable, scoped, optionally expiring waivers |
| `transformations` | `cdisc_transformations` | Ingest, publish, export, and recorded transformation executions with input and output references |

## Identity

The study id comes from the Dataset-JSON `studyOID`. The domain is the upper-cased Dataset-JSON `name`.

| Document | `_id` |
|----------|-------|
| Study | `tenantId:studyId` |
| Snapshot (also `snapshotRef`) | `tenantId:studyId:snapshotId` |
| Dataset (also `datasetId`) | `tenantId:studyId:snapshotId:DOMAIN` |
| Record | SHA-256 of tenant, study, snapshot, domain, and record key. The key is the set of key variables (columns with `keySequence`), or the row ordinal when no key is declared |
| Artifact | `tenantId:sha256:<digest>`, or `tenantId:artifact:<artifactId>` when an `artifactId` is supplied |
| Standards package | `tenantId:packageId` |
| Validation waiver | `tenantId:waiver:<waiverId>` (a UUID when `waiverId` is not supplied) |
| Entity | `snapshotRef:entity:<entityType>:<entityId>` |
| Materialization | `snapshotRef:view:<kind>:<groupId>` |

Duplicate record identities inside one dataset are rejected at ingestion.

## Snapshot

```json
{
  "_id": "default:STUDY01:v1",
  "modelSchemaVersion": "1.1.0",
  "tenantId": "default",
  "studyId": "STUDY01",
  "snapshotId": "v1",
  "packageId": "study01-package",
  "profile": "sdtm",
  "standardsPackageId": "study01-package",
  "state": "canonicalized",
  "datasetIds": ["default:STUDY01:v1:DM"],
  "artifactIds": [],
  "createdAt": "..."
}
```

`state` is one of `received`, `parsed`, `canonicalized`, `validated`, `published`, `quarantined`, `superseded`. The snapshot document is written last during ingestion, so a partial write stays invisible. `standardsPackageId` defaults to `packageId` when it is not supplied. Validation adds `latestValidationRunId`; publication adds `publishedAt`; supersession adds `supersededBy`, `supersededAt`, and `supersessionReason`.

## Dataset Metamodel

A dataset document describes the variables that may appear in each record's `data` object.

| Field | Meaning |
|-------|---------|
| `studyId`, `packageId`, `snapshotId`, `snapshotRef` | Scope |
| `profile`, `domain`, `name`, `label`, `itemGroupOID` | Identity and discriminators |
| `standard` | `family`, `modelVersion`, `implementationGuide`, `implementationGuideVersion`, `terminologyPackages`, `exchangeStandard`, `exchangeVersion`, `regulatorContext` |
| `variables[]` | `ordinal`, `itemOID`, `name`, `label`, `dataType`, `targetDataType`, `length`, `keySequence`, `displayFormat`, `codelistOID`, `origin` |
| `keyVariables` | Variables with a `keySequence`, in key order |
| `recordCount`, `contentHash` | Integrity |
| `sourceArtifactId`, `sourceMetadata` | Provenance, including Dataset-JSON header fields such as `datasetJSONVersion`, `fileOID`, `metaDataVersionOID`, `originator` |
| `publicationState` | `staged`, `published`, `quarantined`, or `superseded` |

Ingesting different content under an existing dataset identity fails with `CDISC_SNAPSHOT_IMMUTABLE`. Ingesting identical content is idempotent.

## Record

```json
{
  "_id": "<sha256>",
  "modelSchemaVersion": "1.1.0",
  "tenantId": "default",
  "studyId": "STUDY01",
  "packageId": "study01-package",
  "snapshotId": "v1",
  "snapshotRef": "default:STUDY01:v1",
  "datasetId": "default:STUDY01:v1:AE",
  "profile": "sdtm",
  "domain": "AE",
  "publicationState": "staged",
  "rowOrdinal": 1,
  "recordKey": {"USUBJID": "STUDY01-001", "AESEQ": 1},
  "data": {"STUDYID": "STUDY01", "USUBJID": "STUDY01-001", "AESEQ": 1, "AEDECOD": "HEADACHE", "AESEV": "MILD", "AESTDY": 3},
  "facets": {"projectionVersion": "1.0.0", "subjectType": "human", "subjectId": "STUDY01-001", "sequence": 1, "studyDay": 3, "severity": "MILD", "eventTerm": "HEADACHE"},
  "entityRefs": [{"type": "humanSubject", "id": "STUDY01-001"}, {"type": "study", "id": "STUDY01"}],
  "lineage": {"sourceDataset": "AE", "sourceRow": 1, "recordHash": "<sha256>"}
}
```

`data` keeps the original CDISC variable names and values. Records are sparse: optional null, blank, empty-array, and empty-object values are omitted. The dataset metamodel and the retained source artifact keep the full variable list and order, so Dataset-JSON and XPT export can reconstruct missing cells.

### Facets

Facets are deterministic typed projections (`projectionVersion` `1.0.0`). They are derived from well-known CDISC variables, with a shared base and one overlay per profile:

| Group | Facets |
|-------|--------|
| Base (all profiles) | `subjectType` (`animal` for SEND, `human` otherwise, `evidence` for TIG), `subjectId`, `sex`, `species`, `strain`, `armCode`, `arm`, `treatmentGroup`, `visitNumber`, `visit`, `epoch`, `sequence`, `studyDay`, `testCode`, `test`, `category`, `specimen`, `severity`, `resultCharacter`, `resultNumeric`, `resultUnit` |
| SEND | `testArticle`, `doseLevel`, `doseUnit`, `route`, `organ`, `finding`, `laterality`, `specimenId` |
| SDTM | `eventTerm`, `intervention`, `startDateTime`, `endDateTime`, `referenceId` |
| ADaM | `parameterCode`, `parameter`, `analysisValue`, `analysisValueCharacter`, `baselineType`, `analysisRecordFlag`, `populationFlags`, `sourceDomain`, `sourceVariable`, `sourceSequence` |
| TIG | `productId`, `productName`, `batchId`, `constituent`, `evidenceType`, `evidenceId` |

For example, `subjectId` comes from `USUBJID` (or `SUBJID`), `studyDay` from `ADY` or the domain's `--DY`, `--STDY`, or `--ENDY`, and `testCode` from `--TESTCD` or `PARAMCD`.

### Entity references

`entityRefs` link a record to `animalSubject` (SEND) or `humanSubject`, `study`, `treatmentGroup`, `specimen`, `testArticle`, `product`, `batch`, and `analysisParameter`, when the source variables are present.

### Semantic projection

When `semantic.enabled` is true, a projection rebuild writes `semantic.text` on each record. The text is limited to `semantic.max_text_chars` and leaves out the variables listed in `semantic.exclude_variables` (by default `USUBJID` and `SUBJID`). Lexical and vector search run over this field only.

## Entities and Materializations

`cdisc_rebuild_projections` (run automatically on publication when `projections.rebuild_on_publish` is true) writes:

- **Entity documents** in `entities`: `entityType`, `entityId`, `studyId`, `profile`, `domains`, `recordIds`, `recordCount`, `projectionBuildId`, `projectionVersion`.
- **Materialization documents** in `materializations`: `kind`, `groupId`, `entries`, `entryCount`, `projectionBuildId`, `projectionVersion`.

The materialization `kind` depends on the profile: `subject-timeline` (SDTM), `nonclinical-finding` (SEND), `analysis-traceability` (ADaM), `product-evidence` (TIG). Entries are ordered by study day, domain, and row ordinal. For SEND subjects, the view also carries a `SubjectExposureResponse` operational object (schema version `1.0.0`) built from the DM, TX, EX, BW, BG, FW, LB, CL, MA, MI, OM, PC, PP, SE, and DS domains, with source-row lineage.

A synchronous rebuild fails when the snapshot has more records than `projections.max_records_per_rebuild`. Use `cdisc_browse_projections` with `resource: entities` or `resource: materializations` to read them.

## Evidence Collections

| Collection | Main fields |
|------------|-------------|
| `cdisc_artifacts` | `artifactId`, `provider`, `uri`, `mediaType`, `size`, `digest` (`algorithm`, `value`), `sourceName`, `acquiredAt`, `metadata` |
| `cdisc_standards` | `packageId`, `profile`, `standard`, `assets[]`, `validationEngines`, `rulePackages[]` |
| `cdisc_validation_runs` | `runId`, `snapshotRef`, `engine`, `engineVersion`, `status` (`passed` or `failed`), `summary`, `inputDigest`, `standardsPackageId`, `rulePackages`, `coverage`, `startedAt`, `completedAt` |
| `cdisc_validation_findings` | `runId`, `snapshotRef`, `severity` (`error`, `warning`, `info`), `ruleId`, `message`, `datasetId`, `recordId`, `variable`, `waived`, `waiverId`, `details` |
| `cdisc_validation_waivers` | `waiverId`, `ruleId`, `justification`, `approvedBy`, optional scope (`studyId`, `snapshotId`, `datasetId`, `recordId`, `variable`), `expiresAt` |
| `cdisc_transformations` | `executionId`, `operation`, `status`, `inputRefs`, `outputRefs`, `startedAt`, `completedAt` (plus mapping and code versions for recorded transformations) |

A waived finding is kept with severity `info`, `waived: true`, and its original severity in `details.originalSeverity`.

Built-in validation rule ids include `SDR.DATASET.MISSING`, `SDR.RECORD_COUNT`, `SDR.STUDYID.CONSISTENT`, `SDR.DOMAIN.CONSISTENT`, `SDR.KEY.REQUIRED`, `SDR.SUBJECT.UNKNOWN`, `SDR.PRODUCT.UNKNOWN`, `SDR.STANDARDS.UNREGISTERED` (when `validation.require_registered_standards` is true), and `SDR.VALIDATION_ENGINE.FAILURE` (when an external engine fails and `validation.external_failure_policy` is `finding`).

## Indexes

`plan` creates these indexes on the configured collection names:

| Index | Collection | Keys | Unique | Spec requirement |
|-------|------------|------|--------|------------------|
| `idx_cdisc_dataset_order` | records | `tenantId`, `snapshotRef`, `datasetId`, `rowOrdinal` | no | required |
| `idx_cdisc_study_subject` | records | `tenantId`, `studyId`, `facets.subjectId`, `domain`, `facets.studyDay` | no | recommended |
| `idx_cdisc_profile_domain_test` | records | `tenantId`, `profile`, `domain`, `facets.testCode` | no | recommended |
| `idx_cdisc_entity` | entities | `tenantId`, `snapshotRef`, `entityType`, `entityId` | yes | required |
| `idx_cdisc_materialization` | materializations | `tenantId`, `snapshotRef`, `kind`, `groupId` | no | recommended |
| `idx_cdisc_dataset_identity` | datasets | `tenantId`, `studyId`, `snapshotId`, `domain` | yes | required |
| `idx_cdisc_snapshot_state` | snapshots | `tenantId`, `studyId`, `snapshotId`, `state` | no | required |
| `idx_cdisc_artifact_digest` | artifacts | `tenantId`, `digest.value` | yes | required |
| `idx_cdisc_validation_run` | validation_runs | `tenantId`, `snapshotRef`, `completedAt` (descending) | no | recommended |
| `idx_cdisc_finding` | validation_findings | `tenantId`, `snapshotRef`, `severity` | no | recommended |
| `idx_cdisc_waiver` | validation_waivers | `tenantId`, `ruleId`, `expiresAt` | no | recommended |
| `idx_cdisc_transformation_input` | transformations | `tenantId`, `inputRefs`, `completedAt` (descending) | no | recommended |

When `semantic.enabled` is true, `plan` also declares two indexes on the records collection:

- an Atlas Search index named by `semantic.lexical_index` (default `cdisc_semantic_text`) over `semantic.text`;
- an Atlas Vector Search index named by `semantic.auto_embed_index` (default `cdisc_semantic_auto_embed`) that uses Automated Embedding on `semantic.text` with the model in `semantic.auto_embedding_model`, and filter fields `tenantId`, `snapshotRef`, `studyId`, `profile`, `domain`.

Arbitrary `data.*` fields stay queryable through `cdisc-query/v1` but are not indexed automatically. `modelSchemaVersion` is deliberately not a standalone index because of its low cardinality.

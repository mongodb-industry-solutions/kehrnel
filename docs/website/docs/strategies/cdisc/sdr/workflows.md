---
sidebar_position: 3
---

# CDISC Workflows

End-to-end path for `cdisc.sdr`. CDISC has no domain router and no dedicated CLI group: every step uses the universal runtime endpoints or the generic `kehrnel` commands.

A normal governed flow is:

1. Activate the strategy and create collections and indexes (plan and apply).
2. Retain source artifacts (XPT, Define-XML, Dataset-JSON).
3. Ingest every dataset into a staged snapshot.
4. Validate the snapshot.
5. Publish the snapshot (projections are rebuilt automatically).
6. Query with `cdisc-query/v1`, search, run grouped analysis, or ask the read-only assistant.
7. Export Dataset-JSON, verified XPT, or a deterministic package.
8. Inspect validation and transformation lineage.

The examples use environment `dev`, database `cdisc_sdr`, study `STUDY01`, and snapshot `v1`.

## Calling Operations

Strategy operations run through:

```text
POST /environments/{env_id}/activations/cdisc/ops/{op}
```

The request body is the operation payload. The response is `{"ok": true, "result": ...}`. The runtime validates the payload against the operation's input schema; most schemas reject unknown fields.

From the CLI:

```bash
# Discover operations and payload schemas
kehrnel op list --domain cdisc --strategy cdisc.sdr
kehrnel op schema cdisc_ingest_dataset_json --strategy cdisc.sdr

# Run one operation (posts the payload file unchanged)
kehrnel core env op cdisc_list_studies --env dev --domain cdisc --payload payload.json

# Universal executor (POST /environments/{env_id}/run)
kehrnel run cdisc_list_studies --env dev --domain cdisc --payload payload.json
```

The `/run` endpoint removes routing keys such as `domain` and `strategy_id` before the payload reaches the operation.

## Activate

```bash
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

curl -sS -X POST "${RUNTIME_URL}/environments/dev/activate" \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_id": "cdisc.sdr",
    "version": "0.2.0",
    "domain": "cdisc",
    "config": {
      "database": "cdisc_sdr",
      "tenant_id": "default"
    },
    "bindings": {
      "db": {"provider": "mongodb", "uri": "mongodb://localhost:27017", "database": "cdisc_sdr"},
      "artifact": {"provider": "filesystem", "root": "/var/lib/kehrnel/cdisc-artifacts"}
    },
    "allow_plaintext_bindings": true
  }'
```

Plaintext bindings are accepted only when authentication is disabled. With authentication enabled, pass `bindings_ref` instead.

CLI equivalent:

```bash
kehrnel core env activate --env dev --domain cdisc --strategy cdisc.sdr \
  --config activation-config.json --bindings-ref secret://kehrnel/cdisc-sdr/default
```

XPT import and export need the `cdisc` extra (`kehrnel-core[cdisc]`); S3 and Azure Blob artifact stores need `cdisc-cloud`.

## Plan and Apply

`plan` returns the collections, B-tree indexes, and (when `semantic.enabled` is true) the Atlas Search and vector index definitions. Pass the returned plan to `apply`.

```bash
# Plan, keep the "result" object, and send it to apply as "plan"
PLAN=$(curl -sS -X POST "${RUNTIME_URL}/environments/dev/plan" \
  -H "Content-Type: application/json" -d '{"domain": "cdisc"}' | jq '.result')

curl -sS -X POST "${RUNTIME_URL}/environments/dev/apply" \
  -H "Content-Type: application/json" \
  -d "{\"domain\": \"cdisc\", \"plan\": ${PLAN}}"
```

`apply` creates the planned collections and indexes; called without a `plan`, it creates nothing. If no Atlas Search adapter is available, it returns a warning and skips the search and vector indexes.

## Retain Artifacts

Store source bytes before XPT or Define-XML intake. Artifacts are addressed by SHA-256 and are immutable.

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/activations/cdisc/ops/cdisc_store_artifact" \
  -H "Content-Type: application/json" \
  -d "{\"contentBase64\": \"$(base64 < ae.xpt | tr -d '\n')\", \"mediaType\": \"application/x-sas-xport\", \"sourceName\": \"ae.xpt\"}"
```

The result includes `artifact.artifactId` and `created` (false when the same bytes were already stored). Supplying `expectedSha256` makes the call fail if the digest does not match.

| Operation | Use |
|-----------|-----|
| `cdisc_store_artifact` | Inline upload, up to `artifact.max_inline_bytes` |
| `cdisc_initiate_upload` then `cdisc_finalize_upload` | Direct object-store upload for large files (`sha256`, `size`, `mediaType`; `expiresIn` 60 to 3600 seconds) |
| `cdisc_register_external_artifact` | Register and verify bytes already uploaded to the object store (`objectKey`, `sha256`, `mediaType`) |
| `cdisc_prepare_download` | Short-lived direct download target |
| `cdisc_replay_artifact` | Return the original bytes inline after digest and size verification, up to `artifact.max_replay_bytes` |
| `cdisc_list_artifacts` | List retained artifacts, optionally for one snapshot |

## Ingest Dataset-JSON

Inspect a Dataset-JSON 1.1 document without persisting it:

```json
{
  "datasetJSON": {
    "datasetJSONVersion": "1.1.0",
    "studyOID": "STUDY01",
    "name": "DM",
    "label": "Demographics",
    "records": 1,
    "columns": [
      {"itemOID": "IT.DM.STUDYID", "name": "STUDYID", "label": "Study Identifier", "dataType": "string"},
      {"itemOID": "IT.DM.USUBJID", "name": "USUBJID", "label": "Unique Subject Identifier", "dataType": "string", "keySequence": 1},
      {"itemOID": "IT.DM.SEX", "name": "SEX", "label": "Sex", "dataType": "string"}
    ],
    "rows": [["STUDY01", "STUDY01-001", "F"]]
  }
}
```

Send it to `cdisc_inspect_dataset_json`. To persist, call `cdisc_ingest_dataset_json` with the same document plus the snapshot contract:

```json
{
  "datasetJSON": {"...": "Dataset-JSON document"},
  "packageId": "study01-package",
  "snapshotId": "v1",
  "profile": "sdtm",
  "standard": {
    "family": "SDTM",
    "implementationGuide": "SDTMIG",
    "implementationGuideVersion": "3.4"
  },
  "publicationState": "staged"
}
```

- `studyId` comes from `studyOID`; the domain comes from `name`.
- `dryRun: true` returns the canonical dataset and record count without writing.
- Ingestion is idempotent for identical content. Different content for an existing dataset identity fails with `CDISC_SNAPSHOT_IMMUTABLE`.
- With `validation.require_before_publish: true`, `publicationState: published` is rejected; ingest as `staged`, then validate and publish.

### Multi-dataset package

`cdisc_ingest_package` preflights and ingests several Dataset-JSON documents into one snapshot, and can validate and publish in the same call:

```json
{
  "packageId": "study01-package",
  "snapshotId": "v1",
  "profile": "sdtm",
  "standard": {"family": "SDTM", "implementationGuide": "SDTMIG", "implementationGuideVersion": "3.4"},
  "datasets": [
    {"datasetJSON": {"...": "DM Dataset-JSON"}},
    {"datasetJSON": {"...": "AE Dataset-JSON"}}
  ],
  "validate": true,
  "publish": true
}
```

## Ingest XPT

1. Store the `.xpt` file (and optionally `define.xml`) with `cdisc_store_artifact`.
2. Optionally inspect the Define-XML with `cdisc_inspect_define_xml` (`artifactId`).
3. Call `cdisc_ingest_xpt`:

```json
{
  "xptArtifactId": "default:sha256:<digest>",
  "defineArtifactId": "default:sha256:<digest>",
  "studyOID": "STUDY01",
  "packageId": "study01-package",
  "snapshotId": "v1",
  "profile": "sdtm",
  "standard": {"family": "SDTM", "implementationGuide": "SDTMIG", "implementationGuideVersion": "3.4"},
  "publicationState": "staged"
}
```

The XPT is converted to Dataset-JSON, optionally enriched with Define-XML metadata, and ingested through the same canonical path.

## Register Standards

Register an immutable standards-package manifest with `cdisc_register_standards` (`package`: `packageId`, `profile`, `standard`, and optional `assets`, `validationEngines`, `rulePackages`). Reference it with `standardsPackageId` at ingestion. Read it back with `cdisc_get_standards_package` or `cdisc_list_standards`.

Set `validation.require_registered_standards: true` to make validation fail when the referenced package is missing.

## Validate and Publish

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/activations/cdisc/ops/cdisc_validate_snapshot" \
  -H "Content-Type: application/json" \
  -d '{"studyId": "STUDY01", "snapshotId": "v1"}'

curl -sS -X POST "${RUNTIME_URL}/environments/dev/activations/cdisc/ops/cdisc_publish_snapshot" \
  -H "Content-Type: application/json" \
  -d '{"studyId": "STUDY01", "snapshotId": "v1"}'
```

- Validation runs the built-in structural and cross-dataset rules and, when a `validation` command binding exists, the external engine. It stores a run and normalized findings, and sets the snapshot to `validated` (pass) or `quarantined` (fail).
- Publication is a single snapshot-marker update. It fails with `CDISC_SNAPSHOT_NOT_PUBLISHABLE` if the snapshot has not passed its gate. Publishing an already published snapshot returns `alreadyPublished: true`.
- Projections are rebuilt on publication when `projections.rebuild_on_publish` is true. You can also run `cdisc_rebuild_projections` explicitly.

Review results with `cdisc_get_validation_run` (`runId`, optional `severity` and `limit`) or `cdisc_list_validation_runs`.

To accept a known finding, register a waiver with `cdisc_register_validation_waiver` (`ruleId`, `justification`, `approvedBy`, and optional scope fields and `expiresAt`), then validate again. Waivers are immutable.

## Discover

| Operation | Returns |
|-----------|---------|
| `cdisc_list_studies` | Studies with their latest snapshot |
| `cdisc_list_snapshots` | Snapshots of one study (`studyId`, optional `state`) |
| `cdisc_snapshot_summary` | One snapshot with its datasets and source artifacts |
| `cdisc_list_datasets` | Datasets, filterable by `studyId`, `snapshotId`, `profile`, `domain` |
| `cdisc_list_validation_runs` | Validation runs, filterable by study, snapshot, `status` |
| `cdisc_list_standards` | Registered standards packages |
| `cdisc_list_artifacts` | Retained artifacts |
| `cdisc_browse_projections` | Entity or materialization documents of one snapshot |

Lists are tenant-scoped and paginated with `pageSize` and an opaque `cursor`.

## Query with `cdisc-query/v1`

Send the query to the runtime query endpoint with `domain: cdisc`:

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/query" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "cdisc",
    "query": {
      "version": "cdisc-query/v1",
      "scope": {"studies": ["STUDY01"], "snapshots": "published"},
      "from": {"profile": "sdtm", "domains": ["AE"]},
      "where": {"and": [
        {"path": "facets.severity", "op": "in", "value": ["MODERATE", "SEVERE"]},
        {"path": "facets.studyDay", "op": "gte", "value": 1}
      ]},
      "select": ["studyId", "snapshotId", "data.USUBJID", "data.AEDECOD", "facets.studyDay"],
      "orderBy": [{"path": "facets.studyDay", "direction": "asc"}],
      "page": {"limit": 100}
    }
  }'
```

Use `POST /environments/dev/compile_query` with the same body to see the MongoDB pipeline and explain without running it. CLI: `kehrnel core env query --env dev --domain cdisc --payload query.json` and `kehrnel core env compile-query ...`, where the payload file contains `domain` and `query`.

Query rules:

| Part | Rule |
|------|------|
| `scope.snapshots` | `"published"` (default) adds the publication constraint; a list of snapshot ids filters on those ids only |
| `from` | Optional `profile` (`send`, `sdtm`, `adam`, `tig`) and `domains` |
| `where.and` | Predicates with `path`, `op`, `value`. Operators: `eq`, `ne`, `in`, `nin`, `gt`, `gte`, `lt`, `lte`, `exists`, `contains` |
| Paths | Envelope fields (`studyId`, `snapshotId`, `datasetId`, `profile`, `domain`, `rowOrdinal`, `recordKey`, `entityRefs.type`, `entityRefs.id`, and others), the documented facets, `data.<VARIABLE>` (upper-case CDISC names), and configured `query.extra_allowed_paths` |
| `page` | `limit` up to `query.max_limit`; `token` from a previous response's `explain.pagination.nextToken` |

The compiler always injects the tenant and current `modelSchemaVersion` and rejects client MongoDB operators.

## Search

`cdisc_semantic_search` runs over published snapshots:

```json
{
  "q": "liver microscopic hypertrophy",
  "mode": "hybrid",
  "profile": "send",
  "domains": ["MI"],
  "studyIds": ["STUDY01"],
  "limit": 20
}
```

`mode` is `lexical`, `semantic`, or `hybrid` (default). Lexical uses the Atlas Search index when available, otherwise a bounded portable scan. Semantic uses the Automated Embedding vector index. Hybrid merges both with reciprocal-rank fusion and falls back to lexical results, with a warning, if vector search is unavailable.

## Grouped Analysis

`cdisc_run_analysis` compiles a `cdisc-analysis/v1` request into a bounded aggregation:

```json
{
  "version": "cdisc-analysis/v1",
  "scope": {"studies": ["STUDY01"], "snapshots": "published"},
  "from": {"profile": "send", "domains": ["MI"]},
  "groupBy": ["facets.treatmentGroup", "facets.organ", "facets.severity"],
  "metrics": [
    {"name": "findings", "op": "count"},
    {"name": "animals", "op": "countDistinct", "path": "facets.subjectId"}
  ],
  "orderBy": [{"field": "findings", "direction": "desc"}],
  "limit": 100
}
```

Up to 5 `groupBy` paths and 20 metrics (`count`, `countDistinct`, `sum`, `avg`, `min`, `max`); `limit` up to 500. Paths follow the same allowlist as `cdisc-query/v1`.

## Assistant

`cdisc_ask_assistant` answers a bounded question about one snapshot (`question`, `studyId`, `snapshotId`) using read-only governed services and returns evidence citations. Optional fields: `intent` (`summary`, `validation`, `lineage`, `search`, `analysis`), `profile`, `domains`, `mode`, `limit`, `analysis`.

## Export

| Operation | Input | Output |
|-----------|-------|--------|
| `cdisc_export_dataset_json` | `datasetId`, optional `persistArtifact` | Reconstructed Dataset-JSON plus a semantic equivalence report |
| `cdisc_export_xpt` | `datasetId`, optional `version` (`5` or `8`) | Retained XPT artifact, verified by read-back |
| `cdisc_export_package` | `studyId`, `snapshotId`, optional `allowUnpublished` | Deterministic ZIP of Dataset-JSON files with manifest and equivalence evidence, retained as an artifact |
| `cdisc_export_solution_evidence` | `studyId`, `snapshotId`, optional `allowUnpublished` | Portable, versioned evidence package, retained as an artifact |

Download retained exports with `cdisc_replay_artifact` or `cdisc_prepare_download`.

## Lineage and Supersession

| Operation | Use |
|-----------|-----|
| `cdisc_get_lineage` | Snapshot, dataset, artifact, validation, and transformation lineage (`studyId`, `snapshotId`) |
| `cdisc_record_transformation` | Record a deterministic transformation (`operation`, `inputRefs`, `outputRefs`, optional `mappingId`, `mappingVersion`, `codeVersion`, `parameters`) |
| `cdisc_supersede_snapshot` | Link a published snapshot to its published replacement (`studyId`, `snapshotId`, `replacementSnapshotId`, `reason`) without deleting history |

## Example Data

`cdisc_list_examples` lists curated public examples pinned by revision and SHA-256: `cdisc-pilot-sdtm-dm` (SDTM), `phuse-ffu-send`, `phuse-nimble-send`, `phuse-instem-send`, and `phuse-pointcross-send` (SEND). The data is fetched on demand and not shipped with \{kehrnel\}.

```json
{"exampleId": "cdisc-pilot-sdtm-dm", "acknowledgeTerms": true, "validate": true, "publish": true}
```

`cdisc_ingest_example` fetches, verifies, retains, ingests, and optionally validates and publishes the example. `acknowledgeTerms` must be `true`; use of the data is subject to the upstream terms listed in `examples/catalog.json`. Publication cannot bypass validation. The script `scripts/fetch_cdisc_examples.py <example-id> <directory>` downloads an example locally.

## Synthetic Studies

`cdisc_generate_synthetic_study` builds deterministic, linked study data:

```json
{
  "recipe": {
    "studyId": "SYNTH-DEMO-001",
    "profile": "sdtm",
    "subjects": 100,
    "seed": 20260821,
    "domains": ["DM", "AE", "LB", "VS"],
    "anomalyRate": 0.0
  },
  "packageId": "synthetic-demo-package",
  "snapshotId": "v1",
  "persist": true,
  "validate": true,
  "publish": true
}
```

| Recipe field | Values |
|--------------|--------|
| `profile` | `sdtm` (domains `DM`, `AE`, `LB`, `VS`), `send` (`DM`, `TX`, `MI`, `LB`), `adam` (`ADSL`, `ADAE`, `ADLB`), `tig` (`PROD`, `BATCH`, `EVID`) |
| `subjects` | 1 to 10000 (default 20) |
| `seed` | Integer; the same recipe produces the same data |
| `anomalyRate` | 0 to 0.25; injects declared anomalies that validation reports |
| `scenario` | `baseline` (default) or `safety-signal` (SEND only, requires `DM`, `TX`, `MI`, `LB`) |

Without `persist`, the operation only returns the generated documents. `publish: true` requires `persist: true` and validation; otherwise it fails with `CDISC_SYNTHETIC_PUBLICATION_REQUIRES_VALIDATION`. The built-in recipes are a small teaching model, not a substitute for licensed CDISC metadata or a sponsor's Define-XML.

## Long-Running Jobs

Run large generation or package operations as background jobs:

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/synthetic/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "cdisc",
    "op": "cdisc_generate_synthetic_study",
    "payload": {
      "recipe": {"studyId": "SYNTH-SEND-001", "profile": "send", "subjects": 1000, "seed": 42},
      "persist": true,
      "validate": true,
      "publish": true
    }
  }'
```

Set `op` explicitly; its default is not a CDISC operation. Track jobs with `GET /environments/{env_id}/synthetic/jobs/{job_id}` and cancel with `POST .../cancel`. The activation must have bindings (a `bindings_ref` or plaintext bindings in development). `KEHRNEL_JOBS_RECOVERY=retry` retries interrupted jobs; use it only for idempotent operations. By default, interrupted jobs are marked failed.

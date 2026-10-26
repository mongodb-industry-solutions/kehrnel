---
sidebar_position: 2
---

# CDISC Configuration

Use `src/kehrnel/engine/strategies/cdisc/sdr/defaults.json` as the activation baseline for `cdisc.sdr`. The activation `config` is deep-merged over these defaults and validated against `schema.json`.

## Pack Layout

| Path | Purpose |
|------|---------|
| `manifest.json` | Identity, capabilities, interaction contracts, adapters, operations with input and output schemas |
| `spec.json` | Logical model, profiles, stores and indexes, transform pipeline, query model, visualization |
| `schema.json` | JSON Schema for the activation config |
| `defaults.json` | Default activation config |
| `strategy.py` | Plugin facade (`CDISCSDRStrategy`) over the service packages |
| `ingest/`, `query/`, `analysis/`, `assistant/`, `artifacts/`, `export/`, `validation/`, `synthetic/`, `projections/`, `packages/`, `lineage/`, `repository/` | Focused services |
| `examples/catalog.json` | Curated, checksum-pinned public example data (fetched on demand, not vendored) |
| `profiles/catalog.json` | Profile catalog |
| `deployment/` | Example activation, bindings, query, search, package, and synthetic requests |

## Activation Baseline

```json
{
  "database": "cdisc_sdr",
  "tenant_id": "default",
  "collections": {
    "studies": "cdisc_studies",
    "snapshots": "cdisc_snapshots",
    "datasets": "cdisc_datasets",
    "records": "cdisc_records",
    "entities": "cdisc_entities",
    "materializations": "cdisc_materializations",
    "standards": "cdisc_standards",
    "artifacts": "cdisc_artifacts",
    "validation_runs": "cdisc_validation_runs",
    "validation_findings": "cdisc_validation_findings",
    "validation_waivers": "cdisc_validation_waivers",
    "transformations": "cdisc_transformations"
  },
  "ingest": {
    "default_publication_state": "staged",
    "batch_size": 1000,
    "max_records_per_dataset": 2000000
  },
  "artifact": {
    "max_inline_bytes": 25000000,
    "max_replay_bytes": 25000000
  },
  "validation": {
    "require_before_publish": true,
    "require_registered_standards": false,
    "external_failure_policy": "finding",
    "blocking_severities": ["error"]
  },
  "projections": {
    "rebuild_on_publish": true,
    "max_records_per_rebuild": 250000
  },
  "semantic": {
    "enabled": true,
    "auto_embedding_model": "voyage-4",
    "max_text_chars": 4000,
    "max_fallback_scan": 10000,
    "lexical_index": "cdisc_semantic_text",
    "auto_embed_index": "cdisc_semantic_auto_embed",
    "exclude_variables": ["USUBJID", "SUBJID"]
  },
  "query": {
    "default_limit": 100,
    "max_limit": 1000,
    "extra_allowed_paths": []
  }
}
```

The schema marks `database`, `tenant_id`, and `collections` as required; the defaults supply them, so set `database` explicitly and review it during activation. The schema rejects unknown keys at every level.

## Keys

### Scope

| Key | Default | Description |
|-----|---------|-------------|
| `database` | `cdisc_sdr` | Strategy-owned MongoDB database. It overrides any `database` in the `db` binding |
| `tenant_id` | `default` | Tenant boundary injected into every governed operation, query, and document id |
| `collections.*` | see baseline | Twelve collection names. All twelve are required and must be distinct; duplicates fail with `CONFIG_INVALID` |

### `ingest`

| Key | Default | Description |
|-----|---------|-------------|
| `default_publication_state` | `staged` | `staged` or `published`. Used when an ingest request does not set `publicationState` |
| `batch_size` | `1000` | Record write batch size (1 to 10000) |
| `max_records_per_dataset` | `2000000` | Maximum records accepted in one dataset |

### `artifact`

| Key | Default | Description |
|-----|---------|-------------|
| `max_inline_bytes` | `25000000` | Largest artifact accepted by the inline `cdisc_store_artifact` operation |
| `max_replay_bytes` | `25000000` | Largest artifact returned inline by `cdisc_replay_artifact` |

Larger files use the direct object-store operations (`cdisc_initiate_upload`, `cdisc_finalize_upload`, `cdisc_prepare_download`).

### `validation`

| Key | Default | Description |
|-----|---------|-------------|
| `require_before_publish` | `true` | Only a `validated` snapshot can be published, and ingest with `publicationState: published` is rejected |
| `require_registered_standards` | `false` | Add the `SDR.STANDARDS.UNREGISTERED` error when the snapshot's `standardsPackageId` is not registered |
| `external_failure_policy` | `finding` | `finding` records an `SDR.VALIDATION_ENGINE.FAILURE` error; `raise` fails the operation with `CDISC_VALIDATION_FAILED` |
| `blocking_severities` | `["error"]` | Severities (`error`, `warning`, `info`) that fail a validation run unless waived. Must not be empty |

### `projections`

| Key | Default | Description |
|-----|---------|-------------|
| `rebuild_on_publish` | `true` | Rebuild facets, entity references, entities, materializations, and semantic text when a snapshot is published |
| `max_records_per_rebuild` | `250000` | Upper limit for one synchronous rebuild |

### `semantic`

| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `true` | Write `semantic.text` on records and declare the search indexes in `plan` |
| `auto_embedding_model` | `voyage-4` | Model used by the Atlas Automated Embedding vector index |
| `max_text_chars` | `4000` | Maximum length of `semantic.text` |
| `max_fallback_scan` | `10000` | Records scanned by the bounded portable lexical fallback, used when no Atlas Search adapter is available |
| `lexical_index` | `cdisc_semantic_text` | Atlas Search index name |
| `auto_embed_index` | `cdisc_semantic_auto_embed` | Atlas Vector Search index name |
| `exclude_variables` | `["USUBJID", "SUBJID"]` | Variables left out of `semantic.text` |

The canonical CDISC documents remain authoritative; the semantic projection is optional and rebuildable.

### `query`

| Key | Default | Description |
|-----|---------|-------------|
| `default_limit` | `100` | Page size when a `cdisc-query/v1` request has no `page.limit`. Cannot exceed `max_limit` |
| `max_limit` | `1000` | Largest accepted page size |
| `extra_allowed_paths` | `[]` | Additional query paths. Each must start with `facets.` or `semantic.` and must not contain a segment starting with `$` |

## Bindings

Bindings carry connections and credentials, separate from the reviewed config. The runtime reads these sections:

| Section | Providers | Fields |
|---------|-----------|--------|
| `db` | `mongodb` | `uri`, `database` (overridden by the activation `database`) |
| `artifact` | `filesystem` | `root` (required) |
| `artifact` | `s3` | `bucket`, `prefix`, `region`, `endpoint_url`, `server_side_encryption`, `kms_key_id`, optional access keys |
| `artifact` | `azure` or `azure-blob` | `container`, `prefix`, `connection_string` |
| `validation` | `command` | `argv`, `timeout_seconds`, `max_output_bytes`, `environment` |

The command validator runs an `argv` array with a JSON file protocol; no shell is invoked. Atlas Automated Embedding is declared by the vector index, so no application embedding adapter is needed.

Local development example (from `deployment/resolved-bindings.example.json`):

```json
{
  "db": {
    "provider": "mongodb",
    "uri": "mongodb://localhost:27017",
    "database": "cdisc_sdr"
  },
  "artifact": {
    "provider": "filesystem",
    "root": "/var/lib/kehrnel/cdisc-artifacts"
  }
}
```

When authentication is enabled, activation accepts only a `bindings_ref` resolved by the configured bindings resolver; plaintext `bindings` are allowed only in development with `allow_plaintext_bindings: true`. Keep production bindings in a secret manager and never commit credentials.

## Activation Example

```json
{
  "strategy_id": "cdisc.sdr",
  "version": "0.2.0",
  "domain": "cdisc",
  "config": {
    "database": "cdisc_sdr",
    "tenant_id": "default",
    "validation": {"require_before_publish": true}
  },
  "bindings_ref": "secret://kehrnel/cdisc-sdr/default"
}
```

See [Workflows](./workflows.md) for the activation call and the plan and apply steps.

# CDISC Study Data Repository strategy

`cdisc.sdr` keeps CDISC study datasets authoritative and treats semantic,
openEHR, graph, and vector representations as rebuildable projections.

## Package layout

- `ingest/`: Dataset-JSON and XPT ingestion, Define-XML enrichment, immutable snapshots, publication.
- `query/`: governed `cdisc-query/v1` compilation and execution.
- `analysis/`: governed `cdisc-analysis/v1` grouped metrics with bounded paths and tenant/snapshot scope.
- `assistant/`: read-only guided study questions backed only by governed services and evidence citations.
- `artifacts/`: checksum-addressed source/generated artifact retention and verified replay.
- `export/`: Dataset-JSON regeneration, semantic equivalence, and transformation evidence.
- `validation/`: built-in structural rules, external-engine adapter normalization, and publication gates.
- `synthetic/`: deterministic linked SDTM, SEND, ADaM, and TIG study generation.
- `projections/`: rebuildable profile facets, entity indexes, timelines, findings, traceability, and evidence views.
- `packages/`: preflighted multi-dataset ingestion and deterministic portable package export.
- `lineage/`: transformation evidence, lineage inspection, and immutable snapshot supersession.
- `repository/`: tenant-scoped study, snapshot, dataset, validation, standards, and artifact discovery.

## Runtime bindings

The initial deployment uses MongoDB for documents and a local filesystem for
artifact bytes. S3-compatible and Azure Blob adapters implement the same
immutable protocol for production deployments.

Set the strategy-owned database explicitly in activation config, for example
`{"database": "tenant_cdisc"}`. The environment binding below supplies the
connection and credentials; its database value, when present for legacy
clients, is overridden by the reviewed activation database.

```json
{
  "db": {
    "provider": "mongodb",
    "uri": "mongodb://localhost:27017",
    "database": "kehrnel"
  },
  "artifact": {
    "provider": "filesystem",
    "root": "/var/lib/kehrnel/cdisc-artifacts"
  }
}
```

Optional secure bindings can add a command-based validator. Commands use an
argv array and a JSON file protocol; no shell is invoked. Atlas Automated
Embedding is declared by the strategy's vector-search index and does not need
an application embedding adapter. See
`deployment/resolved-bindings.production.example.json`.

Use `cdisc_store_artifact` before `cdisc_ingest_xpt`. A normal governed flow is:

1. retain XPT and Define-XML artifacts;
2. ingest every dataset into a staged snapshot;
3. run `cdisc_validate_snapshot`;
4. publish the single snapshot marker;
5. rebuild profile entities/materializations (automatic on publication);
6. query through `cdisc-query/v1` or lexical/vector/hybrid search;
7. run bounded grouped analysis or ask the read-only evidence-cited assistant;
8. export Dataset-JSON, verified XPT, or a deterministic package ZIP;
9. inspect the complete validation and transformation lineage.

The Workbench discovery contract is exposed through `cdisc_list_studies`,
`cdisc_list_snapshots`, `cdisc_snapshot_summary`, `cdisc_list_datasets`,
`cdisc_list_validation_runs`, `cdisc_list_standards`, and `cdisc_list_artifacts`.
Lists are tenant-scoped and return opaque bounded pagination cursors. For large S3 artifacts use
`cdisc_initiate_upload` followed by `cdisc_finalize_upload`; use `cdisc_prepare_download` for a
short-lived signed GET. Filesystem deployments retain the bounded inline store/replay operations.

The optional dependency `kehrnel-core[cdisc]` installs `pyreadstat` and pandas
for XPT import/export.
`kehrnel-core[cdisc-cloud]` adds S3 and Azure Blob clients.
Run `scripts/fetch_cdisc_examples.py <example-id> <directory>` to fetch a
checksum-pinned public example without adding the source data to this
repository. The curated catalog contains the official clinical CDISC Pilot and
five public MIT-licensed PhUSE SEND studies, including larger pathology,
recovery-cohort, and cross-domain safety-assessment examples. Use remains
subject to the upstream terms linked in `examples/catalog.json`.

Synthetic generation follows the same governance principle as openEHR
template-driven generation, with a CDISC-specific source of truth: a pinned
profile, implementation guide, dataset variable metamodel, and controlled
terminology. The generated Dataset-JSON carries the generator and recipe
identity; ingestion pins the chosen standards package and persists the current
`modelSchemaVersion` on every repository document. The built-in recipes are a
small executable teaching model, not a replacement for licensed CDISC metadata
or a sponsor's Define-XML.

Persisted row documents are sparse: optional null, blank, empty-array, and
empty-object values are omitted. Dataset variables and order remain in the
dataset metamodel, and the immutable source artifact remains retained, so
Dataset-JSON/XPT export can reconstruct missing cells without inflating every
MongoDB row.

## Operational boundaries

Kehrnel ships structural and cross-dataset integrity rules plus an adapter for
external engines. CDISC CORE, Pinnacle 21, regulator rule sets, controlled
terminology, and licensed standards content must be supplied and pinned by the
deployment owner. Passing built-in checks alone is not represented as
regulatory conformance.

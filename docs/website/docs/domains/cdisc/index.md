---
sidebar_position: 3
---

# CDISC Domain

This section centralizes CDISC study-data documentation for \{kehrnel\}.

The CDISC domain is served by a single official strategy, `cdisc.sdr` (Document-First CDISC Study Data Repository). An environment uses it through an activation with `domain: cdisc` and `strategy_id: cdisc.sdr`.

## Scope

The domain covers four CDISC profiles. The profile is a discriminator on every snapshot, dataset, and record (`send`, `sdtm`, `adam`, `tig`).

| Profile | What the code models | Dataset examples in the spec | Rebuildable view |
|---------|----------------------|------------------------------|------------------|
| SEND | Nonclinical studies: animals, treatment groups, test articles, specimens, organs, findings | `DM`, `TX`, `MI`, `LB` | `nonclinical-finding` |
| SDTM | Clinical tabulation: subjects, arms, epochs, visits, interventions, events, findings | `DM`, `AE`, `LB`, `VS` | `subject-timeline` |
| ADaM | Analysis datasets with parameters, populations, derivations, and links back to source | `ADSL`, `ADAE`, `ADLB` | `analysis-traceability` |
| TIG | Product and evidence overlays: products, batches, constituents, evidence items | `PROD`, `BATCH`, `EVID` | `product-evidence` |

Exchange formats handled by the code:

| Format | Support |
|--------|---------|
| Dataset-JSON | Version 1.1 only (`datasetJSONVersion` must match `1.1` or `1.1.x`). Inspect, ingest, and export with semantic equivalence checks |
| SAS XPORT (XPT) | Ingest from a retained artifact; export as XPT v5 or v8 with read-back verification. Requires the `cdisc` extra (`pyreadstat`, `pandas`) |
| Define-XML | Parsed from a retained artifact to expose dataset metadata and to enrich XPT ingestion |

### Domain library

`src/kehrnel/engine/domains/cdisc/` is a storage-neutral library used by the strategy:

| Module | Purpose |
|--------|---------|
| `models.py` | Canonical models: `CanonicalSnapshot`, `CanonicalDataset`, `CanonicalRecord`, `StandardReference`, `StandardsPackage`, `ArtifactReference`, `ValidationRun`, `ValidationFinding` |
| `dataset_json.py` | `parse_dataset_json`, `canonicalize_dataset_json` |
| `define_xml.py` | `parse_define_xml` |
| `xpt.py` | `xpt_to_dataset_json`, `dataset_json_to_xpt` |
| `exchange.py` | `export_dataset_json`, `compare_export_to_canonical` (semantic equivalence report) |
| `projection.py` | `derive_facets`, `derive_entity_refs`, `project_record` |
| `query.py` | The `cdisc-query/v1` model (`CdiscStudyQuery`) and its MongoDB compiler `compile_study_query` |

### Standards and licensing

\{kehrnel\} ships structural and cross-dataset integrity rules and an adapter for an external validation command. CDISC CORE, Pinnacle 21, regulator rule sets, controlled terminology, and licensed standards content are supplied and pinned by the deployment owner. Passing the built-in checks is not presented as regulatory conformance.

## API Surface and Contracts

There is **no CDISC domain router**. The API exposes no `/api/domains/cdisc` routes, and there is no CDISC-specific strategy router under `/api/strategies`. All CDISC work goes through the universal runtime endpoints, which dispatch to the `cdisc.sdr` plugin:

| Endpoint | Use for CDISC |
|----------|---------------|
| `POST /environments/{env_id}/activate` | Activate `cdisc.sdr` with `domain: cdisc` |
| `POST /environments/{env_id}/plan` and `/apply` | Create collections, B-tree indexes, and the optional Atlas Search and vector indexes |
| `POST /environments/{env_id}/query` | Run a `cdisc-query/v1` query (body: `domain` plus `query`) |
| `POST /environments/{env_id}/compile_query` | Compile a `cdisc-query/v1` query and return the MongoDB pipeline and explain without running it |
| `POST /environments/{env_id}/activations/cdisc/ops/{op}` | Run one strategy operation, for example `cdisc_ingest_dataset_json` |
| `POST /environments/{env_id}/run` | Universal executor; accepts the same runtime and strategy operations |
| `POST /environments/{env_id}/synthetic/jobs` | Run a long operation as a background job (body: `domain`, `op`, `payload`) |
| `GET /environments/{env_id}/capabilities` | List the operations available in the environment |
| `GET /ops` | Operation catalog for all loaded strategies |
| `GET /strategies/cdisc.sdr` and `/strategies/cdisc.sdr/spec` | Strategy manifest and spec |

All routes are also mounted under `/v1`. See [Runtime Capabilities And Run Endpoints](/docs/api/endpoints/runtime-capabilities-and-run) and [Strategy Synthetic And Job Endpoints](/docs/api/endpoints/strategy-synthetic-and-jobs).

### Interaction contracts

The `cdisc.sdr` manifest declares four interaction contracts:

| Contract id | Mode | Authority | Contract | Operations or builders |
|-------------|------|-----------|----------|------------------------|
| `cdisc.dataset-exchange` | native | CDISC | Dataset-JSON v1.1 API v1.0 and SAS XPORT | `cdisc_inspect_dataset_json`, `cdisc_ingest_dataset_json`, `cdisc_export_dataset_json`, `cdisc_ingest_xpt`, `cdisc_export_xpt` |
| `cdisc.study-query` | compiled | \{kehrnel\} | `cdisc-query/v1` | Builder `cdisc_study_query_v1`, engine `mongo_pipeline` |
| `cdisc.study-analysis` | compiled | \{kehrnel\} | `cdisc-analysis/v1` | `cdisc_run_analysis` (builder `cdisc_analysis_v1`) |
| `cdisc.operational-evidence` | hybrid | \{kehrnel\} | CDISC Solution Evidence Package | `cdisc_rebuild_projections`, `cdisc_browse_projections`, `cdisc_run_analysis`, `cdisc_export_solution_evidence`, `cdisc_get_lineage` |

The exchange contract follows the CDISC standards. CDISC does not define an operational research query interface, so the query and analysis contracts are \{kehrnel\} contracts that compile to bounded MongoDB pipelines and keep CDISC semantics.

## Quick Navigation

### Strategy

- [CDISC Study Data Repository](/docs/strategies/cdisc/sdr/)
- [Configuration](/docs/strategies/cdisc/sdr/configuration)
- [Data Model](/docs/strategies/cdisc/sdr/data-model)
- [Workflows](/docs/strategies/cdisc/sdr/workflows)

`cdisc.sdr` manifest: version `0.2.0`, maturity **preview**. Operations and payloads can still change between releases.

### Core CLI Entry Points

- [CLI Overview](/docs/cli/overview)
- [Domain CLI Layer](/docs/cli/domains)
- [Strategy CLI Layer](/docs/cli/strategies)

CDISC has no dedicated command group. Use the generic runtime commands:

| Command | Purpose |
|---------|---------|
| `kehrnel core env activate --env dev --domain cdisc --strategy cdisc.sdr --config activation.json` | Activate the strategy |
| `kehrnel op list --domain cdisc --strategy cdisc.sdr` | List CDISC operations |
| `kehrnel op schema <operation> --strategy cdisc.sdr` | Show the payload schema of one operation |
| `kehrnel core env op <operation> --env dev --domain cdisc --payload payload.json` | Run one operation |
| `kehrnel run <operation> --env dev --domain cdisc --payload payload.json` | Run any runtime or strategy operation |
| `kehrnel core env query --env dev --domain cdisc --payload query.json` | Run a `cdisc-query/v1` query |
| `kehrnel common validate-pack src/kehrnel/engine/strategies/cdisc/sdr` | Validate the strategy pack |

## Recommended Reading Order

1. strategy introduction
2. configuration and activation
3. workflows (artifacts, ingest, validate, publish, query, export)
4. data model

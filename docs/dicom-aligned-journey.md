# DICOM: one accelerator, four environments

The public experience and connected evaluation use `dicom.tiered_store`. They share source normalization, projection profiles, publication evidence and native MongoDB query compilation. Version 0.4.0 adds persisted domain-sample operations and the aligned application entry points. The strategy remains a preview: the small demo does not qualify 15 billion instances.

| Layer | Audience | Data and authority | Implemented surface |
|---|---|---|---|
| Public showcase | Sellers, product owners, architects | Pinned licensed public dataset; server owns its dedicated activation | Real images; modality/manufacturer filters execute Kehrnel → MongoDB; evidence shows actual pipelines |
| Builder | Architects | Core source/category/strategy catalogs and authored architecture | Imaging category recommends DICOM; reference architecture and connected workspace entry; canonical production package readiness remains gated |
| Connected workspace | Developers | User-selected environment, its authorized centers and storage bindings | Capabilities, sample discovery/import, rule editing, source preview, reception/consolidation, regular indexes and generation/sample queries |
| Python accelerator | Application teams | Caller supplies StrategyContext and adapters | Same manifest operations through StrategyHandle; HTTP run-operation contracts and SDK example |

## Domain samples

Samples live under `engine/domains/dicom/samples`, independently of the persistence strategy. Sample import is optional; activating a customer environment does not import demo data. HTTP callers select registered dataset IDs, versions, variants and verified digests. They cannot choose a filesystem root, database, tenant or binary bucket through the sample operation.

`dicom_sample_import` uses native catalog ingestion and rule reception/consolidation. An immutable receipt links a dataset version, variant, projection profile and ready generation. `dicom_sample_status` and `dicom_sample_query` enforce activation tenant and center access. Query results identify the dataset, actual MongoDB pipelines and bounded result coverage.

## Public deployment

Configure Healthcare Data Lab with `KEHRNEL_DICOM_SHOWCASE_URL` and the server-side `KEHRNEL_DICOM_SHOWCASE_API_KEY` where required. Provision a dedicated authenticated Kehrnel activation named `dicom-showcase-public`, import the four public-imaging variants with the `public-imaging-core` profile, and verify the pinned dataset digest. Public HTTP accepts only bounded modality and manufacturer selectors; it exposes no import or arbitrary operation endpoint.

`examples/dicom/serve_showcase.py` is a loopback development fixture using a local MongoDB database. It loads no credential files. It is not a production server configuration. The website shows unavailable query service explicitly and keeps image assets accessible; it never silently substitutes recorded results.

## Evaluation workflow

1. Connect an environment and activate the DICOM preview through the existing strategy controls.
2. Inspect computed capabilities and authorized centers. Optionally discover/import domain samples.
3. Inspect original study and instance metadata. Edit typed fields, distinct/common/range aggregations and same-instance groups in the profile.
4. Preview collections, then receive a bounded batch and consolidate its unique SOP identities.
5. Apply generated regular indexes. Inspect generation coverage before querying.
6. Compile explicit rule-aware queries to MQL and execute on selected ready generations.

Changing a profile changes projection meaning. Existing generations stay immutable. Sample receipts do not constitute a published complete customer corpus. Historical data lacking headers remains explicitly incomplete.

## Remaining production work

* Resumable bounded ingestion worker, leases/recovery monitoring and correction semantics at operational throughput.
* Selective historical/object-storage backfill execution with checkpoints, budgets and cancellation; current operation produces a plan.
* Atlas Search readiness polling and execution; generated mappings/index requests alone are not execution evidence.
* Production canonical implementation-package activation for this preview, subject to existing approval/readiness gates.
* CDR identity/evidence joins and clinical cohort semantics; imaging tags do not infer diagnoses.
* Large-scale partition/shard routing, benchmark corpus and load/cost qualification.
* Connected synthetic mapping authoring beyond the existing native generator and registered domain samples.

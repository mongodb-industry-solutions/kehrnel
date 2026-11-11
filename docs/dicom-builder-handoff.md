# DICOM builder and Healthcare Data Lab handoff

Kehrnel supplies the DICOM metadata starter assets and an executable source-aware compilation example. Canonical read-only metadata resolution is available for explicit evaluation. Version 0.3.0 also supplies the native rule/reception playground; an isolated HDL worktree implements `/healthcare/dicom`. See [dicom-rule-playground.md](dicom-rule-playground.md) for its actual operations, limits and remaining production work. Canonical package activation and the Builder's complete DICOM journey remain implementation work.

## Assets and discovery

| Asset | Identifier | Purpose |
| --- | --- | --- |
| Source profile | `exchange.dicom-metadata-json` | Customer-compatible study/instance wrappers containing DICOM JSON headers |
| Source category | `healthcare.imaging` | Product-neutral imaging discovery; exact profile reference |
| Starter template | `template.healthcare.dicom-metadata-starter` | Metadata viewer and read-only agent tool consumptions |
| Reference architecture | `architecture.healthcare.dicom-metadata` | Connects source, outcome, template and strategy references |
| Strategy | `dicom.tiered_store` 0.3.0 | Catalog filtering, selected headers, bounded rule intake/generations and partial research |

Always obtain versions/digests from the current catalog. Do not hard-code a digest in HDL. The starter selects no provider or region and includes no credentials.

## Kehrnel bridge journey

1. Read `/v1/source-categories`, `/v1/source-profiles` and `/v1/outcome-catalog`; select the DICOM reference architecture by identifier.
2. Send its exact `artifactRef` to `POST /v1/reference-architectures/architecture.healthcare.dicom-metadata/instantiate` with a blueprint identifier and revision. Review the intake, identity, preview and deployment assumptions.
3. The existing endpoint derives the internal source-aware representation. `POST /v1/architecture-proposals/compile` evaluates that representation. The candidate remains `requires_approval` with a preview issue even though its scoped metadata queries can compile. No source ingestion or environment activation occurs.
4. For an explicit preview demonstration, configure resolver-backed adapters and activate the existing runtime using `examples/dicom/activation.json`. Provide `bindings_ref` for the configured resolver; the bare recipe deliberately has no binding and activation rejects it. The recipe is separately reviewed configuration, not an automatically generated canonical Implementation Package.
5. Request `dicom_query_catalog` from the active environment. Populate fields, examples, operators, authorized centers and limits from this response. Use `dicom-query/v1` with `/compile_query` and `/query`, and `dicom_get_headers` with `/run`.

Run `PYTHONPATH=src python examples/sdk/dicom_builder_journey.py` to inspect the non-activating derivation, proposal and runtime recipe. `examples/sdk/dicom_query_journey.py` demonstrates the bound query execution journey; the metadata demonstration sequence is in `examples/dicom/README.md`.

## Canonical metadata resolution and remaining integration

The canonical architecture contract is `solution-blueprint/v1`, with participants, interactions, semantic authorities and data postures. Run `examples/sdk/dicom_canonical_builder_journey.py` for the canonical DICOM evaluation request. It obtains exact references from `/v1/architecture-resolution-inputs` and compiles through `/v1/architecture-resolutions/compile` with `deliveryLevel: customer_evaluation`. Public demonstrations may select `public_demonstration`. Omitting this field retains the conservative production default and blocks preview DICOM.

The resolver accepts only request/response `dicom-query/v1` metadata query/read interactions with the exact installed preview strategy and published native binding. It produces linked resolved, preparation and runtime plans, with no intake stages and no implicit Search. The target must declare `mongodb.indexed_queries`. Reference target evidence is not observed tenant state. Active environment authorization, collection bindings and field policies remain required for actual queries.

The resulting candidate remains `requires_approval`, with canonical package activation explicitly unsupported. Package building rejects it. Add bounded intake/header contracts and compatible preparation/activation in the next increment; no metadata plan implies pixels, DICOMweb or complete clinical cohorts.

The existing catalog instantiation bridge still yields `legacy-source-blueprint/internal`. HDL must not silently relabel it as canonical or treat bridge compilation as package approval. The canonical starter is currently a separate executable SDK example, not the return value of the reference-architecture instantiation endpoint. Connect that endpoint after the remaining package lifecycle is implemented.

## H1 metadata demonstration

The direct native-runtime playground now implements original headers/ContentIndex inspection, editable extraction rules, embedded summary and series sidecar previews, generated regular/Search mappings, bounded rule-aware queries, and authenticated reception/rebuild controls. Public mode accepts only synthetic presets. It does not implement the canonical package/activation journey or historical worker. The earlier viewer navigation and activation screens below remain separate integration tasks.

Build these HDL screens in a separate checkout:

- Activation/configuration with visible preview status and deployment bindings.
- Patient/day, modality/date and exact/prefix description selection from catalogs.
- Study-to-series-to-instance navigation with bounded pages and numeric instance order.
- Selected-header inspection, available/not-available/deferred state, VR fidelity and retry handling.
- Research example selection from active promoted fields, with explicit partial coverage and instance result grain.
- Inspection of compiled query/routing, without letting the UI construct arbitrary MongoDB pipelines.

Show asynchronous research field promotion and publication as later capabilities until K2 implements project jobs and frozen generations. A ready index alone does not prove complete coverage. No condition codes are inferred from acquisition metadata, and no metadata-only screen should imply pixel rendering.

## Provider qualification

The algorithm and contracts are common across providers. Filesystem, S3 and Azure are the current artifact adapter options; native GCS is future adapter work. Measure concurrent transfers, request/egress costs, retention, restore latency, encryption/authentication, range reads and residency in the selected environment. Azure-specific customer deployment settings belong in a deployment profile, not in shared query schemas or builder templates.

## Acceptance evidence

`tests/contract/test_dicom_builder_journey.py` verifies exact discovery links, provider-neutral assumptions, deterministic preview compilation, implicit-preview rejection, stale source rejection, honest Search readiness and an HTTP catalog-to-runtime handoff. It compiles the nine packaged queries after a separate explicit activation, without a database or object store. K1 already contains real MongoDB execution evidence for the query examples. Neither proves canonical package activation, actual HDL UI behavior or 15-billion-instance scale.

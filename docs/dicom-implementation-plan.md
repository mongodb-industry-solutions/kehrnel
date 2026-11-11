# DICOM implementation plan: Kehrnel and Healthcare Data Lab

This plan advances `dicom.tiered_store` toward the architecture in **DICOM migration and research strategy for 15 billion instances**. Kehrnel owns persistence, query compilation, authorization, metadata retrieval and projection contracts. Healthcare Data Lab consumes those contracts to build the viewer and research journeys. The strategy remains a preview until representative sharded and object-storage validation passes.

## Delivery baseline and first increment

Current increment: **0.3.0** adds the native rule/reception playground described in [dicom-rule-playground.md](dicom-rule-playground.md). It previews embedded per-series summaries, selected series research rows, generated indexes and rule-aware queries; persists and recovers bounded intake generations. The separate HDL worktree implements `/healthcare/dicom`. Large-series streaming, historical backfill execution, Atlas Search readiness/execution and canonical Builder activation remain pending. The 0.2.0 evidence below records the earlier increment and is not rewritten as new validation.

The existing baseline preserves customer study/instance source records, derives a flat series catalog, writes arriving instances independently, refreshes counts separately, and supports inline or immutable bundled metadata. It exposes one native strategy through SDK, HTTP and CLI.

The first increment is version **0.2.0**. It adds a query catalog for builders/agents, multiple promoted-tag predicates, a bounded header-batch interface, nine executable synthetic examples, and an SDK journey. Validation status is recorded below; these features do not establish 15-billion-instance throughput.

| Work package | Deliverable | Dependency | Acceptance evidence | Status |
| --- | --- | --- | --- | --- |
| K1 Querying interfaces and examples | Query catalog, typed AND predicates, selected header batches, shared example pack and SDK journey | Existing metadata preview | Real MongoDB semantics plus SDK/HTTP/CLI contracts; no broad raw-header scan | Implemented and validated |
| K1B Kehrnel catalog and builder bridge | DICOM source profile, immutable starter template, reference architecture and explicit preview runtime handoff | K1 | Content-addressed assets, deterministic source-aware compilation, HTTP discovery and query handoff | Kehrnel implemented and validated. Canonical HDL Builder integration remains H1 work |
| K1C Canonical metadata resolution | DICOM participant/interaction starter, exact native binding and evaluation-only query plans | K1B | Canonical SDK/HTTP resolution, explicit preview pins, unsupported-interaction and package guards | Implemented; canonical package activation and HDL screens remain pending |
| K2 Published research projections | Project-scoped builds, correct result grain, immutable generation receipts and bounded cohort pages | K1; durable metadata source | Series/study/patient deduplication, explicit correlation, coverage and reproducibility | Bounded series generations implemented in 0.3.0; archive/project publication pending |
| K3 Continuous ingestion and migration | Resumable source deltas, durable dirty-series handoff, coalesced summaries, correction/delete/relink reconciliation | Existing ingestion contract | Retry/crash recovery, late arrivals, no shared catalog write per instance, preload/delta cutover rehearsal | Bounded immutable inbox/retry/consolidation implemented in 0.3.0; production worker and corrections pending |
| K4 Object storage and historical metadata | Measured bundles/cache, stable resolver, bounded historical series manifests and scan-friendly exports | K3; artifact adapter | Warm/cold hydration, integrity, rehydration, lifecycle and cost comparison | Planned |
| K5 Production scale qualification | Deployment-specific shard plans, routing, uniqueness, skew and cost evidence | K2–K4; representative customer distributions | Viewer under ingest/backfill load, targeted plans, throughput and cost curves | Planned |
| K6 Clinical cohort composition | Issuer-aware subject links and provenance-linked condition assertions | K2; clinical evidence source | Diagnosis/indication/finding distinction, terminology release and temporal semantics | Planned |
| H1 Metadata demonstration | Strategy activation, query builder, examples, header inspector and progressive import state | K1; K1B; canonical integration for public Builder activation | Uses Kehrnel contracts; shows real result grain and partial coverage; synthetic data only | Native-runtime playground implemented in isolated HDL worktree; canonical Builder integration pending |
| H2 Research project journey | Field/grain selection, estimate, build progress, published generation and cohort export | K2 | No false readiness from index status; frozen coverage receipts and cancellation | Planned |
| H3 Public imaging application | Licensed pixel dataset, DICOMweb adapter, viewer and linked metadata examples | H1; retrieval adapter | License/checksums, medical imaging geometry and pixel delivery validated | Planned |

Each work package should have one writer in its own checkout. Keep shared contracts in Kehrnel; HDL should not generate arbitrary database pipelines or maintain a second tag/operator registry.


## Provider-neutral deployment

The storage model, independent instance ingestion, study/series indexes, deferred header hydration and selective research projections apply across hyperscalers. Azure is the customer's deployment profile. It is not a dependency of the DICOM model or query interface.

The existing artifact adapter contract provides `put`, `get` and `stat` for opaque metadata bytes. Runtime bindings choose filesystem, S3 or Azure placement. Google Cloud is a valid deployment target for the architecture, but a native GCS adapter has not been implemented or validated here. Do not equate a portable contract with identical provider capabilities.

Qualify each deployment's request charges, transfer/egress costs, restore delays, range-read support, retention/deletion policy, residency, encryption and authentication. Cross-cloud database/object-store placement can change both latency and cost. An archive or Atlas Infinite backend is an optional storage capability with its own support matrix; it must not be assumed to provide viewer latency or Atlas Search.

## K1B: Kehrnel catalog and builder bridge

The new immutable assets are `exchange.dicom-metadata-json`, `template.healthcare.dicom-metadata-starter` and `architecture.healthcare.dicom-metadata`. Their exact references connect the imaging source category to the DICOM outcome and installed 0.2.0 strategy. The source profile describes the customer-compatible metadata wrappers; it does not declare a SQL Server migration connector, raw binary ingestion or DICOMweb conformance.

The source-aware compiler recognizes a `dicom` target and consumption. Evaluation must explicitly prefer `dicom.tiered_store`. The compiled candidate retains `PREVIEW_STRATEGY_SELECTED` and `requires_approval`; the portfolio remains `preview`, with no production support promotion. The access planner declares direct document access and regular targeted indexes. A full-text requirement still reports implementation work because Atlas Search query execution is unavailable.

`examples/sdk/dicom_builder_journey.py` derives and compiles the starter without a database or object store. The HTTP contract test follows source/category discovery, outcome discovery, reference-architecture instantiation, proposal compilation, then a separate explicit preview activation using `examples/dicom/activation.json`. It discovers active query capabilities and compiles all nine examples. Compilation itself never persists an activation.

The existing reference-architecture endpoint currently derives `legacy-source-blueprint/internal` through the source-aware bridge. This is an internal migration representation, not the canonical `solution-blueprint/v1` participant/interaction model. K1B does not add a parallel public Blueprint contract or claim canonical implementation-package readiness. H1 must map the metadata journey to canonical participants, interactions and preview activation policy before that Builder path can ship. HDL UI execution has not been tested in this checkout.

The HDL handoff is documented in `docs/dicom-builder-handoff.md`. K1B evidence is recorded in `docs/validation/dicom-builder-interface-report.json` after validation. Research project publication, full-corpus cohorts, pixels and 15-billion-instance performance remain their separate milestones.

## K1C: canonical metadata query resolution

The canonical `solution-blueprint/v1` contract now recognizes the `dicom` interface with protocol version `dicom-query/v1`. `GET /v1/architecture-resolution-inputs` publishes an exact installed strategy reference and a native DICOM metadata execution binding. Its authority is the Kehrnel application metadata contract, not a DICOMweb conformance statement or a clinical condition model.

`ArchitectureResolutionRequest.deliveryLevel` defaults to `production_implementation`. DICOM preview compilation requires explicit `customer_evaluation` or `public_demonstration`, the exact installed version/digest in `preferredFoundationRefs`, and the exact published semantic binding. The selected plan remains `requires_approval`; this review status is a product contract, not a request for confirmation during local implementation.

`examples/sdk/dicom_canonical_builder_journey.py` produces a canonical request and three linked resolution artifacts without activation. It models catalog selection, single-series instance browsing and partial promoted metadata queries, with separate pixel delivery. The HTTP compile endpoint returns the same artifacts. Runtime families identify `dicom_query`, the native schema digest, `mongo_pipeline`, active authorization/binding requirements and configuration-driven query discovery. Regular indexed-query capability must be declared by the target; Atlas Search is not required.

This increment resolves read-only metadata interactions. It does not generate ingestion preparation stages, Search maintenance, published data-product compositions or pixel routes. DICOMweb protocols, Search/export/write interactions and modified bindings fail closed. `POST /v1/implementation-packages/build` still rejects the preview plan; canonical package preparation/activation requires a subsequent reviewed integration. Existing native activation remains the route for executing the demonstrated queries.

Next, integrate bounded intake and selected-header interactions with their actual operation schemas, qualified deployment bindings and an explicit evaluation package lifecycle. Then connect the canonical starter to reference-architecture instantiation and HDL Builder screens. Keep the legacy source-aware bridge labeled until the canonical endpoint replaces its output; never relabel it silently.

The Spanish shareable strategy and generated collection/query examples are maintained in `docs/dicom-strategy-es.md`. K1C validation is recorded in `docs/validation/dicom-canonical-resolution-report.json`.

## K1: querying interfaces

### Discovery

`dicom_query_catalog` returns the active `dicom-query/v1` schema, resource-specific filters, result grain, promoted fields/types/operators, authorized centers, limits, header-batch input schema and packaged examples. It reads configuration and trusted authorization context, not the database. Resource enablement is authoritative for the activation; example data availability is explicitly not checked.

Data Lab should refresh discovery after activation or projection-policy changes. A disabled example may require a research policy or a permitted center. Do not confuse enabled query syntax with populated metadata.

### Viewer selection

Keep patient/day, date/modality and description selection on study/series catalogs. Instance browsing requires a single `seriesUid` and a bounded keyset page. Numeric InstanceNumber is a browsing order; DICOM geometry and enhanced-frame dimension ordering remain the viewer's responsibility.

Exact description matching remains the default. Prefix and study selection by series description require one source day in this preview. Both use regular indexes. Wider historical prefix support requires a measured query plan.

### Research predicates

`filter.tag` remains compatible. `filter.tags` adds up to 16 distinct promoted tags with AND semantics. Each tag/value or numeric range is correlated within one attribute element; all predicates belong to the same SOP-instance projection. Combining two instances that separately satisfy the predicates is not a match.

```json
{
  "version": "dicom-query/v1",
  "resource": "research",
  "allowPartial": true,
  "filter": {
    "modality": "MG",
    "fromDay": "2026-01-01",
    "toDay": "2027-01-01",
    "tags": [
      {"path": "00080070", "equals": "SYNTHETIC_MANUFACTURER"},
      {"path": "00180050", "gte": 1, "lt": 2}
    ]
  }
}
```

String tags use exact equality. Numeric tags support equality, inclusive lower and exclusive upper bounds. Combine bounds for one tag in one predicate. Tags and the singular tag cannot be mixed. Unpromoted tags, duplicate paths, arbitrary operators and nested sequence predicates are rejected.

Research rows are instances with study/series references. They are not distinct studies or complete cohorts. All preview research requires `allowPartial:true`; absent matches do not prove absence of a condition or attribute.

### Header hydration

`dicom_get_headers` accepts one series UID and up to 100 distinct SOP UIDs. Select up to 32 top-level DICOM tags, or omit tags to retrieve each full header. ContentIndex is opt-in. Selected attributes preserve their VR and complete source value; missing tags are reported, not invented.

The full result is bounded by `headers.max_response_bytes` (default 4 MiB, measured as BSON; JSON transport may be larger). Available, not-available and deferred records retain request order. Retry deferred SOP UIDs with fewer records or narrower tag selection. An individual attribute is never truncated to make it fit. Missing/unimported and unauthorized instance selections do not reveal distinct availability states.

```json
{
  "seriesUid": "2.25.2001",
  "sopUids": ["2.25.100001", "2.25.100002"],
  "tags": ["00080070", "00180050", "00200013"],
  "includeContentIndex": false
}
```

This operation hydrates authorized retained metadata. It is not pixel delivery or DICOMweb conformance. It does not change canonical source records or add indexes.

## One contract across SDK, HTTP and CLI

The SDK journey is in `examples/sdk/dicom_query_journey.py`. Compilation can run without a database. Bound execution uses `StrategyContext`, `StrategyHandle.dispatch`, storage/index administration and an artifact adapter for bundle mode.

```sh
PYTHONPATH=src python examples/sdk/dicom_query_journey.py
kehrnel run dicom_query_catalog --env imaging --domain dicom
kehrnel run dicom_query --env imaging --domain dicom --payload examples/dicom/queries/manufacturer-thickness.json
kehrnel run dicom_get_headers --env imaging --domain dicom --payload examples/dicom/headers.json
```

HTTP uses the existing environment runtime, with resolver-backed database bindings configured separately:

```text
POST /v1/environments/{env}/activate
POST /v1/environments/{env}/compile_query
POST /v1/environments/{env}/query
POST /v1/environments/{env}/run
```

For example, call `/run` with `{"domain":"dicom","operation":"dicom_query_catalog","payload":{}}`. Call `/query` or `/compile_query` with `{"domain":"dicom","query":<dicom-query/v1 request>}`. Header batches use `/run` with operation `dicom_get_headers` and the header payload above. Read the operation result from the runtime's `result` envelope; query endpoints retain their existing runtime response contract.

Existing `dicom_query`, `dicom_get_header` and source schemas remain compatible. The strategy version advances to 0.2.0; the query contract remains v1 because the predicates are additive. There is no new instance Search index.

## K2: research publication and correct grain

Prioritize this after K1. A manufacturer/date query usually needs series rows and distinct study references. Reserve instance projections for predicates that require instance evidence. Do not expand the preview's instance sidecar across all history merely because it is available.

1. Define a project contract: authorized source scope, requested fields and VR conversion, predicate correlation, result grain, source revision policy, retention and expected coverage.
2. Estimate catalog candidates, metadata scan bytes, output rows, embedded Search objects and build cost. Enforce bounded jobs/leases/checkpoints rather than loading a complete corpus into one process.
3. Extract into unpublished project/generation partitions. Preserve value variation and source evidence; do not promote the first instance's value as a series-wide truth.
4. Record eligible, processed, unavailable, failed and deleted records. Coverage must be tied to source snapshots and replay positions; index READY is insufficient.
5. Publish an immutable generation receipt only after its declared boundary is reconciled. Never promote a partial generation to a complete corpus implicitly.
6. Query and export distinct series/studies/center-scoped patients against published materializations with bounded keyset pages. Bind cursors to generation, policy and authorization.
7. Add Atlas Search execution only where a workload earns it; use static mappings and same-item operators. Keep normal equality/range predicates on regular indexes where they meet the measured workload.

Acceptance includes adversarial mixed-modality/vendor series, values varying across instances, same-instance versus same-series predicates, duplicated subjects across centers, concurrent source changes, cancellation/retry, deletion, stale projections and repeatable cohort export. Nested DICOM sequence items require occurrence-aware paths before support is advertised.

## K3–K5: production ingestion, storage and scale

Retain independent SOP writes. Build a durable event/outbox handoff and a dirty-series queue with coalescing, one fenced refresher per series, bounded cadence and periodic reconciliation. Keep authoritative source counts separate from imported SOP counts and image/frame counts. Quiet time is a scheduling hint, not proof of finalization.

Pre-stage SQL catalogs and establish resumable per-partition delta capture. The overnight window should drain the final delta and switch the application after reconciliation. Detailed headers and binary copies remain progressive; a missing header does not block catalog discovery.

Compare all-history live locators with a recent-instance cache plus bounded historical series manifests. Keep study/series catalogs online in both profiles. Measure bundle grouping, object requests, cold hydration and scan-friendly metadata exports; signed URLs are generated at retrieval time from stable references.

Benchmark collection-specific shard hypotheses, required routing fields and UID uniqueness. Test busy/large series, three/twelve-series studies, real CT/MR/Enhanced metadata, correction bursts, migration throughput and concurrent research builds. A hashed series key does not split one hot series.

The 15-billion scale remains a qualification target. A 1 KB locator is about 15 TB before indexes and replicas; eight million arrivals/day averages about 93/sec; a seven-day metadata backfill needs about 24,800/sec before extraction/retries/index work. These are sizing arithmetic, not measured performance. Record costs per million locators/projection rows, successful viewer opening and completed research build.

Archive/Federation/Infinite integration requires current provider, residency, query/index, deletion and billing evidence. The implementation must not depend on an archived view being Search-indexable.

## Healthcare Data Lab example journey

H1 can begin with the packaged synthetic metadata before any pixels are downloaded:

1. Activate the DICOM strategy and read `dicom_query_catalog`.
2. Generate the default two-center metadata dataset; show catalogs available before importing all instance metadata.
3. Run patient/day and modality/period examples, then study-to-series-to-instance browsing.
4. Demonstrate exact/prefix series descriptions and numeric ordering (1, 2, 10).
5. Select header fields and request a bounded batch. Show missing fields and deferred metadata honestly.
6. Activate the default manufacturer/thickness projection policy. Build bounded pages separately from ingestion; show partial coverage.
7. Run manufacturer plus thickness predicates and display instance results with study/series references. Avoid labeling their row count as a patient/study cohort count.
8. Display planned published-cohort and condition-linking examples as unavailable until the matching Kehrnel capabilities exist.

The application should import the examples from Kehrnel discovery, not duplicate their query JSON. Builders and agentic workbench tools use the same schema and trusted runtime authorization. Synthetic expected counts apply only to the fresh default dataset and completed example backfill.

H2 adds project field/grain selection, estimates, queued/running/cancelled/failed/published state, coverage receipts, reusable generations and cohort export after K2. H3 adds a license-pinned public imaging dataset and actual DICOMweb/viewer integration. Metadata-only generation does not imply an image can be rendered.

## Validation record and handoff

K1 validation must cover semantic adversaries in real localhost MongoDB, selected-header VR fidelity and byte bounds, inline/bundle behavior, shared SDK/HTTP/CLI discovery, every executable example and catalog digest consistency. Preserve the existing customer sample report as baseline evidence; do not turn its predominantly OT sizes into CT/MR forecasts.

Validation completed: **222 tests passed** across the selected strategy/runtime regression suite (196) and the additional manifest/outcome discovery checks (26), including 74 focused DICOM unit, localhost integration and HTTP/CLI tests. All nine synthetic queries executed through the SDK in both storage modes and through real HTTP; the CLI executed the combined research request through the same HTTP/MongoDB path. The aggregate evidence is in `docs/validation/dicom-query-interface-report.json`. Catalog digests were refreshed and the SDK compile-only example ran successfully. These checks are correctness evidence, not a sharded scale or object-storage benchmark. The next Kehrnel work packages after the catalog bridge are canonical Builder/package integration and K2 for published research. H1 can consume an already activated runtime in a separate HDL checkout. K1B now provides the source profile, starter template and source-aware catalog bridge; the canonical participant/interaction Builder and package activation still need DICOM-specific integration. Production approval follows K3–K5 evidence, with clinical claims gated separately by K6.


K1B validation completed: **191 tests passed** (62 builder/source/catalog/schema checks and 129 semantic/architecture/catalog/DICOM regressions), with no failures, errors or skips. The HTTP handoff validates resolver-backed explicit activation and compilation of all nine query examples; it does not require a live database. The SDK builder example also ran successfully. Catalog refresh reports no drift and published schema bytes match regeneration. This increment does not rerun the historical real-MongoDB execution benchmark or validate canonical package activation, HDL screens, remote object transfers or scale. Evidence is in `docs/validation/dicom-builder-interface-report.json`.

## Shared domain sample foundation

DICOM sample directories belong to the domain. The frozen five-variant synthetic metadata source is packaged under `src/kehrnel/engine/domains/dicom/samples`, with independent dataset versions, manifests and checksums. Native rule discovery/demo and the local SDK loader consume these references. Projection rules and index plans stay strategy-owned. `docs/dicom-domain-samples.md` defines the boundary and the subsequent licensed public-data packages; real pixel acquisition and the public visual release remain separate deliverables.

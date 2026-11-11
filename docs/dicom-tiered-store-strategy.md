# DICOM Tiered Metadata Store: executable preview

The native pack is `dicom.tiered_store`, version 0.2.0. It validates a practical starting point for the customer's migration: preserve the study/instance model, derive a flat series catalog, promote a small set of typed routing fields, and retrieve detailed metadata separately. Research projections are optional.

This is an executable metadata strategy, with SDK, HTTP, CLI, discovery, configuration and data-model contracts. It is not a DICOMweb server or a completed Healthcare Data Lab imaging application.

## What is implemented

| Requirement | Implemented behavior |
| --- | --- |
| Studies for a patient and date | Center-scoped local patient ID, UTC source day, bounded regular-index query |
| Studies for a modality and period | Half-open date range; modality and center must refer to the same series facet |
| Description browsing | Normalized exact matching by default; literal prefix matching within one day; series description stays correlated with modality and center |
| Series for a study | Flat series catalog with its own center ownership |
| Instances for a series | Typed numeric InstanceNumber, missing values last, SOP UID tie-break, continuation cursor |
| Continuous instance arrivals | Independent instance writes; instance-only batches do not rewrite study or series documents |
| Series counts | Separate, index-backed refresh of imported instance-document counts; original source count retained separately |
| Detailed instance metadata | Original header and ContentIndex inline, or retrieved from an immutable metadata bundle |
| Progressive migration | Import study/series catalog first; missing instance metadata is reported explicitly |
| Query builder discovery | Activation-specific schema, promoted fields, bounds and nine synthetic query examples |
| Selected header batches | Up to 100 SOPs in one series, optional top-level fields, byte-bound deferral and VR preservation |
| Multi-tag research predicates | Up to 16 distinct promoted tags with AND semantics on the same SOP instance |
| Research promotion | Configured top-level tags and modality filter produce a side collection through an explicit background build by default |
| Research backfill | Resumable, bounded pages for one series; object bundles can supply the original metadata |
| Optional Search index | Static mapping and apply support on the research sidecar; Atlas Search execution is not implemented |
| Public demonstration seed | Deterministic two-center metadata, configurable modalities/series/day, repeated local patient/accession IDs and numeric ordering examples |
| Schema and projection evolution | Storage version, source content revision and projection-configuration digest |

The [customer-sample report](validation/dicom-customer-sample-report.json) records local database evidence. These checks are correctness and index-plan validation, not a performance benchmark at 15 billion instances.

## Customer model assessment

The exported sample contains 500 studies, 518 series and 1,062 instances. The source model embeds series in each study, while instances retain DICOM JSON headers and a ContentIndex. That is a useful compatibility boundary and remains recognizable in this pack.

The sample has 64 scalar SeriesInstanceUID values, although standard DICOM JSON uses arrays. The reader accepts these without rewriting the source and records a normalization warning. There are also 66 series whose center differs from their parent study's center. Instance ownership therefore comes from the authoritative series, not an assumed inheritance from the study.

The export is predominantly synthetic OT metadata, with a small MG/SR sample. It cannot establish real CT/MR document sizes, compression ratios, peak ingestion rates or Atlas prices. The validation command reads only the study and instance exports; it does not read customer configuration, user collections, credential files or a live database.

### Changes from Claude's proposal

| Idea | Decision |
| --- | --- |
| Keep one polymorphic instance collection | Adopted; modality differences belong in configured projections |
| Keep study/series catalog online | Adopted; the series catalog avoids repeated traversal of instance headers |
| Put every header into Search | Rejected; no default instance Search index |
| Order identity by series plus InstanceNumber | Preserve SOP identity and use an explicit compound query index instead |
| Regular `_id` implies physical clustering | No such assumption; MongoDB explain verifies the actual query plan |
| InstanceNumber is viewer geometry | Numeric browsing order only; geometric frame ordering remains a viewer/DICOM responsibility |
| “27% of current bytes” reduction | Not adopted as a forecast; compare actual BSON and index sizes for representative modalities |
| Move common header attributes into mutable series defaults | Deferred; exact reconstruction needs immutable dictionaries/revisions and exception handling |
| One document for all instances or all metadata of a series | Avoided; both document size and object bundle size are bounded |
| Derive diagnoses from headers | Unsupported; diagnosis requires linked evidence with clinical role, timing and provenance |
| Infinite or Online Archive is the default execution substrate | Deferred; current platform/provider limits must be validated for the target deployment |

## Physical model

The configurable collection names default to `dicom_studies`, `dicom_series`, `dicom_instances` and `dicom_research`.

**Studies** retain their customer fields and embedded series. The reserved `_kehrnel` envelope adds tenant, center, local patient ID/key, study UID, typed date/day, series facets and revision metadata. List queries return basic patient/study display fields but do not return the embedded series array or facets from other centers.

**Series** have the original SeriesInstanceUID as `_id`, the original series object in `source`, and typed routing in `_kehrnel`. The series center is `ci`; the study center is `dm`. They are intentionally separate.

**Instances** retain the original SOPInstanceUID as `_id`. The envelope carries series/study ownership, numeric InstanceNumber, an explicit missing-order flag, the source revision, and optional bundle reference. No field from the request can broaden the configured tenant or center scope.

Mutable shared fields such as patient identity, date and modality remain in the study/series catalogs rather than being copied into every persisted instance envelope. A research build reads the authoritative series catalog to enrich its selected sidecars. Center and hierarchy routing remain on each instance so viewer reads need no authorization lookup join.

**Research** has one document per eligible instance, with only selected typed values in `attrs`. Each value retains its DICOM tag. A predicate uses `$elemMatch` to pair tag and value on the same element. The optional Search mapping uses `embeddedDocuments` for the corresponding semantics.

The default plan has nine explicit regular indexes: four for studies, four for series, and one for series-instance ordering. MongoDB also creates each collection's `_id` index. Research adds three optional regular indexes and, if configured, one Search index. These are workload hypotheses verified by explain, not a guarantee that every filter combination is covered.

### Header placement

`headers.mode=inline` preserves full instance records in MongoDB. This is the compatibility baseline.

`headers.mode=bundle` retains complete instance source metadata in content-addressed BSON objects and stores only a locator envelope in MongoDB. A locator identifies object key, SHA-256, byte length and record ordinal. The default maximum object size is 4 MiB. Objects are written before locators; reads check size, digest and SOP identity. Re-importing the same batch reuses the object key.

Bundles contain metadata, not DICOM pixel bytes. Their grouping is bounded by ingestion batch and byte size; this preview does not promise optimal per-series bundling, byte-range extraction, cross-batch deduplication or a production cache. The artifact adapter supplies filesystem, S3 or Azure placement. Locators persist object keys; runtime-created signed URLs stay outside the locator. The untouched source may contain its original BulkDataURI.

Bundle mode avoids a compulsory object per instance. It still retains one MongoDB locator per imported instance. A future bounded series-manifest mode could remove many cold locators; that optimization is not implemented here.

## Continuous ingestion and aggregates

The customer's typical study has about three series, rising to about twelve for CT/MR. A bounded array of series descriptors can remain useful for catalog display and compatibility. An array of all instances is a different workload: repeated appends and counter updates concentrate writes on the same document while the acquisition is running. This strategy writes each arriving instance independently and retains a flat series catalog for direct queries.

Instance-only ingestion performs no study or series writes. An authoritative study snapshot updates the study and its series descriptors explicitly; it does not run for every instance arrival. Source snapshots preserve the separately maintained series summary. The preview bounds ingestion to 500 documents and a configured byte limit; it is not yet a production bulk-loader or change-data-capture service.

The recommended production pipeline is:

1. Write validated source metadata or its immutable bundle, then the instance locator. Acknowledge the bounded batch after its writes complete.
2. Emit affected series keys to a durable queue or dirty-series set. Coalesce many arrivals for the same series into one pending refresh. Partition work by series and use one active refresher per series.
3. Refresh counts and small aggregates on a measured cadence, for example every few seconds while the series is active and once more after an inactivity interval. An inactivity interval is a scheduling hint, not proof that acquisition is clinically complete.
4. Reconcile from durable instance records after retries or worker failure. Do not increment totals merely because an upload request was retried.

The pack implements independent writes, returns `affectedSeries`, and supplies `dicom_refresh_series_summary`. The durable queue/outbox, coalescing worker, source delta protocol and acquisition-completion detection are follow-on work. A staging input collection is useful if it supplies durable retry/checkpoint semantics; it is not needed merely to duplicate every header before writing it again. Reuse the ingestion platform's durable event log where available. Persisting records and publishing events must have a recoverable handoff before a production worker depends on it.

The summary counts imported SOP-instance documents using the series-order index. It records `asOf`, source revision and partial/unknown metadata coverage. It retains the customer's `ni` as `sourceReportedCount`. The unit of `ni` must be confirmed: images, SOP instances and frames differ for Enhanced multi-frame objects. During progressive migration, a zero imported count does not mean the study has no images. The viewer should show the authoritative source count and metadata import state separately.

The refresh is asynchronous and reflects its read, not a transactionally exact count at the instant of display. Frequent index-backed recounts of very large series still cost work; production cadence, incremental durable accounting and periodic reconciliation must be measured with real acquisition bursts. No per-instance update of a shared counter is required by the preview.

## Viewer description queries

Study and series descriptions are normalized with Unicode NFKC, case folding and whitespace normalization. Source text remains unchanged. The default `descriptionMode=exact` matches the complete normalized catalog description and can use its regular index across history. Explicit `descriptionMode=prefix` matches the normalized start of the description. Prefix queries require one day, either `day` or a one-day half-open range, so the catalog/date filter bounds their candidate set. Word-anywhere matching is outside the viewer query contract, following the customer's selected exact/prefix semantics.

`description` targets the selected study or series resource. `seriesDescription` targets a study's series facets and requires one day even for exact matching. Its predicates share a single `$elemMatch` with modality and center: an MR series description cannot accidentally match a CT series in the same study. Caller-supplied regular expressions are not accepted.

Exact and prefix matching use regular MongoDB queries and do not require an Atlas Search index. Extending prefix browsing across unrestricted history needs a measured candidate/sort and pagination plan; it does not require indexing fifteen billion instance headers. The preview retains its one-day prefix scope.

## Identity, dates and evolution

Patient IDs are scoped by tenant and center. Repeated patient IDs or accession numbers do not merge people. Enterprise issuer-aware linking remains a separate identity service.

Source Study, Series and SOP UIDs remain the database identities. Scoped upserts reject a conflicting UID in another tenant or center. A dedicated database per tenant is recommended. On a sharded collection, global UID uniqueness is not automatically guaranteed when the shard key differs from `_id`; shard-local uniqueness and identity routing must be designed before deployment. This preview does not automatically shard collections.

The day projection uses legacy study `dt` interpreted in UTC. Original DICOM DA, TM, DT, VR, private attributes, nested sequences, empty values, bulk-data references and ContentIndex remain preserved. A local-calendar query policy based on original acquisition dates/timezones needs a separate contract and migration.

`_kehrnel.storage_schema_version` identifies the storage shape. `source_revision` hashes the source content. `projection_contract_version` and research `projectionVersion` identify the effective projection configuration. These are distinct from a retained history store; historical study versions are not automatically persisted.

Changing a promotion policy makes old projection versions invisible to the new policy. Rebuilds progressively replace sidecars. Complete-corpus publication, reconciliation receipts, deactivation cleanup and historical generations remain future work.

## Research and clinical semantics

Research tags are explicitly configured, limited to 32 top-level tag definitions and a bounded number of values per instance. Queries accept the compatible singular `filter.tag` or up to 16 distinct predicates in `filter.tags`, joined with AND on the same SOP instance. Each tag/value or numeric range matches one attribute occurrence. Strings use exact equality; numeric tags require a numeric VR and valid finite numeric values. The policy can restrict modalities.

`projections.refresh_on_ingest` defaults to `false`. Selecting research fields therefore does not add projection extraction and indexing to the viewer ingestion path. Run bounded `dicom_rebuild_projections` pages separately. With research enabled, changed imported instance sources invalidate stale sidecars in one bounded batch operation; exact retries preserve matching sidecars. Catalog changes require research reconciliation as well. An explicit `true` setting is available for small demonstrations, but synchronous extraction is not the recommended acquisition path.

Healthcare Data Lab should collect the project scope, modality, date period, fields and expected cohort unit before starting a build. Estimate source scan bytes, extraction work, projected document count and Search size, then publish a versioned generation only after its coverage checks pass. One day is an acceptable scheduling objective from the customer, not a guaranteed backfill duration. A full fifteen-billion-instance source scan can exceed it. Keep source metadata in scan-friendly bundles or columnar exports so adding a field does not require reopening all original pixel files.

Choose the smallest correct projection unit. A manufacturer/date mammography query usually needs a series-level materialization, then distinct study references. If a value varies between instances, retain explicit variation or instance evidence rather than silently copying the first instance's value to the series. Reserve instance-level sidecars for projects whose predicates actually require that granularity. This preview's research query unit is an eligible instance; grouped series/study materializations and distinct cohort export are part of the publication work package. Enabling that preview across all modalities would still create a potentially expensive instance-scale collection.

Prefer a materialized side collection with an explicit extraction contract over a view as the architectural dependency. It can be built from MongoDB, retained objects or a future archive adapter and indexed after publication. Whether a particular hosted view or archived namespace supports Search must be checked for the deployed platform; the strategy should not depend on that support. Reuse compatible published projections between projects and retire generations through a governed lifecycle, instead of creating an unlimited index for every researcher.

Sequences remain in retained source metadata, but sequence projection is rejected in this release. Matching multiple predicates within the same nested sequence item needs an occurrence-aware path contract; flattening leaves without that context would give incorrect results.

Research responses declare partial coverage, and callers must set `allowPartial:true`. A missing projected match cannot be interpreted as the absence of a clinical finding or acquisition feature.

Condition search is explicitly unsupported for this source. A future assertion model should join diagnoses, indications, confirmed findings and AI hypotheses from HL7, SR, FHIR or openEHR, preserving issuer, clinical role, status, timing, evidence and terminology release. The SNOMED gateway should supply controlled concept expansion; it should not make an order indication equivalent to a confirmed diagnosis.

The useful lessons from existing strategies are applied selectively: openEHR's source/projection separation and same-element semantics, FHIR's typed query contract and projection versioning, CDISC's retained artifacts and governed materializations, and SNOMED's versioned terminology gateway. None requires replacing DICOM with another clinical standard.

## Activation and operations

Activate domain `dicom`, strategy `dicom.tiered_store`, with an explicit database and configured tenant/centers. Default demo centers are `DEMO_A` and `DEMO_B`.

The published operations are:

- `dicom_capabilities`
- `dicom_query_catalog`
- `dicom_get_headers`
- `dicom_ingest`
- `dicom_query`
- `dicom_get_header`
- `dicom_rebuild_projections`
- `dicom_generate_synthetic`
- `dicom_refresh_series_summary`

HTTP uses the existing environment runtime:

```text
POST /v1/environments/{env}/activate
POST /v1/environments/{env}/plan
POST /v1/environments/{env}/apply
POST /v1/environments/{env}/compile_query
POST /v1/environments/{env}/query
POST /v1/environments/{env}/run
```

Example viewer query:

```json
{
  "version": "dicom-query/v1",
  "resource": "instances",
  "filter": {"seriesUid": "2.25.2001"},
  "limit": 100
}
```

Use the returned `explain.page.nextCursor` for the next page. The cursor is bound to the query, tenant, authorized centers and projection configuration. It is a continuation token, not an authorization credential or snapshot-isolation guarantee.

Example viewer query for one day's MR studies by series description:

```json
{
  "version": "dicom-query/v1",
  "resource": "studies",
  "filter": {
    "day": "2026-10-26",
    "modality": "MR",
    "seriesDescription": "MR brain",
    "descriptionMode": "prefix"
  }
}
```

Example research query after promotion:

```json
{
  "version": "dicom-query/v1",
  "resource": "research",
  "allowPartial": true,
  "filter": {
    "modality": "MG",
    "fromDay": "2026-01-01",
    "toDay": "2027-01-01",
    "tag": {"path": "00080070", "equals": "SYNTHETIC_MANUFACTURER"}
  }
}
```

Research rows carry study/series/SOP references; they do not automatically return distinct studies or patients, or complete-cohort counts.

The generic CLI dispatches the same manifest operation:

```sh
kehrnel run dicom_generate_synthetic --env imaging --domain dicom \
  --strategy dicom.tiered_store --set instances=12
kehrnel run dicom_query --env imaging --domain dicom --payload viewer-query.json
kehrnel run dicom_rebuild_projections --env imaging --domain dicom \
  --set seriesUid=2.25.2001 --set limit=100
kehrnel run dicom_refresh_series_summary --env imaging --domain dicom \
  --set seriesUid=2.25.2001
```

The synthetic operation accepts `seriesPerStudy` (1–32), `instances` per series (1–100), two modality codes, and an optional ISO day. A recipe with `seriesPerStudy=12`, `modalities=["MR","MR"]` and `day="2026-10-26"` exercises a twelve-series MRI study at each demo center. Persisted generation imports catalogs first and batches the instances. It generates no pixels and supplies no real acquisition geometry.

The public SDK uses `StrategyHandle` with the same `StrategyContext`, plugin hooks and adapters. Contract tests exercise SDK activation, HTTP discovery/compilation and CLI-to-HTTP dispatch.

## Validation

Run unit, native runtime and optional localhost integration tests:

```sh
KEHRNEL_DICOM_TEST_MONGODB_URI=mongodb://127.0.0.1:28079 \
  PYTHONPATH=src python -m pytest tests/unit/test_dicom_tiered_store.py \
  tests/integration/test_dicom_tiered_store_mongo.py \
  tests/contract/test_dicom_runtime_surfaces.py
```

Validate the customer export against a disposable localhost MongoDB:

```sh
PYTHONPATH=src python scripts/validate_dicom_strategy.py \
  --samples /path/to/samples/dicom \
  --mongo-uri mongodb://127.0.0.1:28079 \
  --output /path/to/aggregate-report.json
```

The command creates and removes only a unique validation database. It compares source-derived ordering and identities, verifies every metadata round trip, tests progressive ingestion and selective backfill, records explain evidence, and reports actual local BSON/storage/index sizes. Customer records and values are not emitted.

The 0.2.0 strategy and regression suite passed 222 tests across the strategy/runtime regression and manifest/outcome discovery checks, including native SDK contracts, CLI/HTTP dispatch, catalog discovery and real localhost MongoDB queries. Atlas Search query execution, object-storage transfer performance, multi-shard behavior and pixel viewing were not exercised.

The recorded customer-sample baseline run verifies 1,062 source-metadata round trips and browsing of all 518 series in both storage modes, zero catalog writes for instance-only ingestion, separate series-summary refresh and description selection. The checked instance, patient/day, modality and study-series plans use their regular indexes without a collection scan or blocking sort. Bundle mode retains eleven metadata objects and reduces live instance-document logical BSON from 3,312,525 to 1,015,312 bytes compared with this pack's inline mode, about 69%. This is a sample comparison, not a CT/MR sizing forecast or a reduction in total storage: retained bundle bytes, indexes, catalogs, replicas and object requests also count.

## Budget and deployment decisions

| Budget profile | Recommended placement | Benefit | Additional work before production |
| --- | --- | --- | --- |
| Minimum | All study/series catalogs online; progressively imported detailed metadata; retain legacy binary resolution | Immediate migration benefit; no global research index | Legacy resolver contract, source reconciliation and CDC |
| Balanced | All study/series catalogs and compact instance locators online; full metadata in bounded object-store bundles | Detailed metadata available with a smaller MongoDB document | provider-specific object-storage load tests, cache, range/prefetch policy and sharded locator design |
| Research expansion | Balanced profile plus selected modality/tag projections and bounded published cohorts | More access without indexing every instance header | Corpus receipts, study/series/patient deduplication, Atlas Search execution and cost controls |
| Advanced archive | Compact historical catalogs plus manifests/columnar exports; hot instance cache and selective projections | Potential reduction in historical locator/index cost | Manifest routing, archival query backend, rehydration SLA and deletion/legal-hold lifecycle |

Monthly cost must include live document bytes, index bytes, replicas, Search copies/embedded-document expansion, object storage, request counts, retrieval, cache, cross-cloud transfers and projection backfills. Logical BSON byte reduction is one input, not the invoice.

At fifteen billion records, each extra 100 logical bytes per instance adds about 1.5 TB before replicas and indexes; a 1 KB locator is about 15 TB. Compact MongoDB locators are a cost reduction, not a zero-cost archive. Benchmark a recent-instance cache plus bounded historical series manifests against all-history live locators before choosing the lower-budget production profile. All study/series catalogs should stay online when old and recent studies must be equally easy to find; header retrieval can use a separate latency budget.

At 15 billion instances, 8 million new instances/day is about 93/sec on average, but migration throughput and acquisition bursts are separate requirements. A week-long backfill requires roughly 24,800 instance records/sec before retries, metadata extraction and index overhead. The preview has not demonstrated that rate.

The supplied daily figures imply seven series per new study (420,000 / 60,000), while the reported typical study has three and CT/MR about twelve. The historical totals imply about 2.5. These can describe different distributions or periods; validate their definitions before capacity planning. Test three-series and twelve-series recipes, heavy-tail series sizes, concurrent acquisitions, replay bursts and the migration load separately. Schema decisions should not depend on a single average.

Pre-stage the large study/series catalog and reconcile it continuously. A night outage should handle the final delta and application cutover, not an assumed full 140-million-study migration. Keep legacy binary resolution until each object-storage copy is verified. Instance metadata can be populated progressively; missing metadata must remain distinct from an empty header.

Online Archive, Data Federation and Atlas Infinite require current, provider-specific proof of query/index capabilities, residency, billing and operational limits. They are future backend options, not supported execution paths in this release.

### Sharding hypotheses for the production POC

Evaluate each collection independently rather than inheriting the SQL Server application partitions. The catalogs are much smaller than the instance corpus, but hundreds of millions of catalog records still warrant a measured shard/index design.

| Store | Candidate routing hypothesis | Tradeoff to test |
| --- | --- | --- |
| Studies | Tenant/center plus a hashed scoped patient key | Patient history can be targeted; date/modality queries fan out within their permitted scope |
| Series | Tenant/center plus a hashed study UID | Study-to-series navigation stays targeted; independent patient/date/modality searches may fan out |
| Instances | Tenant/center plus a hashed series UID | A viewer's series query carries the routing fields; one unusually large or busy series stays a potential hot key |
| Research materializations | Explicit project/generation scope plus grain-specific routing | Research throughput can be isolated; broad cohorts still need planned fan-out and deduplication |

These are hypotheses, not shard keys applied by this pack. Confirm the driver's query routing and MongoDB's actual shard targeting, required shard-key indexes, UID uniqueness policy, zone distribution, chunk behavior and replica costs. Hashed routing distributes keys; it does not split a single hot series. Add a bucket dimension only if measured large-series pressure justifies its extra viewer fan-out and manifest complexity. A patient-routed study query also needs a center-scoped patient key in its compiled filter to be targeted; the current unsharded preview does not add that deployment-specific routing automatically.

## Healthcare Data Lab handoff and next work packages

The runtime exposes the manifest, configuration schema, model shape, query contract and synthetic operation for builder integration. A draft metadata-exploration outcome links the pack into the existing catalog. A DICOM source profile, immutable builder template and the actual Healthcare Data Lab user journey are still to be built; their availability is not implied by the draft outcome.

1. **Production migration/runtime:** bounded bulk ingestion, resumable source checkpoints, CDC, authoritative catalog updates, durable dirty-series handoff and coalescing summaries, removal/relink reconciliation, UID conflict policy, operational telemetry and load/chaos tests.
2. **Sharded catalogs and instance routing:** benchmark patient-led versus study/series-led keys, targeted viewer plans, hot-series handling, UID uniqueness and replica/index costs using representative modality distributions.
3. **Object-storage metadata lifecycle:** validate the artifact adapter under concurrent reads/writes, object grouping, cache byte limits, resolver failure modes, checksum verification, retention, deletion and orphan-object cleanup.
4. **Published research projections:** project/grain contracts, series/study-first materializations, immutable generation receipts, complete coverage boundaries, attribute occurrence paths, selective cohort manifests, deduplicated series/study/patient results, Search query execution and backfill quotas.
5. **Clinical linking:** issuer-aware subject links and explicit clinical assertions; reuse the terminology gateway with bounded expansion and pinned releases.
6. **Public imaging demo:** source a licensed, checksum-pinned, de-identified pixel dataset; add a DICOMweb/viewer adapter; show patient/date/modality browsing and a manufacturer cohort. The synthetic operation provides metadata only.
7. **Data Lab journey:** activation, ingestion state, model inspection, viewer, query builder, projection selection, backfill progress, coverage receipts, agentic tools and evidence export.

Each package should use its own worktree, acceptance evidence and support boundary. Existing architecture/roadmap documents remain the wider plan; this pack supplies the first executable validation surface.

## Next implementation phases

The [Kehrnel and Healthcare Data Lab implementation plan](dicom-implementation-plan.md) defines the query-interface increment and subsequent production work packages. The shared synthetic requests are in `examples/dicom`; `dicom_query_catalog` exposes the same examples to builders. The SDK journey compiles them without a database and executes them with bound adapters. Published series/study/patient cohorts remain the next research milestone.

The [query-interface validation report](validation/dicom-query-interface-report.json) records the 0.2.0 interface evidence, including all nine synthetic examples, multi-tag correlation and bounded selected-header retrieval.

## Deployment portability and builder handoff

The DICOM storage/query strategy is provider-neutral. Azure is the customer's selected object-storage deployment; AWS or Google Cloud can use the same tiering, ingestion and query design with an appropriate adapter. Currently the runtime offers filesystem, S3 and Azure artifact adapters. A native GCS adapter, provider transfer benchmarks and archive query execution are not validated capabilities of this preview.

The Kehrnel catalog now includes a DICOM metadata source profile, immutable starter template and reference architecture. The source-aware compiler can evaluate an explicitly selected DICOM preview without promoting its delivery track. See `docs/dicom-builder-handoff.md` for the supported catalog bridge and the remaining canonical Builder/Healthcare Data Lab work.

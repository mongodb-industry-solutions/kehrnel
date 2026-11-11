# DICOM ingestion, rules and research playground

Status: `dicom.tiered_store` **0.3.0, evaluation preview**. This increment implements a bounded source-to-query journey. It does not qualify ingestion or research across 12 billion historical instances and 2.4 billion new instances per year.

## The model we are working toward

Keep the customer's study document with its small embedded `se[]` series catalogue. A typical study has three series, and CT/MR commonly have twelve; this does not justify splitting every series into a second operational catalogue. New series can arrive later. Bound the embedded catalogue and provide an overflow design for exceptional studies rather than assuming its size is fixed forever.

Each embedded series can contain a **small derived summary** under `_kehrnel.ruleSummary`. It should describe observed object classes, equipment, acquisition/reconstruction groups, rendering characteristics, counts and metadata coverage. Full headers, per-frame geometry, offsets, measurements and large annotations belong outside it. Research fields become independently rebuildable, selected sidecar rows when they need a different query grain or indexing policy.

The earlier native `dicom_ingest` and `dicom_query` path still uses the existing flat series catalogue. The new `dicom_rule_*` path is an **opt-in embedded-study snapshot and series research preview**. It deliberately does not silently replace the earlier persistence path or declare migration compatibility complete.

| Layer | Purpose | Preview collection / storage |
| --- | --- | --- |
| Operational catalogue | Fast patient/day, modality/day and exact/prefix selection | Existing `dicom_studies` and compatibility `dicom_series` |
| Reception | Accept an immutable batch before computing summaries | `dicom_instances_inbox` |
| Source inventory | Unique accepted SOPs, hierarchy, routing and source fingerprint | `dicom_instances_rule_inventory` |
| Complete original metadata | Retain unknown/private attributes for future extraction | Inline by default in the preview; immutable artifact bundles when `headers.mode=bundle` |
| Derived embedded catalogue | Freeze the study and bounded per-series summaries for inspection | `dicom_studies_rule_snapshots` |
| Research projection | Selected typed values and bounded correlated groups, one row per selected series/generation | `dicom_research_series` |
| Rule and publication receipts | Explain what produced the data and whether it is queryable | `dicom_research_profiles`, `dicom_research_generations` |

Names derive from the activation's collection configuration. A profile cannot choose an arbitrary database or collection. These preview snapshots are not the eventual current operational head.

The existing historical enumeration service is a valid inventory provider: SQL already supplies SOP/study/series UIDs and binary locators. We do not need to manufacture 12 billion detailed headers before enabling catalogue access. A production implementation can retain bounded series manifests and use that service instead of requiring every historical instance to be a live MongoDB document.

## What the implemented rules can express

The catalog publishes eleven starters: imaging core, CT, MR, MG, US, PT, NM, SM, SR, SEG and RT. They are partial attribute extraction profiles, **not** a DICOM conformance matrix or complete clinical interpretation of those classes.

```json
{
  "version": "dicom-rule-profile/v1",
  "id": "ct-reconstruction",
  "name": "CT reconstruction research",
  "modalities": ["CT"],
  "fields": [
    {"name": "manufacturer", "source": "header", "path": ["00080070"],
     "type": "string", "aggregate": "distinct", "target": "research", "maxValues": 16},
    {"name": "kernel", "source": "header", "path": ["00181210"],
     "type": "string", "aggregate": "distinct", "target": "research", "maxValues": 16},
    {"name": "sliceThickness", "source": "header", "path": ["00180050"],
     "type": "number", "aggregate": "distinct", "target": "embedded", "unit": "mm", "maxValues": 16}
  ],
  "groups": [
    {"name": "reconstruction", "fields": ["kernel", "sliceThickness"],
     "target": "research", "maxGroups": 16}
  ]
}
```

* `distinct` retains exact observed values, with a cardinality bound; it never silently truncates.
* `common` exposes one value only when every observed SOP has it and the observed unique count equals the source-reported count. A missing header cannot establish commonality.
* `range` computes min/max statistics. It cannot answer arbitrary existential range predicates: values 512 and 1024 do not establish that an image has 700 rows.
* A group retains distinct **same-instance scalar tuples**, with a SOP count per tuple. It does not create the Cartesian product of separate multivalued fields.
* Paths use DICOM tags and explicit `*` sequence traversal, or explicit `ContentIndex` paths. Sequence leaf listing is supported; correlated sequence-item/frame queries are not implemented. Their occurrence identities must be retained by dedicated adapters.
* Numeric extraction checks DICOM VR and rejects invalid/non-finite values. Source units can be declared, but no conversion or clinical calibration is performed. The current numeric implementation uses floating point; exact decimal/high-precision research needs a stronger typed contract.

Limits are 32 fields, eight groups, 64 distinct values/tuples per configured bound, 256 KiB per embedded summary envelope, and 512 KiB per research row. An over-limit projection fails explicitly; the next design is a finer-grained sidecar, not a larger unbounded array.

### What appears inside the study

This is an abbreviated illustrative shape; the playground shows complete generated documents and provenance.

```json
{
  "_id": "<generation>:<studyUid>",
  "studyUid": "2.25.1001",
  "generationId": "<generation>",
  "dm": "DEMO_A",
  "pi": "SYNTHETIC_PATIENT",
  "se": [{
    "si": "2.25.2001", "ci": "DEMO_A", "mo": "CT", "ni": 6,
    "_kehrnel": {
      "ruleSummary": {
        "profileVersion": "v1:<effective-profile-hash>",
        "sourceGeneration": "<source-hash>",
        "counts": {"receivedUniqueSops": 6, "expectedSops": 6,
                   "state": "complete_against_reported_count"},
        "fields": {
          "sliceThickness": {"type": "number", "aggregate": "distinct",
            "values": [1.0, 2.0], "queryable": true,
            "coverage": {"observedSops": 6, "withValue": 6,
                         "missingOrEmpty": 0, "invalidValues": 0, "sourceReportedSops": 6}}
        },
        "groups": []
      }
    }
  }]
}
```

`ni` remains the source-reported count. It is interpreted as a SOP count for this preview; confirm that definition with the customer. Multiframe images make **instances, images and frames different counts**. Equality with `ni` is evidence of coverage against the reported count, not a permanent guarantee that no new object will ever arrive.

### What appears in a research row

```json
{
  "_id": "<generation>:2.25.2001", "seriesUid": "2.25.2001",
  "generationId": "<generation>", "profileId": "ct-reconstruction",
  "profileVersion": "v1:<effective-profile-hash>",
  "_kehrnel": {"tenantId": "demo", "centerId": "DEMO_A", "studyUid": "2.25.1001", "modality": "CT"},
  "attrs": [{"p": "manufacturer", "s": "SYNTHETIC_VENDOR"},
            {"p": "kernel", "s": "SYNTHETIC_KERNEL"}],
  "groups": [{"p": "reconstruction",
              "v": {"kernel": {"s": "SYNTHETIC_KERNEL"}, "sliceThickness": {"n": 1.0}},
              "sopCount": 3}],
  "fields": {"manufacturer": {"coverage": {"observedSops": 6, "withValue": 5, "missingOrEmpty": 1}}}
}
```

Research rows retain study/series routing, typed observations and coverage. The original values and their source VR/multiplicity remain in the retained header. Labels are not diagnoses; condition searches require provenance-linked SR findings or trusted CDR clinical data and issuer-aware subject linkage.

## Reception and consolidation: implemented behavior

1. `dicom_rule_receive` validates hierarchy, authorization, source size and profile; stores an immutable batch with its pinned profile. It does not calculate the projections.
2. Exact batch retries reuse the batch identity. Duplicate SOPs within a batch count once. The permanent inventory checks source fingerprints across batches; a changed SOP source is an explicit conflict, not an overwrite.
3. `dicom_rule_process` obtains an expiring batch lease with an ownership token. It retains metadata before inserting inventory references.
4. Processing loads the previously accepted unique SOPs for the affected series and consolidates them with the new arrivals. Summaries are calculated from that cumulative set, not just the last batch.
5. Study/research rows are immutable. The generation manifest is written last; query execution requires a ready authorized manifest and its exact profile version. A failure before publication is retryable and remains hidden from normal queries.

The preview accepts at most 500 source documents and 12 MiB per batch. Cumulative consolidation is subject to the same document bound. A large CT series therefore requires the forthcoming partitioned worker; it fails explicitly here. Reception is decoupled from processing, but it has not been throughput-qualified. The inbox currently contains the source metadata, so this prototype does incur a temporary copy. Bundle mode removes full headers/ContentIndex from the permanent inventory, not from the inbox.

### Production worker design to implement next

Use a durable per-SOP intake record or small intake batches with object references, plus a **dirty-series queue** keyed by tenant, centre and series. A durable queue/database handoff is required before acknowledging reception; do not rely on an in-memory debounce. Coalesce updates over a configurable interval and schedule immediately when reliable source completion evidence arrives.

Lease by affected series, preserve a source watermark, and store immutable metadata revisions. Uniqueness must survive queue retention and shard placement. Maintain accepted SOP counts independently of frame counts; source expected counts may be unknown or revised. Late objects reopen a series revision. A completion timeout means “quiescent”, not “clinically complete”.

A worker should stream/checkpoint large series, calculate bounded summaries and selected fine-grained rows, then compare the source revision before publishing a current head. If arrivals changed the series meanwhile, replay the delta or reschedule. Add an explicit correction/delete/relink path; the preview currently rejects changed SOP content. Avoid a shared study write per image: independent instance acceptance plus coalesced summary writes is the principal write-cost reduction.

Publication, garbage collection and intake expiry require a tested handoff. Do not TTL unprocessed intake, metadata needed for rebuilds or the only durable deduplication evidence. The current preview has no automatic retention/GC, latest-head promotion or archive-wide publication.

## Changing rules across hundreds of millions of series

A rule edit creates a new profile version. It must not mutate the only searchable generation or automatically scan all headers. Modality, SOP Class, tenant/centre and date narrow the source; a missing legacy SOP Class is an explicit coverage gap. Modality and SOP Class are distinct: enhanced CT, classic CT, derived objects and reports require different adapters even within a workflow.

`dicom_rule_plan_backfill` currently generates the coarse catalogue filter and required steps. It does **not** execute historical backfill. A production job needs:

1. A candidate/byte estimate, including headers that require discovery or hydration.
2. Indexed catalogue selection and inventory resolution of SOP Classes; no header-wide filtering scan just to discover candidates.
3. Stable tenant/centre/day/UID-range partitions, resumable cursors, source/profile watermarks and a recorded selector.
4. Throttled metadata reads and writes, incremental replay of live additions/corrections, cancellation and retry.
5. Immutable per-partition data, reconciled coverage and an explicit publication receipt.
6. An index readiness gate, then a generation pointer change; retain the previous generation until verified.

A MongoDB aggregation ending in `$merge` can materialize ordinary selected data. A view does not make this computation free, provide change propagation, or establish Search availability over an archived/federated source. Raw header extraction may use an external worker and artifact reads. Select the backend after measuring selected bytes, read costs and supported operations; do not promise a one-day rebuild regardless of selection size.

Do not create a permanent complete sidecar for every starter profile. Share compatible projection definitions across research projects, freeze project selectors and generation receipts, and retire unused versions under policy. At 350 million series, **1 KiB per series is about 358 GB** before BSON/indexes/replicas; 4 KiB is about 1.43 TB. Every embedded Search array element also increases Lucene object count. Selective projection and bounded grain are economic requirements.

## Automatic indexing and querying

The native engine derives regular and Atlas Search definitions from the validated profile. Search mapping is `dynamic:false`; scope/routing fields are tokens, typed `attrs[]` and correlated `groups[]` are `embeddedDocuments`. Mapping names hash the **effective mapping**, so changing only a selector or profile label can reuse the mapping instead of creating another index.

`dicom_rule_apply_indexes` applies regular indexes through the configured index administrator. With `includeSearch:true`, it requests the generated index through the Atlas Search adapter. **Readiness polling and actual Search query execution are not implemented or validated in this increment.** Index creation is asynchronous and distinct from projection publication.

Regular research indexes cover tenant/centre/generation plus `attrs.p`/`attrs.s` or `attrs.n`. Inventory selection covers tenant/centre/series/UID; intake has a state/lease index. Correlated group queries compile correctly but their performance/index coverage still needs qualification. These are evaluation indexes, not a sharded deployment prescription.

The query interface accepts structured comparisons or a bounded natural-language grammar:

```text
Find CT series where manufacturer = SYNTHETIC_VENDOR_B
Find MR series where echoTime >= 20 and echoTime < 40
Find CT series where rows = 1024 and photometric = MONOCHROME2
```

The last question has two explicitly different interpretations: observed values anywhere in the same series, or both on the same image using group `rendering`. The generated MQL uses `$elemMatch`; its Search alternative uses `embeddedDocument`. Multiple bounds on one field bind to the **same attribute value**. Unknown/unprojected fields and statistical min/max fields are rejected.

This is deterministic rule-aware compilation, not a general medical-language agent. A later agent should propose the same validated query contract, show the intended grain/correlation/coverage, and reject clinical assumptions unsupported by data. Existing `dicom-query/v1` viewer queries continue to cover patient/day, modality/day, series browsing and exact/prefix description selection.

## Shared SDK, CLI, HTTP and Healthcare Data Lab

All nine operations are published in the installed manifest: catalog, demo, preview, compile query, receive, process, query, apply indexes and plan backfill. HTTP uses `/v1/environments/{id}/run`; CLI uses `kehrnel run <operation> --env <id> --domain dicom --payload <file>`. `examples/sdk/dicom_rule_journey.py` dispatches the same native operations; no second query compiler lives in HDL.

The public Applications gallery includes **DICOM imaging and metadata lab**, opening `/healthcare/applications/dicom-metadata-playground`. Its reference architecture shows the native workflow and exact Kehrnel catalogue reference with capability limits. The same playground is also accessible at `/healthcare/dicom`. It has original-header/ContentIndex inspection, editable rules, generated collection/index inspection, queries, reception/rebuild controls and a modality storage guide. Public exploration generates synthetic metadata in Kehrnel; it accepts no customer source, arbitrary runtime URL or write operation. Existing authenticated environment operations handle imported bounded sources and persistence.

For local public preview, run `PYTHONPATH=src python examples/dicom/serve_playground.py --port 8092`, then start HDL with `KEHRNEL_DICOM_PLAYGROUND_URL=http://127.0.0.1:8092`. The launcher is a loopback-only synthetic fixture with temporary activation state. Production public deployment must use the normal authenticated/scoped runtime configuration; do not deploy this launcher.

This UI is a direct native-runtime playground. Canonical Builder package activation, agentic-workbench ingestion stages, archive job management, public pixel viewing and complete project publication remain separate milestones. Do not present them as implemented by linking to the playground.

## Broader DICOM placement and evidence

Use [DICOM PS3.3](https://dicom.nema.org/medical/dicom/current/output/html/part03.html) for object/module and functional-group semantics, [PS3.6](https://dicom.nema.org/medical/dicom/current/output/html/part06.html) for tags/VRs, and [PS3.18](https://dicom.nema.org/medical/dicom/current/output/html/part18.html) for DICOM JSON and web retrieval. Starter tag identifiers were checked against the [pydicom 3.0.1 dictionary](https://github.com/pydicom/pydicom/blob/v3.0.1/src/pydicom/_dicom_dict.py). Dictionary validation does not establish a complete SOP Class interpretation.

| Family | Bounded series summary candidates | Keep outside / dedicated research adapter |
| --- | --- | --- |
| CT | Equipment, protocol/reconstruction tuples, explicit contrast evidence, SOP/frame counts | Enhanced-frame geometry, dose events and large reconstruction groups |
| MR | Observed TR/TE/sequence/echo/diffusion acquisition tuples with known scope | Shared/per-frame functional groups, dynamic dimensions, diffusion directions and spectroscopy |
| MG/DX/CR | Laterality/view/equipment groups and image/frame counts | Tomosynthesis geometry, processing/presentation differences and dose trees |
| US/XA/RF | Encoding/cine availability and bounded acquisition summaries | Frame timelines, region calibration, waveforms and quantitative context |
| PT/NM | Units, correction state and acquisition summaries | Radiopharmaceutical timing/activity, calibration and dynamic frames; no assumed SUV conversion |
| SM | Total matrix, optical-path/resolution/specimen references | Sparse tiles, pyramids, frame coordinates and specimen/annotation trees |
| SR/SEG/PR/RT/ANN | Object kind, label/template summaries and reference coverage | Correlated concept/value/unit/target items, masks, contours, dose grids and annotations |

`ContentIndex` is primarily a retrieval accelerator: transfer syntax, encoding and frame offsets/lengths support efficient pixel access. It can support operational cost/availability summaries. It does not provide the anatomical geometry, clinical findings or measurement context absent from the header. Preserve its details with the instance/artifact rather than multiplying offsets into every series summary.

The customer sample is useful for correctness and heterogeneity checks; it is not representative capacity evidence for every class. Preserve full original headers and private/bulk references, use modality/SOP Class adapters, and validate on representative CT/MR, enhanced, multiframe, whole-slide and clinical SR datasets before promoting this strategy to production.

## Acceptance and next milestones

This increment has tests for progressive unique-SOP consolidation, immutable rule generations, publication failure/retry, conflict rejection, missing metadata, same-instance correlation, same-value ranges, profile version checks and MongoDB execution. SDK/HTTP/CLI discovery and a real native-runtime browser journey are exercised. Actual Atlas Search, sharding, large-series streaming and CDR joins require their own environment and evidence.

Next order: (1) streamable ingestion/dirty-series worker and correction protocol; (2) selective resumable backfill with measured budgets; (3) current-head and full-project publication with Search readiness/execution; (4) class/frame/clinical adapters and issuer-aware CDR composition; (5) canonical Builder and agentic-workbench integration; (6) qualified pixels and production-scale deployment. Each milestone must expose only implemented operations and tested semantics through the capability catalogue.

## Domain sample ownership

Frozen source fixtures now live at `src/kehrnel/engine/domains/dicom/samples`, shared independently of `dicom.tiered_store`. The native rule catalog advertises their dataset ID, version, provenance and checksums; demo presets load the pinned source version. Strategy rules/indexes remain in the strategy package. See `docs/dicom-domain-samples.md` for directory loading and the public dataset expansion contract.

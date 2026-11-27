# openEHR analytical strategy — ingestion and lifecycle v1

The `openehr.analytics` strategy produces the agreed document model independently
of the operational CDR. Activate it in a **separate Kehrnel environment and database**.
Keep `openehr.rps_dual_ibm` active for operational/AQL queries. This pack supplies
conversion, publication, rule inspection and mapping-coverage operations; it does
not replace the AQL compiler or implement a new analytics dashboard.

For the developer handoff, see the [integration and operating guide](openehr-analytics-integration.md),
[rule-language reference](openehr-analytics-rule-reference.md), and
[GUI and worker setup](openehr-analytics-integration.md).

## Connections

The ignored, private `.env.analytics.local` in the Kehrnel project contains:

- `ANALYTICS_SOURCE_URI`, `ANALYTICS_SOURCE_DATABASE`, `ANALYTICS_SOURCE_COLLECTION`.
- `ANALYTICS_TARGET_URI`, `ANALYTICS_TARGET_DATABASE`.
- `ANALYTICS_COLLECTION_PREFIX` and stable `ANALYTICS_SOURCE_NAMESPACE`.

Use a read-only source account. The destination needs reads, writes, collection
creation and index creation in its analytics database. `ping` proves connectivity,
not those permissions. Credentials are never part of rules, manifests or exported
activation files. Install `kehrnel-core[mongo]` for the standalone library; the
Kehrnel HTTP runtime additionally uses the normal API dependencies.

```sh
python -m kehrnel.engine.domains.openehr.analytics.cli --env .env.analytics.local inspect
python -m kehrnel.engine.domains.openehr.analytics.cli --env .env.analytics.local \
  --config examples/openehr_analytics/config.ibm.json plan
```

`inspect` reads at most 1,000 headers and one composition. Its template list is a
sample, not exhaustive inventory. `plan` does not create source or target indexes.

## One conversion function

```python
from kehrnel.engine.domains.openehr.analytics import Converter, RuleSet

converter = Converter(RuleSet.compile(configuration["rules"]),
                      source_namespace="my-cdr")
projection = converter.convert(composition, accepted_at=accepted_at)
# No I/O above. Explicitly publish when ready:
result = store.write(projection)
```

Construct the converter once per Python worker. Template lookup is
indexed; dictionaries are inverted once; path parsing is cached; per-composition
resolution caches preserve repeated-parent identities without repeatedly scanning
all instances. Conversion holds one source composition at a time. Publication
batches by both composition count and actual BSON bytes. A single composition
exceeding the configured transaction/document limits stops for review rather than
being partially published.

The initial implementation reconstructs the canonical tree in memory before using
the same clinical projector for both source formats. This favors verifiable
semantics and one rule interpreter. It does not claim that reconstruction is the
fastest possible path at production scale. A direct RPS projection can later replace
that stage only with demonstrated equivalence for repeated instances and qualifiers.

## IBM profiles are part of the strategy

`profiles.py` defines explicit source encodings; the operator does not reverse
engineer paths while writing mappings.

| Profile | Path | Repetition identity | Dictionaries |
|---|---|---|---|
| `rps_dual_full_chain_v1` | configurable separator, normally `:` | full aligned `li`/`pi` chain | `_codes`, `_shortcuts` document dictionaries |
| `ctti_ibm_legacy_v1` | `/`, leaf to root | scalar local `li`; source nodes must remain in contiguous depth-first preorder | `ArchetypeCodes` rows and `CodedValues` rows; packaged key shortcuts |

The legacy profile expands `$>` values and the local node prefixes
`D → at000`, `C → at00`, `B → at0`, `A → at`. These settings are explicit, versioned
code, and can be overridden by an exact exported dictionary. Specialized roots
such as `B.0.2 → at0.0.2` are expanded before identifier validation. Unknown codes,
ambiguous attachments, missing parent instances and invalid ordering fail. The
legacy source does not carry complete ancestor indexes: **arbitrary node reordering
cannot be supported losslessly**. Current RPS full-chain inputs can be reordered.
The old operational profile is left unchanged.

Some legacy path tokens disagree with the stored payload node identifier. The
adapter uses paths and indexes for structural reconstruction and preserves the
payload identifier. Both input routes reject contradictory `archetype_node_id`
and `archetype_details.archetype_id` declarations unless the pinned rule explicitly
permits the [OPT-bound copied-composition metadata policy](openehr-analytics-source-compatibility.md).
That policy preserves source values and records the exception once in `source_refs`.
Other identity conflicts remain rejected. No reference clinical mappings are approved
by selecting a source profile. Author, preview and approve each exact template.

Standalone workers may export the resolved dictionary snapshot into a portable
configuration. Hosted environment sources instead load their linked dictionaries. It contains reference dictionaries, not credentials or compositions:

```sh
python -m kehrnel.engine.domains.openehr.analytics.cli --env .env.analytics.local \
  --config examples/openehr_analytics/config.ibm.json pin-config \
  --output /private/tmp/ctti-analytics-pinned.json
```

Use that pinned JSON as the strategy configuration. Python partition workers can
instead load the same dictionaries once from the configured source database.
Dictionary digests are recorded in provenance and pinned for resumable backfills.

## What the collections contain

Names below use the default `analytics_` prefix. This is a current-state projection,
not a copy of every historical composition version.

| Collection | Purpose |
|---|---|
| `facts` | One reviewed clinical block instance with small nested business values and the query context |
| `activity` | One current composition: owner, provider, template, clinical time and selected composition metadata |
| `patients` | Separately supplied patient metadata snapshots; never automatically copied into facts |
| `source_refs` | One source/mapping/dictionary provenance record per composition |
| `rule_revisions` | Immutable rules, canonical units, field paths, block definitions, epoch and activation time |
| `projection_state` | Last source version, rule version, outcome and count receipt for idempotency |
| `activity_rollups` | Daily composition-count deltas by provider/template and fixed counter partition |
| `jobs` | Historical backfill specification, cursor, lease and progress |
| `streams` | Incremental consumer lease and acknowledged resume token |
| `issues` | Processing diagnostics; no copied composition payload |
| `model_binding` | Source namespace and immutable counter partition policy |

Facts retain small IDs linking them to their composition and detailed provenance.
`quality` is omitted when it adds nothing. Conversion evidence, null flavours and
missing selected values remain explicit. Shared source paths are in the rule catalog,
not repeated beside every ordinary scalar. Patient snapshots are updated through
`store.upsert_patient()` with an explicit BSON `snapshot.as_of` timestamp.

Activity counts **compositions**, not visits, admissions, encounters or facts.
An admission can have several compositions. Encounter counts require reviewed
encounter identifiers and their own definition. Fact expiry does not delete activity
history or decrement composition counts. The receipt's `fact_count` records the
projection's output count, not a live count after TTL deletion.

Daily counters default to 64 deterministic partitions so workers do not all update
one provider/day document. Sum `metrics.compositions` across partitions to obtain
that day's count. This is not MongoDB sharding. Changing this partition count in an
existing target is rejected; create a new target and rebuild if it must change.

## Clinical rules and ENTRY types

Each immutable revision binds one exact template name/version. It may additionally
restrict `source_template_ids` to approved source template UUIDs. Different source
versions can use the same stable business variables when their meanings and units
agree. Reusing a variable key with a different type or canonical unit is rejected.

- **OBSERVATION:** EVENT is the normal fact boundary; a reviewed CLUSTER can be a
  finer block, for example an analyte. EVENT time and INTERVAL_EVENT width/statistical
  meaning are retained. Systolic and diastolic from one event stay together.
- **EVALUATION:** the clinical assertion ENTRY, or a reviewed repeated CLUSTER.
- **ACTION:** the action ENTRY or reviewed CLUSTER. Recorded `ism_transition`
  is retained; a missing current state is reported as missing, never invented.
- **INSTRUCTION:** the planned ENTRY, ACTIVITY or reviewed CLUSTER. Planned intent
  and recorded timing are retained; these are not treated as performed actions.
- **ADMIN_ENTRY:** the administrative ENTRY or reviewed repeated CLUSTER.

Every configured field declares ownership: inside its block, or an explicit
qualifier inherited from the **same ENTRY instance**. Protocol is not composition
context. It is optional selected interpretation data from its owning ENTRY. Parallel
state HISTORY requires a separate time-alignment design and is rejected as an
implicit inherited binding. A scalar ELEMENT cannot be selected as a new fact
boundary. Runtime validation checks the actual source RM type as well as the rule.

Required subject identities, interpretation values and mapped timestamps cannot be
silently omitted. A repeated source binding that would produce multiple values for
one scalar fails; it is never zipped with a different repeat by array position.
For related people, such as a fetus, require an explicit subject binding. The EHR
owner is not automatically assumed to be the clinical subject of a related-party
ENTRY.

Rules are data: JSON configuration stored in `rule_revisions`, with a digest and
monotonically increasing per-template epoch. Typed unit conversions support explicit
positive scale and optional offset; strict decimal parsing is optional. The mapping
reviewer must establish semantic and unit compatibility. There is no runtime LLM
inference, automatic SNOMED expansion, arbitrary executable expression, or silent
clinical imputation in this converter.

## Rule activation and historical changes

`effective_at` determines which revision applies to a new event's stable
`accepted_at`. Persist/reuse that event timestamp on retries; do not replace it with
`now()` every time. Clinical event time, source commit time and rule activation time
have different meanings.

A rule revision does not itself start a billion-document scan. For historical
changes create a new `Backfill` with an exact template, a pinned rule digest and
optional half-open ID/committed-time bounds. The job's accepted time, dictionaries,
source field profile, bounds and revision cannot change when it resumes. A new
configuration or template version gets a new revision/job, not an edited old receipt.

Initial migration can use `template="*"`, pinning the complete rule-set digest.
Unmapped templates still emit activity and `needs_rules` receipts. The
`analytics_coverage` operation lists these by template in bounded pages. After rules
are reviewed, run a template-scoped backfill to replace those outputs with facts.

```python
job = Backfill(job_id="bp-revision2-partition0001",
               template="Analytics Blood Pressure.v1",
               rule_revision=approved_revision,
               accepted_at=fixed_job_time,
               source_namespace="my-cdr",
               lower_id=lower_inclusive, upper_id=upper_exclusive,
               batch_size=64)
run_backfill(source_collection, store, converter, job)
```

The scanner uses the exact template and `_id` keyset bounds, not `$skip`. Request a
source index such as `{template:1, _id:1}` from the source operator; this code does
not modify source indexes. UUID bounds must use BSON UUIDs, not their printed strings.
An ID cursor is a scan mechanism, **not an insertion-time watermark**. Capture a
replayable change stream before the initial scan and replay it afterwards. Otherwise
late inserts behind the cursor and concurrent changes can be missed.

See `examples/openehr_analytics/python_worker.py`. Assign disjoint ranges and
stable job IDs externally; retries must reuse the same job definition and pinned
configuration. Python worker concurrency, distribution of ranges, skew, source throttling and shard-key design
remain deployment decisions. No implementation here measures production capacity
from the synthetic examples.

## Publication, retries and incremental input

`store.prepare()` creates only destination collections/native indexes and stores
rule revisions. Use a replica set or sharded MongoDB deployment: atomic transactions
are required. There is no fallback to partially updating related collections.

Publication replaces a composition's prior facts, activity, source reference and
receipt and updates composition-count deltas in one bounded transaction. Removed
fields/facts disappear on reprocessing. An exact retry has no count effect. A
changed payload at the same source version is rejected. Concurrent advancement of
source and rules in opposite directions stops the job so it can reread the latest
source under an appropriate revision; it does not acknowledge a silently lost update.
Source version ordering supports positive **linear** openEHR version numbers;
branched version-tree reconciliation is outside v1.

`process_events()` uses this same converter/store and checkpoints a resume token
only after acknowledged publication. Call it on ordered finite event batches with
an exact versioned full document and stable accepted time. `mongo_events()` adapts
MongoDB events opened with `full_document="required"`; source update post-images
must be enabled by the source operator. `updateLookup` is deliberately not used
because it can return a later version than the event. Hard deletes need an explicit
versioned tombstone from the source integration, not an invented version number.

Leases prevent cooperative workers sharing one job/stream concurrently. If a worker
loses its lease after publication, the next worker safely replays from the previous
checkpoint. The external supervisor handles stream history expiration, outages and
restarting workers. A rule change does not automatically restart that supervisor or
edit a Healthcare Data Lab deployment.

## Retention and indexes

Default retention is `{"mode":"retain"}`. Optional
`{"mode":"delete_facts","clock":"clinical_time","days":1825}` sets BSON
`expires_at`; committed-time expiry is also supported explicitly. Days mean elapsed
24-hour periods, not calendar years. Clinical-time expiry refuses a fallback source
commit timestamp. MongoDB TTL deletion is asynchronous. **TTL does not archive.**
Archive/export, legal retention and coordinated disposal of source references are
separate deployment workflows. Changing TTL rules affects old facts only when their
compositions are reprocessed.

The plan creates native indexes for composition replacement, patient/subject/kind
history, kind/time, provider/time, mapping coverage, rule epochs and leases, plus the
optional-use fact TTL index. It does not provision Atlas Search or Vector Search.
Those serving indexes and analytics query APIs remain a separate integration with
the demonstrated lab; clinical conversion does not require embedding every fact.

## Validation boundary

Unit tests cover actual RPS writer/decoder parity, repeated parents, BP pairing,
ENTRY interpretation, protocol ownership, intervals, conversions, required values,
rule activation, UUIDs and stale/conflicting receipts. Strategy contract tests cover
pack discovery and SDK hooks. Opt-in MongoDB tests exercise atomic replacement,
rollback, historical field removal, keyset resume and event replay using uniquely
named synthetic collections and exact cleanup paths.

```sh
PYTHONPATH=src pytest tests/unit/openehr_analytics
ANALYTICS_TEST_ENV_FILE=.env.analytics.local PYTHONPATH=src \
  pytest tests/integration/openehr_analytics
```

A bounded source dry run verifies compatibility only for the sampled records. It
is not a clinical reconciliation against IBM/Oracle, coverage of every template,
proof of lossless reconstruction of every source variant, or a throughput claim at
one billion compositions. Source-version immutability, legacy node ordering, mapping
review and incremental capture must be verified during deployment qualification.


## Generating an initial field selection

Use the [all-clinical-values default workflow](openehr-analytics-defaults.md) to
produce a per-OPT proposal, inspect its fact boundaries and deselect fields. This
adds offline `propose-defaults` / `finalize-defaults` commands and the
`analytics_default_mapping` strategy operation. Generated proposals do not activate
rules, repair source records, or change the Healthcare Data Lab UI automatically.

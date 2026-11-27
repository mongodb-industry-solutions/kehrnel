# openEHR analytics: maintenance and release runbook

Companion to the [integration guide](openehr-analytics-integration.md),
[rule reference](openehr-analytics-rule-reference.md) and
[existing HDL API contract](https://github.com/mongodb-industry-solutions/HealthcareDataLab/blob/main/docs/integration-api-analytics-releases.md).

Deploy the matching Kehrnel and HDL releases for the GUI and approved configuration API. Continuous capture, durable quarantine, worker acknowledgements and reconciliation remain executor responsibilities. Follow the [GUI and worker workflow](openehr-analytics-integration.md).

## 1. Separate the identities

| Identity | Changes when | Operational action |
|---|---|---|
| Source namespace + composition ID/version | A source record is created/corrected | Route ordered versions to the same worker partition; retain exact event image |
| Exact template identity + OPT digest | A new template/version or different OPT appears | Resolve authoritative OPT; no fuzzy version substitution |
| Authored HDL revision | A curator saves an edit | Review the draft; saving is not release approval |
| Rule epoch + revision | An approved clinical transformation changes | Increase per-template epoch and effective time; retain earlier revisions |
| Configuration release digest | A consistent collection of rules/dependencies is published | Pin workers/jobs to that release |
| Decoder revision | Source decoding profile/dictionary snapshot changes | Compatibility review and explicit replay/reprocessing policy |
| Projection `schema_version` | Physical interpretation contract changes | Upgrade interpreter/migrate as designed; current interpreter supports only `1` |
| Code/wheel + strategy manifest digest | Implementation changes | Build, validate and retain the exact artifacts |
| Backfill job ID + input boundary | A historical scope is approved | Resume that immutable job; changed scope needs a new job |

Editing a field selection is normally a new rule revision, not a new physical schema
version. Hiding a workbench column is presentation only and changes none of these.
Business keys are stable semantic identifiers; changing meaning or units may require
a new key and coordinated query/catalog changes, not a cosmetic rename.

## 2. Curator workflow in Healthcare Data Lab

1. Select the analytics environment and exact template/OPT. Show missing, conflicting
   and ready templates separately. A source template without rules remains a visible
   coverage gap; source metadata contradictions remain source issues.
2. Choose a suggested, validated fact block. Then select its fields and interpretation
   context. Protect dependencies such as time, related subject and required units.
3. Use the supported conversion controls, with a before/after example. Review full
   typed values and explicit nulls; do not treat absent values as zero.
4. Preview the edited rule on approved samples, including repeats/subjects and errors.
   Show actual fact, activity and source-reference output, selected/unmapped paths
   and intended removals. Any edit invalidates the previous preview approval.
5. Approve an immutable revision and choose **from an activation boundary onward** or
   **from that boundary onward plus an explicit historical scope**. Review any retention
   deletion separately. Show source committed-time bounds accurately.
6. Publish a release, distribute it before its effective boundary and track loaded
   workers. Then track historical jobs independently. The API key is used to read the
   release; user/operational credentials authorize publication and job control.

The initial default is broad clinical coverage from the exact OPT, with unresolved
choices blocked for review. It is not permission to promote every tree node to a fact
or manufacture subject identities. The approved saved representation is JSON rules;
UI widgets and any future YAML import must compile to that same validated contract.

## 3. Common changes and their consequences

| Change | New incoming events | Existing projected compositions |
|---|---|---|
| Add/remove a selected field | New rule after its effective boundary | Explicit scoped backfill; replacement removes deselected output |
| Change a fact boundary or repeat policy | Compile and preview all correlation/subject cases before approval | Reprocess affected templates; old fact IDs may be replaced, counters must reconcile |
| Change business meaning, type or canonical units | Validate semantic compatibility; use a new business key if required | Plan query/catalog migration and backfill; do not silently mix meanings under one key |
| New exact template/version | Require its OPT and approved rule | Backfill held `needs_rules` records after approval |
| Different OPT under the same template name | Hold ambiguous identity until authoritative version/source binding is known | Reconcile affected source periods; do not pick the first file by name |
| Prospective-only change | Apply by stable captured `accepted_at` | Leave previous outputs until corrected source events or an explicit backfill |
| Historical rule change | Keep compatible incoming processing active under the agreed release | New immutable backfill job, explicit revision, committed-time/ID bounds and reconciliation |
| Retention change | Set approved expiry on newly projected facts | Backfill required to alter existing expiry; TTL is asynchronous deletion |
| Dictionary change | Pin a new decoder snapshot per release/worker | Review digest/replay compatibility; never reload mid-job |
| Source correction | Consume a newer exact version | Atomic replacement; retries do not duplicate facts or composition counts |
| Remove a template from the catalog | Alert/hold future unsupported records according to policy | Do not infer permission to delete data from inventory disappearance |
| Roll back rule logic | Reissue old logic at a higher epoch | Explicit reprocessing; restoring an old activation does not undo output |

Current historical date bounds refer to source committed time. A clinical-date
backfill selector is a separate implementation requirement. Prospective rule choice
uses the source event's stable accepted time, so a later correction of an old clinical
record can use the new rule. Retain old rule revisions for delayed/replayed events.

## 4. Retention and calculations

Implemented retention forms are `{"mode":"retain"}` and:

```json
{"mode":"delete_facts","clock":"clinical_time","days":1825}
```

The alternative clock is `committed_time`; supported bounds are 1–36500 elapsed days.
1825 elapsed days is not a calendar-aware “five years” calculation. UI wording must
make this explicit. TTL deletes facts; it does not archive them, remove activity,
decrement composition counts or delete the operational source. A recorded projection
receipt counts facts generated, not the number still present after TTL.

Supported quantity conversion applies an explicitly configured scale and optional
offset, with exact source/target unit checks. Strict numeric-text parsing is also
supported where configured. A curator must approve dimensional/clinical meaning.
Generic formula evaluation, BMI calculation, inferred clinical diagnoses and automatic
archiving are not part of the current rule language. Do not offer an unrestricted
formula editor or label a projection error as a clinical quality assessment.

## 5. The integration team's daily and continuous responsibilities

**Configuration supervisor:** conditionally poll the existing HDL read API, verify
environment, candidate consistency and supported clinical adapter, retain complete
approved artifacts, compare against the applied baseline, and distribute changes.
Track downloaded, loaded, active and historically reconciled states separately.
A daily fetch must not silently activate an unreviewed draft or start repeat backfills.

**Continuous ingestion:** keep durable capture running, route versions in order, use
exact post-images and stable accepted times, process bounded batches, and record
acknowledged positions only after successful publication or approved durable quarantine.
Monitor capture retention independently of destination lag; a stored resume token alone
cannot recover an expired source history. Plan versioned deletion tombstones explicitly.

**Historical workers:** pin immutable job/config/decoder identities, scan exact scopes,
checkpoint acknowledged batches and resume rather than restart with changed parameters.
When source versions and rule epochs advance in opposing order, resolve the conflict
under the appropriate newer rule; the helper does not reconcile this race automatically.

**Destination operations:** verify transactions, native indexes and separately provisioned
Search readiness. Monitor write failures, TTL behavior and storage. Keep source connection
credentials distinct from target credentials. Patient metadata is a separate governed
feed with its own snapshot semantics; source compositions do not magically provide it.

**Clinical review:** resolve held templates, related subjects, repeat ambiguity and
unmapped values. Changes to clinical meaning require preview and approval, even when
an automated compiler accepts the shape.

## 6. Required operations screen and incident handling

Show, per template/job/worker scope:

- Approved rule epoch/revision, OPT and decoder digests; loaded and active worker release.
- Capture position/lag, processed position, failed/held counts, `needs_rules` and zero-fact
  outcomes separately. A rejected composition must never be displayed as “zero facts”.
- Historical scope, pinned revision, checkpoint, completed/replaced/skipped outcomes,
  and unresolved source/rule races. Show mixed historical revisions explicitly.
- Native/Search desired definitions and actual readiness, plus serving freshness.

Current helpers stop on errors. Before adding quarantine-and-continue, make the failure
record durable with namespace, composition/version, source fingerprint, token, template,
rule/decoder identities and exact replay reference. Define whether an earlier projection
is retained and how stale output is disclosed. A log line is insufficient to skip an event.

| Incident | Required response |
|---|---|
| Missed configuration poll | Catch up before crossing the affected activation boundary; no `now()` substitution |
| Expired/revoked key or paused environment | Resolve credential/pause state; do not bypass it using runtime admin access |
| Interrupted bundle download | Discard incomplete candidate; keep last verified job snapshot and freshness policy |
| Source identity contradiction | Hold evidence for IBM; no automatic archetype replacement |
| Source/rule epoch race | Schedule controlled reconciliation; do not discard the newer source event |
| Expired capture history / invalidation | Stop claiming complete coverage; plan a bounded reconciliation/rebaseline |
| TTL deleted needed facts | Rebuild from retained source under an approved retention rule, if source still exists |
| Restore destination backup | Restore control state coherently, check replay horizon and reconcile before resuming |

## 7. Handover acceptance

Before unattended operation, demonstrate initial scan plus replay, late UUID insert,
correction, retry, crash recovery, deletion, exact rule-time boundary, missed poll,
retention expiry, same-name OPT ambiguity, missing template, related subjects, repeated
containers, source metadata rejection, rule rollback and dictionary change. Compare
approved clinical values and composition counts, not merely worker success messages.

Retain the wheel/config/OPT/dictionary artifacts, preview evidence, approval identity,
job input manifest/checkpoints, quarantine references and reconciliation results under
agreed access and retention controls. Do not put clinical payloads in general logs.
The sampled defaults and real-source checks support this supervised handover; they
are not validation of billion-record performance or a complete autonomous deployment.

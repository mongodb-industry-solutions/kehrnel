# openEHR analytics: GUI and Kehrnel workflow

`openehr.analytics` turns canonical compositions or supported operational RPS
representations into analytical facts, activity, source references and processing
receipts. The GUI, Kehrnel runtime and Python workers use the same rules and decoder.

## Two environments, two independent strategy configurations

An activation belongs to an environment and domain. The same strategy can be
configured differently in multiple environments. Keep operational RPS in the source
environment and activate analytics in a separate destination environment/database.
Selecting a source does not activate, deploy, index or write to that source.

1. In HDL **Environments**, configure the operational environment's database and
   connection. Associate its actual operational strategy (`openehr.rps_dual` or the
   entitled `openehr.rps_dual_ibm`) and collection/field settings. For an existing
   externally managed IBM database, use an **external execution association**; do
   not provision a managed operational deployment merely to read existing data.
2. For IBM legacy row dictionaries, in that source strategy's configuration set
   `sourceReader.dictionary_format = ctti_rows`, `collections.codes.name` to the
   actual archetype-code collection and `collections.shortcuts.name` to the actual
   coded-values collection. Typical names are `ArchetypeCodes` and `CodedValues`.
   Kehrnel document dictionaries use `kehrnel_documents` (the operational default).
   Configure `collections.compositions.name` and operational `fields` accurately;
   `sourceReader.fields` can override composition ID, patient, template, version and
   committed-time envelope names without changing operational writes.
3. Switch to **Test_Analytics**. Its existing connection and `database` are the
   output; all analytical collections and indexes are created there. A copied
   domain database mapping cannot redirect this output.
4. For a new environment, open **Strategy Studio → Browse Strategies → openEHR
   Analytical Facts → Review Setup → Configuration**. Once activated, use
   **Data Factory → Analytics Pipeline**. Choose **Read another environment**
   and select the operational environment. The server derives the encoding profile,
   collection, field names and dictionary layout. Save, review and apply analytics.
5. Alternatively choose **Upload compositions (JSON or ZIP)** for canonical
   composition envelopes. Preserve patient/composition IDs, ordered versions and
   timestamps. Each interactive upload is limited to 25 documents and 6 MiB expanded.
   Raw IBM exports require their source dictionaries; use the environment reader.

The source must belong to the same current workspace and be accessible to the
operator. A paused or removed source fails closed. The destination saves a pinned
profile and its digest; later source changes require refresh, review, save and apply.
Standalone workers use the reviewed exported snapshot until explicitly updated.
Credential rotation stays in the source environment and does not change clinical rules.

IBM `/` paths, `D1`/`C14` at-codes, dotted suffixes and local sibling indexes are
handled by Kehrnel. Do not split on dots, coerce tokens to numbers, or look up `D1`
in the archetype dictionary. Unknown codes or ambiguous node relationships fail
validation. The exact source dictionary must be available; unrelated bundled
archetype numbers cannot substitute for the database's code table.

## Configure facts and release rules

In the analytics environment, import exact OPT templates into **Data Model Catalog**.
Open a template's **Analytics** tab. Generate a proposal from its OPT, select fact
blocks and fields, review business variable keys, subject/time policies and retention,
and preview a representative sample. Record the review, save the draft and approve
an immutable release with an explicit effective date. Anna selects these facts in
the editor; the workflow does not derive rules from the AQL list.

Return to Analytics Pipeline, refresh approved rules and apply. Approval alone does
not install rules or scan historical data. Older and scheduled epochs remain in the
configuration for accepted-time selection and replay. The business variables view
shows the released model, not measured data completeness.

**Show index definitions** displays/downloads the proposed native MongoDB index plan.
**Prepare output and indexes** applies it to the linked destination. This facts
strategy does not provision Atlas Search/vector indexes. Operational RPS Search
indexes are a separate strategy concern. Patient demographics are a separate
patient feed (`upsert_patient`); composition facts do not invent demographic snapshots.

Test the source connection, preview a bounded sample without writes, then generate.
Check read/fact/held counts and receipts. A repeat of the same version and rules is
idempotent. Historical load requires an exact template, approved rule revision,
stable job ID and accepted time; GUI requests process at most ten batches of 25.
Use Kehrnel workers for sustained or large-volume ingestion.

## Juan's isolated worker

Download the complete GUI configuration, or use the **destination environment's**
integration key to read:

```
GET /api/integrations/v1/strategies/openehr.analytics/configuration?view=bundle
Authorization: Bearer <destination-environment-key>
```

Create the candidate config as `{**bundle["deployment"], "rules": bundle["rules"]}`.
`deploymentRevision` identifies retained source/output settings. Keep the complete
bundle/digest for conditional manifest reads and `POST configuration/delta`.
The earlier RPS `analytics-config?view=dictionaries` is not the analytical rule release.

The bundle contains no connection strings. Resolve the source environment reference
locally in the worker's secret store. The CLI uses `ANALYTICS_SOURCE_URI` and
`ANALYTICS_TARGET_URI`; database, collection, namespace and prefix come from the
reviewed config. Conflicting local settings are rejected. Install this Kehrnel version
with its MongoDB dependencies and use the supplied converter; HDL need not be a
clinical-data gateway.

```sh
# Local secret file contains ANALYTICS_SOURCE_URI and ANALYTICS_TARGET_URI only.
kehrnel-openehr-analytics --env .env.analytics.local --config analytics-configuration.json plan
kehrnel-openehr-analytics --env .env.analytics.local --config analytics-configuration.json prepare
kehrnel-openehr-analytics --env .env.analytics.local --config analytics-configuration.json backfill --job job.json --max-batches 10
```

For Python use [python_worker.py](../examples/openehr_analytics/python_worker.py).
For an independent Kehrnel runtime, bind destination `db` and
`extras.analytics_source = {connection_ref: config.source.connection_ref, uri: <local secret>}`.
Use `analytics_source_from_strategy` to derive the same profile directly from an
operational strategy configuration; it performs no I/O. HDL-hosted Kehrnel resolves
both environment secrets and verifies owner/profile consistency before reading.

## Lifecycle and deployment boundary

The source account should be read-only. The destination needs collection/index
creation and transaction support (replica set or sharded deployment). Jobs retain
cursor, source/rule/decoder identities and resume only compatible work. Publication
atomically updates facts, activity, provenance and processing state. Changes to the
same source version are rejected rather than silently overwritten.

Continuous capture, durable queues, worker scheduling and deletion event semantics
belong to the executor. Approving a new rule does not automatically backfill.
Production throughput and clinical reconciliation require representative testing;
synthetic acceptance is proof of integration, not clinical approval.

Deploy the Kehrnel PR first, then HDL. Existing environments and operational
activations are not migrated or replaced automatically. See the
[rule reference](openehr-analytics-rule-reference.md),
[maintenance contract](openehr-analytics-maintenance.md), and
[source compatibility policy](openehr-analytics-source-compatibility.md).

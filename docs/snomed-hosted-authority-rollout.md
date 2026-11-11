# SNOMED hosted authority rollout

New activations default to `source.mode: hosted_reference`, `source.id: healthcaredatalab`. The server registry, not tenant configuration, binds this identifier to the hosted catalog. The source is pinned to release `20260601` and the current qualified projection. `database` is separate tenant storage for ValueSets, reviews, gold notes and usage; it must never resolve to the authority database. Existing activations without a source mode retain customer-owned behavior until explicitly migrated.

## Installation and credentials

Install the source-policy, retrieval, runtime seed protection and evidence contracts together. Supply `SNOMED_AUTHORITY_URI` privately to the Kehrnel server using a MongoDB user with read permission only on the authority. The temporary qualification reused an existing connection; it does not prove that a dedicated production reader has been provisioned. Do not store a URI in tenant configuration, browser settings or an exported database. `KEHRNEL_SNOMED_SOURCES_JSON` optionally overrides the operator-owned descriptor registry; it is server configuration.

Hosted consumers can query the canonical concepts and term projection. Catalog writes, seed copies, ingestion, release replacement, projection/index maintenance and writes hidden in aggregate pipelines are rejected. Credentials and database mappings are not part of authority evidence. Customers obtain their own authorized Ministry JSON distribution and select `customer_owned`; they do not receive a copy of this hosted catalog. RF2 preparation remains a later implementation stage.

## Demo cutover

1. Reconcile the prepared source delta with the current checkout under the repository workspace guard. Preserve other writers' changes and capture a fresh private activation/configuration rollback checkpoint.
2. Deploy the tested Kehrnel implementation and install the private reader binding. Confirm the server discovers the matching pack manifest/schema and source policy; do not update tenant bindings while an older engine is still running.
3. Replace the HealthcareDataLab SNOMED activation through the supported activation API, with separate `hdl-team_snomedct` workspace storage and `source: {mode: hosted_reference, id: healthcaredatalab}`. Runtime readiness must identify the authority before HDL records the new activation link. Reconcile any old local/deployed activation ID difference by readback, not by copying an old ID.
4. Set the HDL demo environment consistently to `env-1768371326510`; deploy the matching HDL proxy/readiness changes. Verify terminology search, hierarchy/ECL, FHIR cohort and openEHR cohort receipts identify the same authority/release/projection. Verify hosted mutation requests return 403 and other tenants remain untouched.
5. Retain the old empty target through qualification. Remove it only after source, runtime registry, deployed jobs and tenant settings contain no remaining references. Rollback restores the old activation and application versions together; never copy the authority into the old target as a recovery shortcut.

## Projection maintenance

`scripts/snomed/build_projection.py` defaults to a bounded read-only plan. Run it from this repository with `PYTHONPATH=src`, a configured reader binding and a new `--projection-id`. The current 129,536 terms are a selected projection. A 100-concept sample estimated around 1.79 million full terms; that ordered sample is not representative capacity proof.

A reviewed `--apply` requires a separate `SNOMED_PROJECTION_OPERATOR_URI` on the same authority cluster and creates a new versioned collection and a build receipt. It refuses an existing target. It does not publish the projection or provision Search indexes. Verify coverage, concept identities, languages, hierarchy fields, indexes, EN/ES relevance and concurrent performance, then change the operator descriptor in a separate maintenance action. Preserve the previous projection for rollback. Failures leave the staging collection for inspection.

## Remaining data work

Establish the owner/provenance of existing grounded notes and usage currently in `terminology` before migrating them to tenant workspaces. Update the standalone application's writers/readers as well as Kehrnel. Keep canonical concepts, term projections and operator receipts in the authority. Review Atlas Search CPU/memory, concurrency and index sizing before building full coverage or choosing a cluster tier. Quiet cluster metrics and a few successful queries are not a sizing study.

## Qualified local workspace rollout, 6 November 2026

The signed-in HealthcareDataLab demo environment now uses the prepared isolated runtime for all six strategy domains. Its SNOMED activation reads the existing hosted authority and keeps mutable terminology workspace data separate. Native and FHIR implicit ValueSets share ECL resolution without tenant registration. The heart-failure set expands to 120 codes with displays, and both FHIR hierarchy predicates return the ten matching Conditions. Bilingual lexical, lookup, validation, subsumption, ECL and hybrid paths are qualified. Incomplete pages cannot become partial clinical filters; standard FHIR expansion reports pagination truthfully.

The original backend and public routes remain separate. The owner keeps a private, credential-free launcher and encrypted rollback checkpoint outside the export. Credentials are resolved only in memory from existing encrypted configuration. The current connection is an existing operator connection; a dedicated database reader and deployed/public rollout remain owner work. Browser automation was disabled, so qualification used the frontend’s actual server-side resolver and live runtime calls.

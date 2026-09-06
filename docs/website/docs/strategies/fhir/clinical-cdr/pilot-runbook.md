---
sidebar_position: 6
---

# Supported Pilot Runbook

This runbook defines the current operational boundary for a supported
`fhir.clinical_cdr` pilot. It is an accelerator deployment, not a claim of a
complete or certified FHIR server.

## Recommended topology

- Run Kehrnel inside the customer's infrastructure and network boundary.
- Give every activated strategy its own reviewed MongoDB database name.
- Keep the core database for transversal environment, activation, and access
  metadata—not FHIR resources or migration payloads.
- Expose Kehrnel through Healthcare Data Lab or a partner-controlled gateway;
  do not expose an unauthenticated runtime directly to the internet.
- Store secrets in environment bindings or a secret manager, never in the
  activation document.

Minimum service configuration:

```bash
export KEHRNEL_AUTH_ENABLED=true
export KEHRNEL_API_KEYS='<managed-secret>'
export KEHRNEL_API_KEY_ENV_SCOPES='<key-to-environment-policy>'
export CORE_MONGODB_URL='mongodb://core-service/...'
export CORE_DATABASE_NAME='hdl_core'
export KEHRNEL_FHIR_IG_STAGING_ROOT='/var/lib/kehrnel/fhir-ig-staging'
export KEHRNEL_FHIR_IG_UPLOAD_MAX_BYTES=33554432
export KEHRNEL_FHIR_IG_STAGING_MAX_BYTES=536870912
```

The activated binding must point to the dedicated FHIR database. Review both
the binding and `config.database` before activation; they should describe the
same intended tenant strategy store.

## Preflight evidence

1. Activate FHIR Core first. IGs, profiles, and semantic pipelines may remain empty.
2. Read `/metadata` and `/capabilities`; confirm the selected release and support tier.
3. Download `/support-matrix?format=markdown` and attach it to the pilot decision record.
4. Inspect `fhir_index_manifest`; resolve budget violations before importing.
5. Run a dry-run migration with representative de-identified records.
6. Verify the database name and collection prefix in the returned report.

For R4, the current evidence must say **minimal** and list only Patient and
Observation. Do not reinterpret that as general R4 conformance.

## Migration procedure

Use the Data Lab Migration Workbench or the equivalent domain endpoints:

1. Select a Bundle or NDJSON source. The file stays with the client.
2. Choose a bounded chunk size; begin with 250–500 resources.
3. Validate first. Review all error findings before a write run.
4. Start the write run in `upsert` mode unless create-only conflict behavior is required.
5. Observe checkpoint, valid/invalid/written totals, and per-chunk reports.
6. Resume from `checkpoint.next_chunk` after a transient failure.
7. Run the informational reference-integrity report.
8. Exercise representative FHIR searches and inspect their generated MQL.
9. Save the migration report and runtime support matrix with the project evidence.

An exact retry of a completed chunk is replayed from its stored report. A
different payload at the same completed checkpoint is rejected. A failed chunk
may be corrected and retried; its previous bounded attempt metadata remains in
the chunk audit record.

Cancellation is cooperative. Kehrnel checks the cancel marker while validating
and projecting a chunk, and always stops before accepting the next one. Smaller
chunks therefore improve cancellation latency and retry granularity.

## Backup and rollback

Take a MongoDB snapshot or `mongodump` of the dedicated FHIR database before a
material migration. Record the activation id and projection contract version
alongside that backup.

Kehrnel does not automatically delete or roll back canonical resources. For the
safest rollback, restore the pre-migration snapshot into a new database, verify
it, and activate the strategy against that reviewed database. Use migration-run
provenance for targeted investigation; do not delete by provenance without a
customer-approved retention and referential-impact procedure.

## Current operational limits

| Area | Current behavior |
|---|---|
| Request body | 25 MiB default (`KEHRNEL_FHIR_IMPORT_MAX_BYTES`) |
| Resources per import call | 10,000 default; clients should use smaller chunks |
| Migration history | Tenant FHIR database; source payload is not retained |
| Stored findings | First 250 per chunk; total count and truncation flag retained |
| Reference check | Relative references only; informational, bounded, non-mutating |
| IG upload | 32 MiB per archive and 512 MiB staged per environment by default |
| Profile conformance | Not implemented or claimed |
| Semantic execution | Extraction preview only until provider and vector adapters are configured |
| Recovery | Client resumes from the persisted chunk checkpoint |

## Customer-managed deployment example

The customer operates MongoDB, Kehrnel, and its gateway in its own cloud or data
center. Healthcare Data Lab is the enablement and operations portal. Application
teams call the same Kehrnel FHIR endpoints after the Lab has proved storage,
search translation, and migration behavior. The customer owns availability,
backups, access policy, monitoring, and the additional FHIR interactions its
application requires.

## Partner-productized example

A partner packages Kehrnel with deployment automation, SMART authorization,
terminology, profile validation, observability, support, and service-level
commitments. The partner publishes the generated runtime matrix as its precise
accelerator baseline and separately documents every added conformance feature.
Healthcare Data Lab remains useful for configuration, evidence, migration
rehearsal, query development, and operational diagnostics.

## Exit criteria

A pilot is ready for customer evaluation when a clean environment can complete
activation, dry run, checkpointed import, read/search round trips, MQL inspection,
reference reporting, and support-matrix export using de-identified customer-like
data. Production approval still requires customer-specific security, recovery,
performance, and conformance acceptance.

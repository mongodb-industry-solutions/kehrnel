# Deployment runbook

1. Install `kehrnel-core[api,mongo,cdisc]` for filesystem artifacts, or add
   `cdisc-cloud` for S3/Azure Blob.
2. Store the resolved binding document in the deployment secret manager and
   expose only its `bindings_ref` to Kehrnel. Never commit credentials from the
   production example.
3. Activate `cdisc.sdr`, then invoke the runtime `plan` and `apply` capabilities
   to create collections, compound indexes, and enabled Atlas indexes.
4. Register the licensed standards/rule manifest and its checksum-addressed
   artifacts before enabling `validation.require_registered_standards`.
5. Submit large generation or package operations through
   `POST /v1/environments/{env_id}/synthetic/jobs`, explicitly setting the
   strategy operation. Set `KEHRNEL_JOBS_RECOVERY=retry` only for idempotent
   operations; the safer default marks interrupted jobs failed.
6. Monitor validation runs and lineage before publishing. Publication is one
   snapshot-marker update and automatically rebuilds configured projections.

Recommended production controls include a private object-store endpoint,
server-side encryption, object retention/versioning, MongoDB backups, Atlas
Automated Embedding governance and billing controls, and an approved validator
executable/container. Regulator acceptance and validation
of the deployed computerized system remain organizational responsibilities.

## Long-running CDISC job example

```json
{
  "domain": "cdisc",
  "op": "cdisc_generate_synthetic_study",
  "payload": {
    "recipe": {"studyId": "SYNTH-SEND-001", "profile": "send", "subjects": 1000, "seed": 42},
    "persist": true,
    "validate": true,
    "publish": true
  }
}
```

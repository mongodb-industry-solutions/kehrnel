# Portable deployment configuration

Healthcare Data Lab owns tenant authoring, publication, integration keys and downloads. Kehrnel owns compilation of the strategy's storage and projection contract. An external projection writer consumes an immutable release; it does not use HDL as a query or ingest gateway.

## Compiler API

`POST /strategies/{strategy_id}/deployment-config/compile` supports `openehr.rps_dual` and `openehr.rps_dual_ibm`. The endpoint requires the existing admin API-key check. HDL’s internal compiler connection must use an authorized admin service key when admin keys are separated. Compilation receives supplied configuration only, requires no environment activation or target bindings, and makes no target database connection.

```json
{
  "version": "<installed exact version>",
  "packageDigest": "sha256:<installed package digest>",
  "config": { "database": "catsalut_cdr" },
  "templates": [
    {
      "templateId": "Example",
      "fields": [
        { "path": "/context/start_time/value", "rmType": "DV_DATE_TIME" }
      ]
    }
  ],
  "dictionaries": {}
}
```

HDL always pins `version` and `packageDigest`; a changed installation returns 409 rather than silently compiling a different strategy. The standalone compiler permits omission for inspection. The request schema is [deployment-compilation-request.schema.json](deployment-compilation-request.schema.json).

The response contract `kehrnel.deployment-configuration/v1` includes:

- Exact request input and input digest, verified by HDL before publication.
- Exact strategy version, manifest/package digests and an implementation digest incorporating the shared RPS code and compiler source. Compilation uses the installed IBM defaults and native IBM flattener, including UUID IDs, envelope fields and the shortcut value prefix. It does not substitute a development profile.
- Portable configuration with selected analytics mappings, dictionary seeds and Search definition inline. There are no caller file references, database credentials or bindings.
- Collection requirements, ordered B-tree keys as `[field, direction]` pairs, all options, index names and per-index digests. Optional indexes appear with `autoCreate: false` and their requirement metadata.
- One Atlas Search definition for the enabled physical Search collection, combining the selected templates' analytics fields. It preserves `extract`/`valuePath`, shortcuts, field aliases and ID encodings. Search disabled means no Search collection indexes. Invalid, broad or scalar/document-conflicting analytics fail with 422; export does not fall back to a dynamic index.
- Projection mappings, compiled rules, code book, shortcut keys/values, encoding profiles, algorithm version and readiness diagnostics.

`readiness.indexes` means a complete plan was compiled. `readiness.projection` checks that selected analytics paths can compile against the supplied dictionaries. `readiness.runtime` is `not_observed`: this API does not prove a target database, Atlas capability, credentials or external writer. A writer must validate its deployment and test projection parity separately. A package with incomplete dictionary coverage remains useful for index inspection, but its writer must resolve diagnostics before execution.

## Dictionaries and external execution

For existing encoded compositions, supply the exact immutable codes and shortcuts used by that store. Do not allocate new numeric codes independently on each server. Inline `codes` accepts the native dictionary document (`_id`, RM/name/version subtrees and `at`) or a canonical `ar_code`/`at` code book for an external writer. `shortcuts` contains `keys` (or `items`) and `values`. Omitted snapshots resolve only the installed manifest's curated seed. Live catalog/database references require an explicit snapshot.

Configuration fingerprints use the existing Kehrnel canonical JSON subset: sorted ASCII object keys, strings (including Unicode values), arrays, booleans, null and safe integers. Float configuration values and Unicode object keys return 422. Compound index order is carried by arrays rather than object ordering. Clinical data, including floating point measurements, is outside this configuration contract.

The synthetic [IBM projection example](examples/deployment-projection-ibm-example.json) verifies the installed UUID and envelope profile. The generic [projection example](examples/deployment-projection-example.json) provides canonical input and expected flattened/Search documents in MongoDB Extended JSON, including BSON date handling. The contract test rebuilds Search documents from the exported projection settings and verifies the same output as ingest. It is an example of compatibility, not a substitute for CatSalut's representative fixtures.

The external writer is responsible for provisioning collections, checking required index options and Search support, applying an explicit optional-index policy, and rebuilding projections when analytics or encoding changes. Do not automatically delete indexes or replace active projections when a new release appears. Keep the previous release until the new projection and indexes are validated. In a future isolated Kehrnel/HDL installation, the same compiler and release contracts can be used locally; execution credentials remain local.

The production PR preserves existing activation planning. The strict export path resolves physical index aliases and optional-index metadata independently. Existing runtime plans retain their current policy. Installed manifests expose a `packageDigest` for HDL to pin before publication. Identity and curated dictionary resolution use the loaded package root, retained in the local registry and omitted from manifest responses. Explicitly empty pins are rejected.

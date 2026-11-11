# Data Lab Core M0 readiness baseline

**Decision date:** 2026-09-26
**Machine-readable record:** [data-lab-core-m0-readiness.json](./data-lab-core-m0-readiness.json)

## Decision

Use `openehr.rps_dual` as the primary clinical strategy for the first
**Clinical Semantic Query** proof, with `openehr.rps_dual_ibm` as its
release-blocking exact-model profile. Use stable IDs
`clinical-semantic-query` for the demo and `clinical-snomed` for the
composition. Keep `fhir.resource_store` as the supported secondary healthcare
strategy and `cdisc.sdr` as the supported Life Sciences strategy. All other
current packs are preview-only.

This product-priority decision is separate from public-demo readiness. The
readiness score still shows that FHIR currently has fewer public-demo blockers;
openEHR remains ineligible for a public executed claim until its data,
terminology, reset and execution-evidence blockers close. No strategy or demo
artifact reference is promoted here; those immutable versions are produced by
KDL-003 and KDL-004.

## Readiness evidence

| Gate | FHIR | openEHR | Decisive difference |
| --- | ---: | ---: | --- |
| Terminology integration | 4/5 | 1/5 | FHIR already calls the provider-neutral terminology gateway during validation. |
| Deterministic demo data | 4/5 | 2/5 | FHIR has versioned cohort plans and fixed seeds; openEHR generation still uses global randomness and `uuid4`. |
| Trace completeness | 4/5 | 3/5 | FHIR exposes standard query, compiled MQL and execution evidence; openEHR has strong AQL compilation but incomplete failure evidence. |
| Redistribution rights | 2/5 | 1/5 | Both need clearance; the openEHR reference pack explicitly derives from masked private source data. |
| Reset safety | 3/5 | 1/5 | FHIR uses logical-ID upserts and has a guarded prefix-cleanup example; a complete demo reset is still missing. |
| Setup dependencies | 3/5 | 3/5 | Both require packaged runtimes and an execution database. |
| Tests | 4/5 | 4/5 | FHIR audit: 215 passed, 37 skipped, 0 failed. openEHR audit: 32 passed, 0 failed, including IBM exact-model compilation. |
| **Total** | **24/35** | **15/35** | FHIR is currently easier to demonstrate publicly; openEHR is the product-primary engineering path. |

The delivery portfolio is authoritative for engineering priority:

| Track | Strategies | Release blocking |
| --- | --- | --- |
| Primary | `openehr.rps_dual`, `openehr.rps_dual_ibm` | Yes |
| Supported | `fhir.resource_store`, `cdisc.sdr` | Yes |
| Preview | `contextobjects.rps`, `genomics.variant_first`, `snomedct.mongodb`, `x12.co_single` | No |

Preview packs still must be safe and discoverable. They do not carry the same
runtime regression commitment. The standalone SNOMED pack's preview status is
separate from the provider-neutral terminology gateway required by openEHR,
FHIR and future compositions.

## Promotion blockers

1. Compose pinned terminology expansion with a bounded AQL query and show the
   complete AQL-to-MQL trace through RPS Dual.
2. Replace or clear the current openEHR sample-data provenance and terminology
   rights. SNOMED must use a
   legally accessible, version-locked provider; no licensed release is bundled.
3. Add a full-resource, cross-process determinism golden test.
4. Add guarded `demo up/reset/cleanup` against an isolated demo namespace.
5. Run the skipped Mongo tests against an ephemeral database and capture the
   winning-index evidence.

Until these close, the primary state is `engineering_primary_not_public_ready`.
The UI must not label the public journey executed or production-ready.

## Current technical baseline

- Eight discoverable strategy packs now pass the common compatibility loader.
- Seven packs carry a strategy specification.
- There are 120 declared operations: all 120 have an input-schema object, but
  only 108 are constrained contracts; 12 are explicitly tracked as permissive
  placeholders. There are 104 output schemas.
- After the first Mapping IR slice, the API tree contains 209 direct
  `@router`/`@app` HTTP method declarations across 18 Python modules.
- The repository contains 153 `test_*.py` files: 89 contract, 56 unit, 2
  integration, and 1 end-to-end file, with 5 in support categories.
- The bounded OpenMRS numeric-laboratory mapping produces the same validated
  `sample_laboratory_v0.4` composition from batch and CDC after-images, with
  lineage for every output leaf. Its maturity remains `simulated`.
- The runtime and SDK manifest classes had drifted. KDL-001a now gives them one
  authoritative model while leaving the distinct runtime and SDK plugin ABIs
  unchanged.

Run the non-destructive foundation gate with:

```bash
scripts/test-data-lab-foundation
```

This gate validates contract schemas, compatibility loading, package identity,
path containment and the current strategy-pack contract. It does not connect
to MongoDB or run customer/environment data.

The foundation gate is green: 82 passed. The broader contract snapshot is
green: 443 passed and 37 skipped, with no
failures, expected failures or collection errors. The skips are database-backed
cases for which no dedicated test database was configured.

The snapshots were captured with `KEHRNEL_SKIP_DOTENV=true` and all dedicated
`FHIR_TEST_*`, `CDISC_TEST_*` and `KEHRNEL_TEST_*` database variables unset:

```bash
.venv/bin/python -m pytest -q tests/contract
.venv/bin/python -m pytest -q tests/contract/resource_store
.venv/bin/python -m pytest -q tests/contract/rps_dual
```

The release-blocking portfolio gate is `scripts/test-supported-strategies`.
It covers the primary openEHR dual and IBM profiles, supported FHIR and CDISC
packs, and their shared activation/API boundary. Preview strategies are not
release blockers. Its current result is 452 passed and 37 skipped, with no
failures, expected failures or errors.

## Safety ledger

- Secret-bearing `.env*` files are rejected from strategy packages;
  `.env.example` may be included as non-secret documentation. Local registry
  state and `.kehrnel/` runtime state are not copied into fixtures.
- The openEHR reference samples are not approved for a public demo until their
  provenance and redistribution rights are cleared or they are replaced by a
  wholly synthetic pack.
- SNOMED CT release content remains tenant-licensed or remote-provider-only.
  Fictional terminology can support development but cannot be labelled
  SNOMED in the promoted M0 journey.
- FHIR cohorts are synthetic, but bundled standards and terminology dependency
  licenses still require a release audit.

## Claims explicitly not made

The baseline does not claim a production-complete FHIR server, a public-ready
FHIR/SNOMED composition, public-safe openEHR data, a generic source-to-ODL
compiler, production composite activation, horizontal scale evidence, or a
wheel/container identity. Those claims require their own executable evidence.

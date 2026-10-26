---
sidebar_position: 5
---

# Domain CLI Layer

Domain commands group operations by data standard. Each official domain is reached through a different CLI surface:

| Domain | CLI surface | Strategy |
|--------|-------------|----------|
| openEHR | `kehrnel domain openehr <action>` | `openehr.rps_dual` |
| FHIR | `kehrnel fhir <command>` | `fhir.resource_store` |
| SNOMED CT | `kehrnel terminology <command>` | `snomedct.mongodb` |
| CDISC | `kehrnel op` and `kehrnel run` (strategy operations) | `cdisc.sdr` |

`kehrnel domain list` prints the domains that have a dedicated domain command group: `openehr`, `fhir` and `snomedct`.

## openEHR

```bash
kehrnel domain openehr <action> ...
```

Actions: `validate`, `generate`, `transform`, `ingest`, `map`, `identify`. Each one passes its remaining arguments to the matching openEHR tool.

See [openEHR RPS Dual CLI Workflows](/docs/strategies/openehr/rps-dual/cli-workflows).

## FHIR

```bash
kehrnel fhir <command> ...
```

| Area | Commands |
|------|----------|
| Discovery | `metadata`, `capabilities`, `support-matrix`, `resource-catalog`, `search-parameters`, `index-manifest`, `stats` |
| Load | `stage-ig`, `import-data` |
| Query | `search`, `explain` |
| Synthetic cohorts | `cohorts`, `plan-cohort` |

See [FHIR Resource Store CLI Workflows](/docs/strategies/fhir/resource-store/cli-workflows).

## SNOMED CT and terminology

```bash
kehrnel terminology <command> ...
```

| Area | Commands |
|------|----------|
| Providers | `capabilities`, `provider-capabilities`, `configure` |
| SNOMED CT releases | `snomed-releases`, `inspect-snomed-release`, `diff-snomed-release`, `ingest-snomed-release`, `rebuild-snomed-sidecar`, `ensure-snomed-indexes` |
| Terminology operations | `lookup`, `validate-code`, `subsumes`, `expand`, `translate`, `concept-history` |
| Value sets | `value-sets`, `register-value-set`, `validate-value-set` |
| Search and scope | `suggest`, `hybrid-search`, `resolve-scope` |
| Grounding | `nlp-map`, `llm-ground`, `save-grounding-review`, `query-grounded-corpus` |
| Benchmarks | `benchmark-retrieval`, `gold-notes`, `save-gold-note`, `delete-gold-note` |

The `terminology` group uses the provider-neutral terminology API, so the same commands work for SNOMED CT served by `snomedct.mongodb` and for other code systems routed to an external terminology server.

See [SNOMED CT Workflows](/docs/strategies/snomedct/mongodb/workflows).

## CDISC

CDISC has no dedicated command group. Its ingest, export, validation and query operations are strategy operations of `cdisc.sdr`:

```bash
kehrnel op list --env dev            # operations of the active strategies
kehrnel op schema <operation> --env dev
kehrnel run <operation> --env dev --domain cdisc --strategy cdisc.sdr ...
```

See the [CDISC Study Data Repository](/docs/strategies/cdisc/sdr/) strategy.

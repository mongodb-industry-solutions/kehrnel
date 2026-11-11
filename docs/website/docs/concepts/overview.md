---
sidebar_position: 1
---

# Concepts Overview

These concepts are shared by every domain and strategy in \{kehrnel\}. Each one maps to a type or contract in the code. Domain-specific ideas (such as openEHR path encoding) live under each domain.

## Domain

A domain is a healthcare data standard that \{kehrnel\} serves: openEHR, FHIR, CDISC or SNOMED CT. Each strategy manifest declares its `domain`, and the runtime uses the lowercase domain name (`openehr`, `fhir`, `cdisc`, `snomedct`) as the routing key. Query and compile requests must name a domain.

## Strategy

A strategy is a pack that defines how one domain's data is stored, transformed and queried on MongoDB. A pack contains:

- `manifest.json`: `id` (for example `openehr.rps_dual`), `domain`, `version`, `entrypoint`, `capabilities`, `ops`, `interaction_contracts`, adapter requirements
- configuration defaults and a JSON schema
- the plugin: a `StrategyPlugin` subclass named by the manifest `entrypoint`

The official strategies are `openehr.rps_dual`, `fhir.resource_store`, `cdisc.sdr` and `snomedct.mongodb`.

## Environment

An environment (`EnvironmentRecord`) is the runtime scope that strategies are activated into. It has an `env_id`, optional name, description and metadata, and an optional `bindings_ref` shared by its activations.

## Activation

An activation (`EnvironmentActivation`) binds one strategy, at a version and with a configuration, to one domain of an environment. An environment holds at most one activation per domain.

On activation the runtime:

- merges the strategy defaults with the submitted configuration and validates it against the strategy schema
- records the `config_hash` and the `manifest_digest`
- requires a `bindings_ref` (plaintext bindings are rejected unless explicitly allowed)
- returns the existing activation unchanged when the strategy, configuration and bindings are the same; otherwise it replaces it and keeps a reference to the previous activation

Activations can be upgraded, rolled back or removed per domain (`/environments/{env_id}/activations/{domain}/upgrade`, `/rollback`). At dispatch time, an activation whose manifest digest no longer matches the strategy, and is not compatible, fails with `ACTIVATION_STRATEGY_MISMATCH`.

## Bindings

Bindings carry connectivity (for example the MongoDB connection) for an activation. They are referenced by `bindings_ref` and resolved at dispatch time by the configured bindings resolver; per-request plaintext bindings are rejected. For MongoDB-backed strategies, the logical database comes from the activation configuration, not from the bindings.

## Capability

Capabilities are the strings in the manifest `capabilities` list that declare what a strategy implements, such as `ingest`, `transform`, `query`, `synthetic` or qualified values like `terminology.subsumes`.

## Interaction Contract

An interaction contract declares who defines an interaction and how \{kehrnel\} executes it. Fields:

| Field | Meaning |
|-------|---------|
| `id`, `name` | identity, for example `fhir.rest-search` |
| `mode` | `native` (the standard's own interaction), `compiled` (a \{kehrnel\} contract compiled to MongoDB), or `hybrid` |
| `kind` | for example `query`, `resource-api`, `exchange`, `analysis`, `semantic-object` |
| `authority`, `standard`, `contract` | who defines it and which specification or contract version applies |
| `engines`, `builders` | execution engines and plan builders bound to the contract |
| `operations` | strategy ops that implement it |
| `source_domains` | standard sub-domains covered |

Only contracts that list an engine take part in query-plan attribution: when exactly one contract matches a plan's engine (and builder), the explain block names it. Examples: `fhir.rest-search` (native), `cdisc.study-query` (`cdisc-query/v1`, compiled), `cdisc.dataset-exchange` (native).

## Ops

Ops are strategy-specific operations declared in the manifest `ops` list, each with a `name`, a `kind` (for example `ingest`, `maintenance`, `query`, `diagnostic`, `synthetic`, `export`), a `summary` and JSON input and output schemas. The runtime validates the payload against the input schema before calling the strategy's `run_op`. Alongside ops, every strategy can implement the standard operations `plan`, `apply`, `transform`, `ingest`, `compile_query`, `query` and `authoritative_write`.

See [Ingest and Transformation](/docs/domains/openehr/composition-flattening) and [Query and Execution](/docs/domains/openehr/aql-query-engine) for how these fit together at runtime.

## Domain Concepts

- openEHR: [Dual-View Persistence](/docs/domains/openehr/concepts/dual-collection), [Canonical To Operational Transformation](/docs/domains/openehr/concepts/flattening), [Hierarchical Path Encoding](/docs/domains/openehr/concepts/reversed-paths), [Query Translation](/docs/domains/openehr/concepts/aql-to-mql), [Source Sampling for Synthetic Data](/docs/domains/openehr/concepts/data-sampling)
- FHIR: [FHIR Resource Store Strategy](/docs/strategies/fhir/resource-store/)
- CDISC: [CDISC SDR Strategy](/docs/strategies/cdisc/sdr/)
- SNOMED CT: [SNOMED CT on MongoDB Strategy](/docs/strategies/snomedct/mongodb/)
- [All official strategies](/docs/strategies/overview)

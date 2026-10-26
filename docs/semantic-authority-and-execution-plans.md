# Semantic Authority and Execution Plans

## Decision

Kehrnel preserves an established native semantic model whenever it can satisfy
the selected consumption contract. ContextObjects are not mandatory plumbing
and do not replace FHIR StructureDefinitions, openEHR archetypes/templates, or
CDISC metadata.

Each consumption contract resolves to one of three semantic modes:

- `native`: compile directly against one established semantic authority;
- `derived`: expose a ContextObject view over one native authority when shared
  HDL tooling or a purpose-built result shape requires it;
- `composed`: join several authorities, or model a domain without an adequate
  native authority, through an explicit ContextObject definition.

The consumption contract decides the mode. The number of sources is useful
evidence, but is not the rule by itself.

## Implemented Contract

`semantic-execution-planning-request/v1` wraps a
`strategy-compilation-request/v1` and optional explicit semantic requirements.
Kehrnel returns `semantic-execution-plan/v1` from:

```text
POST /v1/semantic-execution-plans/compile
```

The response records:

- native and composed semantic authorities;
- one resolved semantic mode per consumption contract;
- the ContextObject definition only when one is needed;
- native execution bindings such as `openehr_aql` and `fhir_search`;
- blocking or review issues inherited from strategy compilation;
- a deterministic content digest.

The plan is design-time evidence. It does not activate a strategy or execute a
query.

## Native Examples

```text
openEHR source
 -> openEHR authority
 -> AQL consumption contract
 -> openehr_aql subplan
 -> openehr.rps_dual
```

No ContextObject is required.

```text
FHIR source
 -> FHIR authority
 -> FHIR Search consumption contract
 -> fhir_search subplan
 -> fhir.resource_store
```

FHIR Search is not routed through AQL.

## Composition Example

```text
FHIR + openEHR + operational source
 -> explicit ContextObject definition
 -> purpose-built patient-context contract
 -> service or agent-tool subplan
```

Without the ContextObject reference, compilation fails closed with
`CONTEXT_OBJECT_REQUIRED`. Kehrnel never invents a semantic contract silently.

## Query-Plan Boundary

A semantic execution plan is an orchestration and evidence envelope, not a
universal query language. Its subplans retain their native semantics. AQL,
FHIR Search, CDISC query, MQL, services, streams, and exports remain distinct
bindings that can evolve independently.

The next executable increment is to attach the existing OPT-bound semantic AQL
planner and AQL-to-MQL compiler evidence to the `openehr_aql` subplan. The same
envelope can then carry the independently compiled FHIR Search proof.

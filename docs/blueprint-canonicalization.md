# Structural Blueprint canonicalization

`POST /v1/solution-blueprints/canonicalize` validates `solution-blueprint/v1`
and returns `solution-blueprint-canonicalization-result/v1`. The producer is
stateless: it validates internal participant, scope, authority and composition
invariants without retrieving any artifact named in the supplied Blueprint.
It does not save a project, compile a plan, prepare or activate a runtime.

The receipt contains the complete `canonicalBlueprint` and its `blueprintDigest`.
`validationScope` is always `structural_only`. `catalogReadiness`,
`compilerReadiness` and `activationReadiness` are always `not_evaluated`.
A saved canonical Blueprint therefore provides no readiness or runtime grant.
The caller must verify the returned identity and digest before tenant storage.

The canonical wire uses camelCase, the established canonical JSON bytes and
normal contract defaults. `dataProductCompositions` alone preserves presence:
an absent field stays absent, an explicit `[]` stays present, and nonempty
compositions retain their validated content with composition/input ordering
canonicalized. This behavior also applies when the Blueprint is nested in a
receipt or serialized by FastAPI. Historical Blueprint digests and receipts
must not be rewritten to add an absent field.

For the shared HDL bridge, configure its existing service key as an
`hdl-delegator` with the narrow `blueprint:validate` action and authorized owning
scopes. Send a server-signed `X-HDL-Principal-Assertion` alongside the API key.
A validation-only assertion may contain `strategyRefs: []`; an empty list is
invalid whenever any package or runtime action is requested. An assertion may
never derive authority from the artifacts inside the Blueprint body.

Assertions retain the established RS256 algorithm, configured verification keys,
issuer and instance audience, actor and owning scope/workspace binding, maximum
60-second lifetime, five-second clock skew, and bounded payload. Missing or
invalid assertions on a delegator key fail closed. HDL constructs the scope
from its authenticated server session and live workspace membership; browser
assertions and claims are not accepted as authority. Keep signing keys,
assertions and server API keys out of browsers and logs.

A validation-only service ceiling can use `actions: ["blueprint:validate"]`,
`strategyRefs: []` and `environmentIds: []`. It confers no package discovery,
compilation, runtime queries, environment mutation or lifecycle entitlement.
Ordinary standalone installations continue to use their existing API-key
authentication for this stateless operation. Python library callers require no
HTTP identity. Existing package, configuration and deployment digest contracts
are unchanged. Responses use `Cache-Control: private, no-store`.

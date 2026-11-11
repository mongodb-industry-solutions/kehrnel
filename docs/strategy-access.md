# Strategy access and K1a migration

K1a adds HTTP package access control without changing strategy manifests, package digests, deployment compiler coverage, or compiler/configuration contract versions. Enable `KEHRNEL_AUTH_ENABLED=true` for these controls. The existing auth-disabled development mode retains its runtime behavior and is not an access-controlled deployment.

## Admit exact packages explicitly

Set `KEHRNEL_STRATEGY_ACCESS_FILE` to an operator-owned JSON file outside all installed package roots. Missing records deny HTTP access; installing a package or possessing an admin key does not publish it. Records use the exact installed `id`, `version`, and `packageDigest`, including version build metadata:

```json
{
  "contract": "strategy-access/v1",
  "records": [
    {
      "ref": {
        "id": "openehr.rps_dual",
        "version": "0.1.0",
        "packageDigest": "sha256:0000000000000000000000000000000000000000000000000000000000000000"
      },
      "owner": {"kind": "team", "id": "example-team"},
      "dependencies": [],
      "assets": ["README.md"]
    }
  ]
}
```

The example identity is illustrative: replace it with the installed package reference before use. Owners may be `team` or `individual`. Publication defaults to private. Anonymous reads require an explicit `publication` object with `visibility: "public"` and boolean `approved: true`; approved `internal` publication still requires scoped authority. Dependencies must be exact admitted references and readable throughout the closure; missing, inaccessible, or cyclic dependencies deny access. Package-declared dependencies must agree with the sidecar. Declare each distributable relative asset path; traversal, symlinks, and undeclared files are denied.

One policy covers list/detail, compatibility retrieval, spec, declared assets, generated docs/OpenAPI, compilation, and package references resolved by runtime operations. Inaccessible and unknown strategy IDs share a 404 response. Access records stay outside executable bytes and cannot be replaced through `/strategies/load`.

## Scope installation and delegator keys

Retain the existing `KEHRNEL_API_KEYS`, optional `KEHRNEL_ADMIN_API_KEYS`, and environment-access configuration. Add `KEHRNEL_API_KEY_AUTHORITIES`, a JSON object keyed by the first 20 lowercase hex characters of the API key's SHA-256 fingerprint:

```json
{
  "0123456789abcdefabcd": {
    "kind": "installation",
    "scopes": [{"kind": "team", "id": "example-team"}],
    "actions": ["strategy:read", "strategy:compile", "runtime:query"],
    "strategyRefs": [
      {
        "id": "openehr.rps_dual",
        "version": "0.1.0",
        "packageDigest": "sha256:0000000000000000000000000000000000000000000000000000000000000000"
      }
    ],
    "environmentIds": ["example-runtime"]
  }
}
```

Replace the illustrative fingerprint and references. An `installation` key has exactly one owner scope and needs no person assertion. `environmentIds` contains Kehrnel runtime keys, not HDL environment IDs; the existing `KEHRNEL_API_KEY_ENV_SCOPES`/default-environment guard must also permit the target. Admin authorization remains required for compilation and existing privileged runtime operations.

A key with `kind: "hdl-delegator"` defines the ceiling for allowed owner scopes, actions, exact packages, and runtime keys. Send its API key together with `X-HDL-Principal-Assertion`. Configure:

- `KEHRNEL_HDL_ISSUER`: exact trusted issuer.
- `KEHRNEL_HDL_AUDIENCE`: exact instance audience.
- `KEHRNEL_HDL_ISSUER_KEYS`: JSON mapping configured `kid` values to PEM RSA public-key strings, with JSON-escaped newlines.

Assertions require RS256, `typ: "hdl-delegated-principal+jwt"`, a configured `kid`, and claims `iss`, `aud`, `sub`, `iat`, `nbf`, `exp`, `jti`, `v: 1`, `scope`, `workspace`, `actions`, and `strategyRefs`. Workspace equals the admitted owner scope. An optional `environment` contains `{id, runtimeId}`. Dates are integer UTC seconds, `nbf=iat`, with a lifetime of at most 60 seconds and five seconds of clock tolerance. Assertions are limited to 8 KiB and 64 unique exact references. No token-selected key URLs are fetched. Effective authority intersects the key ceiling and installed package policy, and stays request-local. Missing/invalid assertions never grant private access; assertions on non-delegator keys are rejected.

`strategy:read` and `strategy:compile` do not grant clinical query execution. `runtime:query` additionally requires the exact admitted runtime key, package reference, and existing compatible activation. The supported delegated query path is `/environments/{runtimeId}/query` (including its existing `/v1` alias). Missing activation returns preparation-required; resolution never creates one. Delegation grants no general operations, activation, write, registration, or publication permission. Legacy domain query routes using shared database settings are unavailable to delegated callers.

## Registry and operational limits

Installed inventory and diagnostics require both admin authorization and a fingerprint listed in `KEHRNEL_INVENTORY_API_KEY_FINGERPRINTS`; discoverability does not imply inventory access. Operator configuration is loaded when the app is created, so restart after changing sidecar or key-authority configuration.

Registry persistence adds `manifest_identities`. Exact repeated registration is idempotent; an existing ID with a different version or content returns 409 before registry or access-record changes. Startup validates discovered identities before replacing its manifest cache. Older registry records are revalidated against installed roots before identity metadata is saved. Resolve conflicting installations explicitly; retain the old registry/package checkpoint rather than overwrite it through loading.

The Python registry/runtime remains independent of HDL sessions and JWT. Manifest-only SDK identities use deterministic JSON that permits finite fractional config/schema values and Unicode keys, but cannot be admitted as HTTP package references. The package canonicalization contract retains its existing strict subset. `PyJWT[crypto]==2.15.1` is confined to API/all installation extras.

No absent `/v1` design producer is recreated. `jti` is tracing metadata; there is no replay database or publication-management service. Compiled readiness remains `runtime: not_observed`. Live MongoDB execution, terminology, installed clinical-data shape, and deployment qualification require separate integration checks; synthetic security tests do not establish them.

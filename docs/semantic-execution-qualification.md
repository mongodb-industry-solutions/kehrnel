# Semantic execution qualification

Kehrnel can qualify the deployed openEHR and FHIR semantic paths against one
named environment. A successful artifact proves that the requested terminology
expansion, compiled database query, active strategy revision, physical data
target, and projection readiness were part of the same execution.

It does not certify production performance, clinical correctness beyond the
probe, or the natural-language layer. Those require separate benchmark and
evaluation evidence.

## Run the reference qualification

1. Copy
   [`examples/semantic-execution-qualification.reference.json`](../examples/semantic-execution-qualification.reference.json)
   and set `environmentId`, `baseUrl`, and any deployment-specific request
   values.
2. Provision that environment with active `openehr.rps_dual`,
   `fhir.resource_store`, the pinned SNOMED provider/release, the reference OPT,
   representative data, current projections, and the required indexes.
3. Put the API key in an environment variable; never add it to the spec.
4. Run:

```bash
KEHRNEL_API_KEY='...' .venv/bin/python scripts/qualify_semantic_execution.py \
  --spec /path/to/qualification.json \
  --base-url http://127.0.0.1:8080 \
  --environment-id your-environment-id \
  --output /path/to/qualification-result.json
```

The command exits with `0` only when every requested probe qualifies, `1` when
an endpoint or its evidence fails, and `2` when the local spec or invocation is
invalid.

## Evidence and privacy boundary

The result is a `semantic-execution-qualification-result/v1` artifact with a
content digest. It retains endpoint paths, request/response digests, deployment
identity, compiled artifact digests, terminology expansion digests, readiness
digests, engine names, and row counts.

It deliberately excludes API keys, server URLs, database and collection names,
query results, FHIR resources, openEHR rows, and raw error bodies. The artifact
is structural evidence rather than a MongoDB cryptographic attestation. Keep
the original spec and deployment logs under the organization's normal evidence
retention policy when independent audit or performance qualification is needed.

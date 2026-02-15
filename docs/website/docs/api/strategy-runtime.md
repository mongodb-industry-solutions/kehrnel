---
sidebar_position: 6
---

# Strategy Layer API

The strategy layer exposes strategy-scoped runtime behavior.

## Strategy Prefixes

Primary examples:

- `/api/strategies/openehr/rps_dual/...`
- `/environments/{env_id}/...` activation and synthetic job endpoints

Typical usage:

- activate strategy in environment
- configure strategy
- run strategy operations
- submit/query/cancel synthetic jobs

This layer is where \{kehrnel\}'s multi-strategy design is operationalized.

## Detailed Endpoint Groups

- [Strategy Config & Ingest](/docs/api/endpoints/strategy-config-and-ingest)
- [Strategy Synthetic & Jobs](/docs/api/endpoints/strategy-synthetic-and-jobs)

## Related

- [API Layers](/docs/api/layers)
- [Core Layer API](/docs/api/core-openapi)

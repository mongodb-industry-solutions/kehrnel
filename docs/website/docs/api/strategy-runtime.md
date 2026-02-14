---
sidebar_position: 5
---

# Strategy Runtime API

Strategy runtime APIs expose strategy-scoped behavior and operations.

Examples:

- `/api/strategies/openehr/rps_dual/...`
- `/environments/{env_id}/...` activation and synthetic job endpoints

Typical usage:

- activate strategy in environment
- configure strategy
- run strategy operations
- submit/query/cancel synthetic jobs

This layer is where \{kehrnel\}'s multi-strategy design is operationalized.

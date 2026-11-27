---
sidebar_position: 5
---

# Domain CLI Layer

`domain` commands group operations by data domain.

## Commands

- `kehrnel domain list`
- `kehrnel domain openehr <action> ...`

Current openEHR actions:

- `validate`
- `generate`
- `transform`
- `ingest`
- `map`
- `identify`

SNOMED CT currently uses the universal runtime and domain API surfaces rather than a dedicated `kehrnel domain snomedct` CLI group:

- strategy ops via `/environments/{env}/run`
- domain routes under `/api/domains/snomedct/*`
- strategy pack metadata under `snomedct.mongodb`

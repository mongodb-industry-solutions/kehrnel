---
sidebar_position: 6
---

# Strategy CLI Layer

`strategy` commands manage strategy selection and discovery.

## Commands

- `kehrnel strategy use <strategy_id>`
- `kehrnel strategy current`
- `kehrnel strategy list [--domain ...]`

`strategy list` calls runtime catalog APIs and can filter by domain.

## Recommended workflow

```bash
kehrnel auth login --runtime-url http://localhost:8000
kehrnel strategy list --domain openehr
kehrnel strategy use openehr.rps_dual --domain openehr
kehrnel common transform ...
```

## Continue Reading

- [Common CLI Layer](/docs/cli/common)
- [openEHR RPS Dual CLI Workflows](/docs/strategies/openehr-rps-dual/cli-workflows)
- [openEHR RPS Dual Configuration](/docs/strategies/openehr-rps-dual/configuration)

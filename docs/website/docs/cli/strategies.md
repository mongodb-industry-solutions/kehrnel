---
sidebar_position: 6
---

# Strategy CLI Layer

`strategy` commands manage strategy selection and discovery.

## Commands

- `kehrnel strategy use <strategy_id>`
- `kehrnel strategy current`
- `kehrnel strategy list [--domain ...]`
- `kehrnel strategy build-search-index [--env ...] [--out ...]`

`strategy list` calls runtime catalog APIs and can filter by domain.
`strategy build-search-index` asks the active strategy to generate the Atlas Search definition that matches its current mappings configuration.

## Recommended workflow

```bash
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

kehrnel setup --runtime-url "$RUNTIME_URL"
kehrnel strategy list --domain openehr
kehrnel strategy use openehr.rps_dual --domain openehr
kehrnel op list --strategy openehr.rps_dual
kehrnel op schema synthetic_generate_batch --strategy openehr.rps_dual
kehrnel run ensure_dictionaries --env dev --domain openehr
kehrnel strategy build-search-index --env dev --domain openehr --strategy openehr.rps_dual --out .kehrnel/search-index.json
```

## Continue Reading

- [Common CLI Layer](/docs/cli/common)
- [openEHR RPS Dual CLI Workflows](/docs/strategies/openehr/rps-dual/cli-workflows)
- [openEHR RPS Dual Configuration](/docs/strategies/openehr/rps-dual/configuration)

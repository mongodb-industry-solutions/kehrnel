---
sidebar_position: 2
---

# Auth And Context

The unified CLI uses one persistent state file:

- `~/.kehrnel/config.json`

It stores:

- auth: API key + default runtime URL
- context: environment + domain + strategy + data mode + source/sink defaults + optional runtime URL override
- resources: reusable source/sink profiles

The file is written with owner-only permissions when possible (`chmod 600`) because it may contain API keys.

## Authenticate once

```bash
kehrnel setup --runtime-url http://localhost:8000
# or, if you prefer the explicit primitives:
# kehrnel auth login --runtime-url http://localhost:8000
kehrnel auth whoami
kehrnel doctor
```

## Set working context

```bash
kehrnel context set --env dev --domain openehr --strategy openehr.rps_dual
kehrnel context show
```

Set optional workflow defaults:

```bash
kehrnel context set \
  --data-mode profile.search_shortcuts \
  --source resource://src \
  --sink resource://dst
```

Manage reusable profiles:

```bash
kehrnel resource add src --type mongo --uri "$MONGODB_URI" --db hc_openEHRCDR --collection samples
kehrnel resource add dst --type mongo --uri "$MONGODB_URI" --db hdl_user_test --collection compositions_rps
kehrnel resource use --source src --sink dst
```

Context-aware commands (for example `kehrnel common transform`) will use this context unless explicitly overridden with `--domain` or `--strategy`.

## Setup Wizard (Recommended)

`kehrnel setup` is an interactive first-run experience that:

- checks `GET /health`
- lists strategies from `GET /strategies` so you can pick a domain + strategy
- persists auth and context in `~/.kehrnel/config.json`
- can optionally activate the strategy in your environment (`--activate`)

Examples:

```bash
# Minimal interactive setup
kehrnel setup --runtime-url http://localhost:8000

# Fully non-interactive (CI / automation)
kehrnel setup \
  --non-interactive \
  --runtime-url http://localhost:8000 \
  --api-key "$KEHRNEL_API_KEY" \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual

# Verify resulting state
kehrnel doctor
```

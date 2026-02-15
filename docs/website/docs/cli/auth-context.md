---
sidebar_position: 2
---

# Auth And Context

The unified CLI uses one persistent state file:

- `~/.kehrnel/config.json`

It stores:

- auth: API key + default runtime URL
- context: environment + domain + strategy + optional runtime URL override

## Authenticate once

```bash
kehrnel auth login --runtime-url http://localhost:8000
kehrnel auth whoami
```

## Set working context

```bash
kehrnel context set --env dev --domain openehr --strategy openehr.rps_dual
kehrnel context show
```

Context-aware commands (for example `kehrnel common transform`) will use this context unless explicitly overridden with `--domain` or `--strategy`.

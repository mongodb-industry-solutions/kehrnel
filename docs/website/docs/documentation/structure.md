---
sidebar_position: 2
---

# Documentation Structure

This page defines the canonical documentation information architecture for `{kehrnel}`.

If a new module/domain/strategy is added in code, docs should be added in the matching path here.

## Primary Documentation Spaces

| Runtime Area | Code Path | Docs Path |
|---|---|---|
| CLI | `src/kehrnel/cli/` | `docs/website/docs/cli/` |
| API | `src/kehrnel/api/` | `docs/website/docs/api/` |
| Engine | `src/kehrnel/engine/` | `docs/website/docs/engine/` |
| Persistence | `src/kehrnel/persistence/` | `docs/website/docs/engine/core/` and strategy pages |
| Cross-domain common capabilities | `src/kehrnel/engine/common/` | `docs/website/docs/common/` and `docs/website/docs/engine/common/` |
| Domain guides | `src/kehrnel/engine/domains/<domain>/` + `src/kehrnel/api/domains/<domain>/` | `docs/website/docs/domains/<domain>/` + `docs/website/docs/api/domains/<domain>/` |
| Strategy packs | `src/kehrnel/engine/strategies/<domain>/<strategy>/` + `src/kehrnel/api/strategies/<domain>/<strategy>/` | `docs/website/docs/strategies/<domain>/<strategy>/` + `docs/website/docs/api/strategies/<domain>/<strategy>/` |

## API Documentation Rules

API docs always follow this layout:

- `api/core/`: environment activation, strategy registry, runtime-level operations.
- `api/common/`: shared API conventions and cross-domain common endpoints.
- `api/domains/<domain>/`: domain API contract and behavior.
- `api/strategies/<domain>/<strategy>/`: strategy-specific API contract.
- `api/endpoints/`: grouped endpoint deep dives.

When adding a new domain API:

1. Add `docs/website/docs/api/domains/<domain>/index.md`.
2. Add endpoint-group pages under `docs/website/docs/api/endpoints/`.
3. Link from `docs/website/docs/api/domains/index.md`.

When adding a new strategy API:

1. Add `docs/website/docs/api/strategies/<domain>/<strategy>/index.md`.
2. Add endpoint-group pages under `docs/website/docs/api/endpoints/` if needed.
3. Link from `docs/website/docs/api/strategies/index.md`.

## Engine Documentation Rules

Engine docs mirror `src/kehrnel/engine/`:

- `engine/core/`
- `engine/common/`
- `engine/domains/<domain>/`
- `engine/strategies/<domain>/<strategy>/`

When adding a new domain engine module:

1. Add `docs/website/docs/engine/domains/<domain>/index.md`.
2. Link domain-level behavior from `docs/website/docs/domains/<domain>/index.md`.

When adding a new strategy engine module:

1. Add `docs/website/docs/engine/strategies/<domain>/<strategy>/index.md`.
2. Link operational details from `docs/website/docs/strategies/<domain>/<strategy>/`.

## Common Module Documentation Rules

For new common modules (for example mappings, validation, transforms):

1. Add module guide under `docs/website/docs/common/<module>/`.
2. Add engine-facing details under `docs/website/docs/engine/common/<module>/`.
3. If module has API endpoints, add `docs/website/docs/api/common/<module>/`.

## API Documentation Policy

Narrative documentation stays in Docusaurus.

API contracts are documented live in Swagger/ReDoc:

- `/docs`, `/redoc`
- `/docs/core`, `/redoc/core`
- `/docs/domains/{domain}`, `/redoc/domains/{domain}`
- `/docs/strategies/{domain}/{strategy}`, `/redoc/strategies/{domain}/{strategy}`

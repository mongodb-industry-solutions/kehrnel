---
sidebar_position: 1
---

# Engine Core

Engine core contains runtime primitives that are domain-agnostic and strategy-agnostic.

Typical responsibilities:

- strategy discovery and loading
- pack manifest/schema/default validation
- activation registry and environment runtime state
- strategy runtime dispatch (`run`, capabilities, operation routing)

Primary code location:

- `src/kehrnel/engine/core/`

Related API layer:

- [API Core Layer](/docs/api/core)

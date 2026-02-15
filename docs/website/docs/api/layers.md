---
sidebar_position: 2
---

# API Layers

The API is organized in four layers. Start here to decide where an operation belongs, then use the layer page for details.

## 1. Core Layer

Platform-level runtime surfaces that are domain-agnostic and strategy-agnostic.

- OpenAPI contracts and slices
- Strategy registry and activation lifecycle
- Environment endpoint discovery

Use this layer for contract generation, platform introspection, and runtime control.

See: [Core Layer API](/docs/api/core-openapi)

## 2. Common Layer

Cross-cutting API behavior shared by all layers.

- Authentication and authorization model
- Request and response conventions
- Error handling and status codes

Use this layer to implement consistent clients and middleware.

See: [Common Layer API](/docs/api/common-api)

## 3. Domain Layer

Clinical domain operations that implement standard domain behavior.

- openEHR APIs (EHR, templates, composition, query, versioning)
- FHIR preview APIs

Use this layer for canonical healthcare data operations.

See: [Domain Layer API](/docs/api/domain-openehr)

## 4. Strategy Layer

Strategy-specific behavior for a concrete persistence or execution strategy.

- Strategy configuration
- Ingest/preview operations
- Synthetic generation and job management

Use this layer for strategy runtime workflows.

See: [Strategy Layer API](/docs/api/strategy-runtime)

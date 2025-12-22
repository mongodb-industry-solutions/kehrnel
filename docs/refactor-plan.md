# Kehrnel Multi-Strategy Refactor (Draft)

This document captures the target architecture for evolving kehrnel from an openEHR-only toolkit into a plugin-driven platform that can host multiple data strategies (openEHR, FHIR, genomics, terminology-backed enrichers, etc.), expose discovery/activation to the Healthcare Data Lab portal, and avoid duplicated logic across strategy variants.

## Goals
- Single host that can load multiple strategies via a consistent Strategy SDK and manifest.
- Clear separation between **kernel** (orchestration/runtime/registry), **shared libs** (protocol/domain utilities), **adapters** (storage/search/vector/queue), and **strategy plugins** (composition of shared libs + adapters).
- Optional remote strategy hosts for isolation/scaling, but unified registry/discovery.
- JSON Schema–driven config and lifecycle hooks per strategy; versioned activation records with rollback.
- Keep current openEHR capabilities working as a “reference strategy” while we migrate.

## High-Level Architecture
- **Kernel (new)**
  - Strategy loader (local modules / entrypoints) and capability router.
  - Registry service (manifests + versions) and activation records (per environment/tenant).
  - Job runner for backfill/reindex/embedding/synthetic data.
  - Observability (health/metrics/logs) per strategy.
- **Strategy SDK (new)**
  - Manifest model: `id`, `protocols`, `version`, `capabilities`, `config_schema`, `adapters_required`, `ui` metadata.
  - Lifecycle hooks: `validate_config`, `provision`, `ingest`, `transform`, `query/search`, `embed/enrich`, `deactivate`, `migrate`.
  - Capability flags: ingest, query, search, mapping, validate, transform, generate, embed, synthetic, enrich.
- **Shared Libraries**
  - `libs/openehr`: traversal, coding, reverse/forward path helpers, shortcuts/rules, validator.
  - `libs/fhir`: resource validation/profile resolution, search parameter evaluation, bundle helpers.
  - `libs/terminology`: SNOMED/LOINC/etc. adapters.
  - `libs/enrichment`: embedding pipelines, chunkers, NLP bindings, synthetic generators.
- **Adapters**
  - Storage/search/vector/queue abstractions: Mongo/Atlas, Postgres, Elastic/OpenSearch, pgvector/Atlas Vector, Kafka/NATS/SQS.
  - Strategy plugins compose adapters; kernel provides bindings from activation config.
- **Strategies (plugins)**
  - `strategies/openehr/rps_dual` (reference): reuse existing flattener/transformer.
  - Future: `strategies/fhir/resource_first`, `strategies/genomics/...`, `strategies/openehr/*` variants reusing `libs/openehr`.
- **Portal Integration**
  - Registry API for catalog/discovery (read-only).
  - Admin API to activate/deactivate strategies with config payload (JSON Schema-driven UI).
  - Status API to show active strategy per environment and available updates.

## Instance Model
- Default: single kehrnel instance hosting multiple enabled strategies; strict config isolation per environment/tenant.
- Optional: register remote strategy hosts (another kehrnel node) in the registry; portal treats them uniformly.

## Migration Plan (high level)
1) Land SDK scaffolding (manifest, capability model, plugin loader) and keep existing APIs/CLIs intact.
2) Extract shared openEHR utilities into `libs/openehr` and wrap current ingestion/transform pipeline as `strategies/openehr/rps_dual`.
3) Add registry/activation models + lightweight read API; wire portal to consume.
4) Refactor ingestion endpoints to call strategy plugins via kernel router; keep openEHR API as a reference strategy.
5) Add example (not full) FHIR/genomics strategies showing how to use SDK.
6) Add observability/versioning + migration hooks for upgrades/rollbacks.

## Current status (incremental)
- SDK models/hooks and plugin loader landed.
- OpenEHR RPS dual strategy wrapped as a plugin.
- In-memory registry/runtime and capability router added; ingest API can route via strategy_id.
- Mongo storage adapter bridges existing MongoStore to bindings.
- Minimal strategy API (`/v1/strategies`) lists manifests and allows in-memory/file-backed activation.
- File-backed registry store added for manifests/activations (dev).
- FHIR resource-first strategy stub manifest added (placeholder).
- Protocol-aware routing (optional) and built-in registration of openEHR + FHIR manifests.
- OpenEHR shared lib facades (`libs/openehr`) introduced to decouple strategies from concrete module paths.
- FHIR strategy ingest now stores native resources via adapters (search mirror TBD).
- Registry persistence improved (atomic file writes, activation restore hooks), with startup restore in `app/strategy_runtime`.
- OpenEHR field remapping extracted to shared lib (`libs/openehr/remap.py`) and reused by ingest API.
- OpenEHR coding helpers facade added (`libs/openehr/coding`); openEHR strategy uses remap helper.
- Additional openEHR shared facades (traversal, rules, validator) added to avoid future duplication across strategies.
- FHIR strategy now builds a lightweight search mirror document (subject/meta/text/date fields) for search collection.
- Added search adapter scaffold (OpenSearch) and binding builder support for search adapters per activation.
- Genomics stub removed to focus on FHIR examples. Added a simulated FHIR skeleton strategy for guidance.
- FHIR strategies use bound search adapters (via bindings) for search queries.
- At-code codec role is set via shared helpers to keep transformer/codecs in sync.
- Legacy AQL transformers are wired through the new runtime (partial parity). See `docs/status-aql-parity.md` for current coverage/gaps and debug guidance (`compile_query?debug=true` includes builder/scope info).

Next: Persist registry/activations, add adapter wiring per activation, extract openEHR shared libs, and add richer FHIR sample manifests.
This document will be refined as we implement the steps above.

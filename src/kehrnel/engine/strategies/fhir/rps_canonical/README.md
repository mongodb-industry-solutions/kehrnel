# FHIR RPS Canonical (`fhir.rps_canonical`)

Strategy pack for **native FHIR R5** resources in MongoDB:

- **Generation:** **fhir-gen** (`fhir_gen`) — vendored at `src/kehrnel/engine/domains/fhir/libs/fhir-data-generation`
- **Search:** **fhir-mql** (`fhir_search_to_mql`) — vendored at `src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql`

## Pack layout

| Location | Contents |
|----------|----------|
| [`specification/`](specification/README.md) | Pack manifest, schema, defaults, spec, API sample JSON |
| [`scripts/`](scripts/README.md) | Runtime Python modules + `spike_generate_and_search.py` |
| [`engine/domains/fhir/libs/`](../../../domains/fhir/libs/README.md) | Vendored fhir-gen + fhir-mql |
| [`engine/domains/fhir/scripts/`](../../../domains/fhir/scripts/README.md) | `sync-fhir-libs.*` (refresh vendored libs) |

## Status

- **Implemented:** `synthetic_generate_batch`, `fhir_denormalize`, `fhir_ensure_indexes`, `fhir_search`, `fhir_list_search_params`, `fhir_stats`
- **Planned:** `negotiate_fhir_search` (see manifest ops)

## Configuration

See `specification/defaults.json` and `specification/schema.json`. Activation merges manifest defaults with environment bindings (`database`, `schema_version`, `collections.mode: per_resource_type`).

## Docs

Portal: `/guide/docs/strategies/fhir/rps-canonical` · Local: [FHIR_TESTING.md](../../../../../../FHIR_TESTING.md)

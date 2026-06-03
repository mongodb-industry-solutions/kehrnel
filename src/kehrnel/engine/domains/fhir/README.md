# FHIR domain (Kehrnel)

| Layer | Location |
|-------|----------|
| HTTP API (`POST /api/domains/fhir/search`) | `src/kehrnel/api/domains/fhir/` — not under this folder |
| FHIR search compile/execute | `src/kehrnel/engine/strategies/fhir/rps_canonical/scripts/query.py` |
| Strategy pack | `src/kehrnel/engine/strategies/fhir/rps_canonical/` |
| Vendored libraries | `libs/` (**fhir-gen**, **fhir-mql**) — see [libs/README.md](libs/README.md) |
| Sync from standalone repos | [scripts/sync-fhir-libs.ps1](scripts/sync-fhir-libs.ps1) / [sync-fhir-libs.sh](scripts/sync-fhir-libs.sh) |

Install and test: [FHIR_TESTING.md](../../../../FHIR_TESTING.md)

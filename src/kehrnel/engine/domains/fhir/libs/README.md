# FHIR libraries (vendored in Kehrnel)

Kehrnel ships **fhir-gen** and **fhir-mql** as first-class packages under this directory (`src/kehrnel/engine/domains/fhir/libs/`). The former repository-root `libs/` folder is no longer used. They are **part of this repository** (not git submodules). Each library keeps its own `pyproject.toml`, CLI, and tests and can be developed or deployed independently.

| Directory | Product | Python import | CLI |
|-----------|---------|---------------|-----|
| `fhir-data-generation/` | **fhir-gen** | `fhir_gen` | `fhir-gen` |
| `fhir-search-to-mql/` | **fhir-mql** | `fhir_search_to_mql` | `fhir-mql` |

**Indexes:** fhir-gen writes canonical documents only (no indexes on save). fhir-mql owns denormalization and index creation (`fhir_ensure_indexes`, `fhir-mql indexes`).

**Testing guide:** [FHIR_TESTING.md](../../../../../FHIR_TESTING.md)

## Install with Kehrnel (recommended)

From the kehrnel repo root:

```bash
pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql
pip install -e ".[api,mongo,fhir]"
```

The `[fhir]` extra installs both libraries from `src/kehrnel/engine/domains/fhir/libs/` via path dependencies in `pyproject.toml`. Core Kehrnel (`pip install -e .`) does **not** install FHIR libraries—openEHR and other domains are unaffected.

## Use a library on its own (inside Kehrnel)

```bash
pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation
fhir-gen --help

pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql
fhir-mql --help
```

Run each library’s tests from its directory:

```bash
cd src/kehrnel/engine/domains/fhir/libs/fhir-data-generation && pytest
cd src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql && pytest
```

## Kehrnel strategy integration

`fhir.clinical_cdr` imports these packages only through `src/kehrnel/engine/strategies/fhir/clinical_cdr/scripts/` (bridge, generation, denormalize, query). Do not import `fhir_gen` from unrelated kehrnel modules.

## Refresh from upstream (optional)

If you still maintain standalone repos at `code_repositories/fhir-data-generation` and `fhir-search-to-mql`, sync into `src/kehrnel/engine/domains/fhir/libs/` with:

```bash
# Bash (from kehrnel repo root)
./src/kehrnel/engine/domains/fhir/scripts/sync-fhir-libs.sh

# PowerShell
.\src\kehrnel\engine\domains\fhir\scripts\sync-fhir-libs.ps1
```

Review diffs before committing—`src/kehrnel/engine/domains/fhir/libs/` is the source of truth for Kehrnel CI and Docker builds.

## Docker

| Image | Dockerfile | Installs |
|-------|------------|----------|
| API (no FHIR) | `docker/Dockerfile.kehrnel-api` | `.[api,mongo]` |
| API + FHIR | `docker/Dockerfile.kehrnel-fhir` | `.[api,mongo,fhir]` |
| Legacy all-in-one | `Dockerfile.backend` | `.[all]` (includes FHIR) |

```bash
docker compose up kehrnel-api          # slim
docker compose --profile fhir up kehrnel-fhir-api   # includes engine/domains/fhir/libs
```

---
sidebar_position: 3
---

# Full Workflow Test

This page documents a complete smoke workflow for:

- local template-driven composition generation
- validation and flattening
- runtime capability discovery
- strategy maintenance operation
- synthetic generation dry-run
- query compile and query execution

## Script

Run:

```bash
examples/cli/full_workflow_console.sh
```

## Required Environment Variables

Set these before running:

```bash
export RUNTIME_URL=http://localhost:8080
export ENV_ID=dev
export DOMAIN=openehr
export STRATEGY_ID=openehr.rps_dual
export API_KEY="<x-api-key-if-auth-enabled>"
export BINDINGS_REF="env://DB_BINDINGS"
```

Optional:

```bash
export SOURCE_DB=hc_openEHRCDR
export SOURCE_COLLECTION=samples
export PATIENT_COUNT=50
export TEMPLATE_OPT=samples/templates/T-IGR-TUMOUR-SUMMARY.opt
export WORKDIR=.kehrnel/workflow-smoke
export VALIDATION_STRICT=0
export REQUIRE_RUNTIME=1
```

## What The Script Tests

1. CLI setup + health check
2. `map-skeleton` generation with macros (`code`, `term`, `system` shortcuts)
3. composition generation from OPT
4. composition validation against OPT
5. canonical-to-flattened transform
6. runtime activation (if `BINDINGS_REF` is provided)
7. capability discovery (`op capabilities`)
8. universal strategy run (`run ensure_dictionaries`)
9. synthetic batch dry-run (`run synthetic_generate_batch --dry-run`)
10. compile query + query via universal `run`

## Expected Outputs

The script writes artifacts under:

- `${WORKDIR}/mapping.skeleton.yaml`
- `${WORKDIR}/composition.json`
- `${WORKDIR}/flattened.json`
- `${WORKDIR}/synthetic.payload.json`
- `${WORKDIR}/query.aql`

If activation or source data are not available, runtime steps fail with explicit API errors and the script exits non-zero.

---
sidebar_position: 2
---

# openEHR RPS Dual CLI Workflows

Practical command recipes for `openehr.rps_dual` using the unified `{kehrnel}` CLI.

## Prerequisites

```bash
kehrnel setup --runtime-url http://localhost:8000 --domain openehr --strategy openehr.rps_dual
# or:
# kehrnel auth login --runtime-url http://localhost:8000
# kehrnel context set --env dev --domain openehr --strategy openehr.rps_dual
```

## Preferred Universal Workflow (resource + op + run)

```bash
# 1) Setup context
kehrnel setup --runtime-url http://localhost:8000 --env dev --domain openehr --strategy openehr.rps_dual

# 2) Define reusable source/sink profiles
kehrnel resource add src --type mongo --uri "$MONGODB_URI" --db hc_openEHRCDR --collection samples
kehrnel resource add dst --type mongo --uri "$MONGODB_URI" --db hdl_user_test --collection compositions_rps
kehrnel resource use --source src --sink dst

# 3) Discover capabilities and operation schemas
kehrnel op capabilities --env dev
kehrnel op schema synthetic_generate_batch --strategy openehr.rps_dual

# 4) Run maintenance and generation operations
kehrnel run ensure_dictionaries --env dev --domain openehr
kehrnel run synthetic_generate_batch --env dev --domain openehr --set patient_count=100 --dry-run
```

## Runtime Activation (Recommended)

To run the strategy through the runtime API (auth enabled), set an environment and activate with `bindings_ref`:

```bash
kehrnel context set --env dev --domain openehr --strategy openehr.rps_dual
kehrnel core env activate --bindings-ref env://DB_BINDINGS
```

RPS Dual uses dictionaries (`_codes`) and shortcuts (`_shortcuts`). On activation, `{kehrnel}` will try to bootstrap dictionaries automatically when `transform.apply_shortcuts=true`.

If you need to re-run seeding or rebuild:

```bash
kehrnel core env op ensure_dictionaries
kehrnel core env op rebuild_codes
kehrnel core env op rebuild_shortcuts
```

Equivalent universal commands:

```bash
kehrnel run ensure_dictionaries --env dev --domain openehr
kehrnel run rebuild_codes --env dev --domain openehr
kehrnel run rebuild_shortcuts --env dev --domain openehr
```

## Important: Pass-through syntax

`kehrnel common` accepts top-level context options, then forwards operation args after `--`.

```bash
kehrnel common <op> [--strategy ...] [--domain ...] -- <op_args...>
```

To inspect operation help:

```bash
kehrnel common transform -- --help
kehrnel common ingest -- --help
kehrnel common map -- --help
```

## Transform (canonical -> flattened)

```bash
kehrnel common transform -- -- flatten ./composition.json -o flattened.json
```

## Validate composition against OPT

```bash
kehrnel common validate -- -- -c ./composition.json -t ./template.opt --stats
```

## Generate minimal/random composition skeleton

```bash
kehrnel common generate -- -- -t ./template.opt -o generated.json
kehrnel common generate -- -- -t ./template.opt -o generated-random.json --random
```

## Map source data into canonical composition

```bash
kehrnel common map -- -- \
  -s ./source.json \
  -t ./webtemplate.json \
  -p ./template.opt \
  -o ./mapped-composition.json
```

With explicit mapping:

```bash
kehrnel common map -- -- \
  -m ./mapping.yaml \
  -s ./source.json \
  -t ./webtemplate.json \
  -p ./template.opt \
  -o ./mapped-composition.json
```

## Generate a mapping skeleton

```bash
kehrnel common map-skeleton -- -- ./template.opt -o mapping.skeleton.yaml --macros
```

## Ingest flattened NDJSON batch

```bash
kehrnel common ingest -- -- file ./batch.ndjson -d ./driver.yaml --workers 4
```

## Identify incoming document type

```bash
kehrnel common identify -- --document ./incoming/ --output identified.json --recursive
```

## Bundle operations

```bash
kehrnel common bundles -- validate-bundle ./bundle.json
kehrnel common bundles -- import-bundle ./bundle.json --upsert
kehrnel common bundles -- list-bundles
kehrnel common bundles -- export-bundle vital_signs.v1 ./exported.json
```

## Validate strategy pack

```bash
kehrnel common validate-pack -- ./src/kehrnel/engine/strategies/openehr/rps_dual --json
```

## Next

- [RPS Dual Configuration](/docs/strategies/openehr/rps-dual/configuration)
- [RPS Dual Data Model](/docs/strategies/openehr/rps-dual/data-model)
- [RPS Dual Query Translation](/docs/strategies/openehr/rps-dual/query-translation)

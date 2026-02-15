---
sidebar_position: 2
---

# openEHR RPS Dual CLI Workflows

Practical command recipes for `openehr.rps_dual` using the unified `{kehrnel}` CLI.

## Prerequisites

```bash
kehrnel auth login --runtime-url http://localhost:8000
kehrnel strategy use openehr.rps_dual --domain openehr
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
kehrnel common map-skeleton -- -- generate ./template.opt -o mapping.skeleton.yaml
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

- [RPS Dual Configuration](/docs/strategies/openehr-rps-dual/configuration)
- [RPS Dual Data Model](/docs/strategies/openehr-rps-dual/data-model)
- [RPS Dual Query Translation](/docs/strategies/openehr-rps-dual/query-translation)

---
sidebar_position: 4
---

# Common CLI Layer

`common` commands are compatibility pass-through workflows that execute local runners, optionally using selected strategy/domain context.

For extensible multi-strategy runtime workflows, prefer:

- `kehrnel resource ...`
- `kehrnel op ...`
- `kehrnel run ...`

## Command Pattern

Each command follows:

```bash
kehrnel common <operation> [--strategy ...] [--domain ...] -- <operation_args...>
```

Use `--` to pass operation-specific arguments to the underlying runner.

Examples:

```bash
kehrnel common transform --strategy openehr.rps_dual --domain openehr -- flatten input.json -o out.json
kehrnel common ingest -- -- file batch.ndjson -d driver.yaml
```

## Available Operations

- `transform`
- `ingest`
- `validate`
- `generate`
- `map`
- `map-skeleton`
- `identify`
- `bundles`
- `validate-pack`

## Context behavior

Context requirement depends on operation type:

- `transform`, `validate`, `generate`, `map` require strategy/domain context, either:
  - from `kehrnel context set`, or
  - by passing `--strategy` / `--domain` overrides.
- `ingest`, `identify`, `bundles`, `validate-pack`, `map-skeleton` run without strategy/domain context.

If a required command is missing context, CLI returns a clear prompt to set one.

## Operation Parameters

### `transform`

```bash
kehrnel common transform --strategy openehr.rps_dual --domain openehr -- flatten <source.json> -o <output.json>
```

Core params:
- `flatten <SOURCE>`
- `-o <PATH>` output (`-` for stdout)
- `-c <PATH>` optional transform config override

### `ingest`

```bash
kehrnel common ingest -- -- file <batch.ndjson> -d <driver.yaml> [--workers 4]
```

Core params:
- `file <JSONL>`
- `-d <PATH>` required driver config
- `--workers <INT>`

Driver helper subcommands:
- `drivers` list registered persistence drivers.
- `init-driver` scaffold driver config for `mongodb` or `filesystem`.
- `init-source-mongo` scaffold source Mongo config for `mongo-catchup`.
- `mongo-catchup` read canonical compositions from Mongo and write flattened docs to target driver.

Create driver config for MongoDB:

```bash
kehrnel common ingest -- -- drivers
kehrnel common ingest -- -- init-driver --driver mongodb --db openehr_db --out .kehrnel/driver.mongodb.yaml
export MONGODB_URI='mongodb+srv://...'
kehrnel common ingest -- -- file ./batch.ndjson -d .kehrnel/driver.mongodb.yaml
```

Create driver config for filesystem:

```bash
kehrnel common ingest -- -- init-driver --driver filesystem --base-path .kehrnel/persistence
kehrnel common ingest -- -- file ./batch.ndjson -d .kehrnel/driver.filesystem.yaml
```

### `validate`

```bash
kehrnel common validate --strategy openehr.rps_dual --domain openehr -- -c <composition.json> -t <template.opt>
```

Core params:
- `-c <PATH>` composition JSON
- `-t <PATH>` OPT template
- `--json`, `--verbose`, `--stats`, `--fail-on-warning`

### `generate`

```bash
kehrnel common generate --strategy openehr.rps_dual --domain openehr -- -t <template.opt> -o <out.json> [--random]
```

Core params:
- `-t <PATH>` OPT template
- `-o <PATH>` output (`-` for stdout)
- `--random`

### `map`

```bash
kehrnel common map --strategy openehr.rps_dual --domain openehr -- -s <source> -t <webtemplate.json> -p <template.opt> -o <out>
```

Core params:
- `-s <PATH>` source data (required)
- `-t <PATH>` web template JSON (required)
- `-p <PATH>` OPT template (required)
- `-o <PATH>` output path
- `-m <PATH>` optional mapping file
- `-S <strategy_id>` strategy mapping resolver
- `--strict`

### `map-skeleton`

Generate a mapping skeleton from an OPT or WebTemplate:

```bash
kehrnel common map-skeleton -- generate ./template.opt -o mapping.skeleton.yaml
```

### `identify`

```bash
kehrnel common identify -- --document <file-or-dir> --output <out.json>
```

Core params:
- `--document/-d <PATH>` input file or directory
- `--output/-o <FILE>` consolidated JSON output
- `--glob <pattern>`, `--recursive/--no-recursive`
- `--patterns/-p <FILE>` additional pattern files
- `--no-default`, `--debug`

### `bundles`

```bash
kehrnel common bundles -- validate-bundle <bundle.json>
kehrnel common bundles -- import-bundle <bundle.json> --upsert
kehrnel common bundles -- list-bundles
kehrnel common bundles -- export-bundle <bundle_id> <out.json>
```

### `validate-pack`

```bash
kehrnel common validate-pack -- <strategy_pack_path> [--json]
```

## Continue Reading

- [Strategy CLI Layer](/docs/cli/strategies)
- [CLI Overview](/docs/cli/overview)
- [openEHR RPS Dual CLI Workflows](/docs/strategies/openehr/rps-dual/cli-workflows)

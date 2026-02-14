---
sidebar_position: 1
---

# CLI Overview

\{kehrnel\} provides a comprehensive set of command-line tools for managing your Clinical Data Repository. All commands follow the `kehrnel-<command>` naming convention.

## Available Commands

| Command | Description |
|---------|-------------|
| `kehrnel-api` | Start the REST API server |
| `kehrnel-transform` | Transform compositions between canonical and flattened formats |
| `kehrnel-ingest` | Bulk ingest flattened documents into MongoDB |
| `kehrnel-validate` | Validate compositions against OPT templates |
| `kehrnel-validate-pack` | Validate strategy pack structure and configuration |
| `kehrnel-generate` | Generate composition skeletons from templates |
| `kehrnel-map` | Transform source data to openEHR compositions using mappings |
| `kehrnel-skeleton` | Generate mapping skeleton files from templates |
| `kehrnel-map-skeleton` | Alias entrypoint for mapping skeleton generation |
| `kehrnel-identify` | Identify document types using pattern matching |
| `kehrnel-validate-bundle` | Validate bundle structure |
| `kehrnel-import-bundle` | Import bundles into the store |
| `kehrnel-list-bundles` | List all stored bundles |
| `kehrnel-export-bundle` | Export a bundle to a file |

## Quick Examples

### Start the API Server

```bash
kehrnel-api --host 0.0.0.0 --port 8000
```

### Transform a Composition

```bash
kehrnel-transform flatten ./composition.json -o flattened.json
```

### Validate Against a Template

```bash
kehrnel-validate -c composition.json -t template.opt
```

### Generate a Composition Skeleton

```bash
kehrnel-generate -t template.opt -o skeleton.json --random
```

## Getting Help

All commands support `--help` for detailed usage information:

```bash
kehrnel-api --help
kehrnel-transform --help
kehrnel-validate --help
```

## Environment Variables

Most commands respect the following environment variables when applicable:

| Variable | Description |
|----------|-------------|
| `CORE_MONGODB_URL` | MongoDB connection string |
| `CORE_DATABASE_NAME` | Default database name |
| `KEHRNEL_API_HOST` | API server bind host |
| `KEHRNEL_API_PORT` | API server bind port |

See [Configuration](/docs/getting-started/configuration) for the complete list.

## Canonical Inventory

For exhaustive command and endpoint inventory generated from `pyproject.toml` and OpenAPI, see:

- [Canonical CLI + API Inventory (generated)](https://github.com/mongodb-industry-solutions/kehrnel/blob/main/docs/cli-api-reference.md)

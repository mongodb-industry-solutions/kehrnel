---
sidebar_position: 5
---

# Source Sampling for Synthetic Data

The openEHR RPS Dual strategy can generate synthetic compositions by sampling real or reference compositions from a source collection. This happens in the `synthetic_generate_batch` op, usually run as a synthetic job (`POST /environments/{env_id}/synthetic/jobs`).

## Generation Modes

`generation_mode` selects where each template's content comes from:

| Mode | Behavior |
|------|----------|
| `auto` (default) | try the source collection first, then a model definition |
| `from_source` | sample from the source collection |
| `from_models` | use model definitions from the model catalog |

The request lists templates through `models` or `templates`; in source mode, templates can also be discovered from the source collection.

## Sampling Parameters

| Parameter | Meaning |
|-----------|---------|
| `source_collection` | collection to sample from (default `samples`) |
| `source_database` | optional database for the source collection |
| `source_filter` | MongoDB filter combined with the template match |
| `source_sample_size` | in source mode, caps template discovery and sets the sample size per discovered template (default 200) |
| `sample_size` / `sample_pool_size` | per-template sample size when templates are listed explicitly (default 25) |
| `source_min_per_patient`, `source_max_per_patient` | compositions per synthetic patient in source mode |

For each template, the strategy runs `$match` (template id plus `source_filter`) followed by `$sample` on the source collection. If no source document and no model definition is found, the op fails with `SOURCE_TEMPLATE_NOT_FOUND`.

## Safety Switches

- `plan_only` returns the generation plan without generating documents
- `dry_run` generates and transforms documents without writing them
- `store_canonical` additionally writes the generated canonical compositions (default collection `compositions_canonical_synthetic`)

## Related

- [openEHR RPS Dual CLI Workflows](/docs/strategies/openehr/rps-dual/cli-workflows)
- [Ingest and Transformation](/docs/architecture/transformation-pipeline#synthetic-generation) - synthetic jobs across domains


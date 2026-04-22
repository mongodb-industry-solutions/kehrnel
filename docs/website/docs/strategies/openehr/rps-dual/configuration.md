---
sidebar_position: 4
---

# RPS Dual Configuration

Use `src/kehrnel/engine/strategies/openehr/rps_dual/defaults.json` as the
authoritative activation baseline for `openehr.rps_dual`.

That file is what the runtime merges into an environment when the strategy is
activated. In practice, most users should keep the defaults and apply small,
explicit overlays for their deployment or example workflow.

## How The Strategy Pack Is Wired

These files work together and should stay aligned:

- `manifest.json`: exposes the strategy to the runtime, including entrypoint,
  capabilities, maintenance ops, and documentation metadata.
- `defaults.json`: the default activation config merged into the environment.
- `schema.json`: the user-facing validation contract for activation overrides.
- `config.py`: the Python mirror of the config model; it normalizes raw config
  and builds the helper structures consumed by the flattener and query layer.
- `spec.json`: the machine-readable strategy specification for logical model,
  storage model, encoding profiles, and index planning.
- `strategy.py`: the runtime implementation for `plan`, `apply`, `ingest`,
  `query`, and maintenance ops.
- `bundles/...`: packaged seed artifacts for `_codes`, `_shortcuts`, and the
  sample Atlas Search definition snapshot.

In other words, `manifest.json` tells Kehrnel that the strategy exists,
`defaults.json` says how it starts, `schema.json` says what users may change,
`config.py` turns that into runtime-safe Python structures, `spec.json`
describes what should be materialized, and `strategy.py` actually performs the
work.

## Activation Baseline

The current default activation baseline is:

```json
{
  "collections": {
    "compositions": {
      "name": "compositions_rps",
      "encodingProfile": "profile.codedpath"
    },
    "search": {
      "name": "compositions_search",
      "encodingProfile": "profile.search_shortcuts",
      "enabled": true,
      "atlasIndex": {
        "name": "search_nodes_index",
        "definition": "file://bundles/searchIndex/searchIndex.json"
      }
    },
    "codes": {
      "name": "_codes",
      "seed": "file://bundles/dictionaries/_codes.json"
    },
    "shortcuts": {
      "name": "_shortcuts",
      "seed": "file://bundles/shortcuts/shortcuts.json"
    }
  },
  "ids": {
    "ehr_id": "string",
    "composition_id": "objectid"
  },
  "paths": {
    "separator": "."
  },
  "fields": {
    "document": {
      "ehr_id": "ehr_id",
      "comp_id": "comp_id",
      "tid": "tid",
      "v": "v",
      "time_committed": "time_c",
      "sort_time": "sort_time",
      "cn": "cn",
      "sn": "sn"
    },
    "node": {
      "p": "p",
      "pi": "pi",
      "data": "data"
    }
  },
  "transform": {
    "apply_shortcuts": true,
    "coding": {
      "arcodes": {
        "strategy": "sequential"
      },
      "atcodes": {
        "strategy": "negative_int",
        "store_original": false
      }
    }
  },
  "bootstrap": {
    "dictionariesOnActivate": {
      "codes": "ensure",
      "shortcuts": "seed"
    }
  }
}
```

Important detail: there is no top-level `database` property in the current
strategy activation config. Database connectivity comes from runtime bindings,
not from `defaults.json`.

## What Users Can Override

The main supported override areas are:

- `collections.compositions.name` and `collections.search.name` if you want
  different MongoDB collection names.
- `collections.compositions.encodingProfile` and
  `collections.search.encodingProfile` if you want to switch between the
  currently supported encoded-path profiles:
  `profile.codedpath` and `profile.search_shortcuts`.
- `collections.search.enabled` if you want a single-store mode without the slim
  search sidecar.
- `collections.codes.*` and `collections.shortcuts.*` if you want different
  dictionary collection names or seed sources.
- `collections.search.atlasIndex.name` if your Atlas Search index needs a
  different name.
- `ids.*` if you want to change how `ehr_id` or `composition_id` are encoded.
- `fields.document.*` and `fields.node.*` if you want different field labels in
  stored MongoDB documents.
- `transform.apply_shortcuts` if you want the slim search payload to preserve
  full RM key names instead of shortcut labels.
- `transform.coding.arcodes.*` and `transform.coding.atcodes.*` if you want
  different archetype or at-code strategies.
- `transform.mappings` if you want the search-side projection to be built from a
  specific mappings file or inline object.
- `bootstrap.dictionariesOnActivate.*` if you want dictionary collections only
  ensured, fully seeded, or skipped at activation time.

Current consistency boundary:

- `paths.separator` is fixed to `.` in the current strategy surface.
- `collections.compositions.encodingProfile` and
  `collections.search.encodingProfile` are restricted to
  `profile.codedpath` or `profile.search_shortcuts`.
- when the search sidecar is enabled, the strategy now emits the configured
  `comp_id` field into the search document as well, so search-side lookup and
  metadata filters use the same identifier label as the base document.
- top-level `ehr_id` and `comp_id` predicates are coerced according to
  `ids.ehr_id` and `ids.composition_id`, so the query layer matches the ingest
  layer for string, `objectid`, and UUID-backed storage.

## Congruence Rules

User-facing config in this strategy is spread across JSON files and Python
builders, so it is important to keep them congruent.

When a config surface changes, update these together:

- `defaults.json`
- `schema.json`
- `config.py`
- `spec.json`
- strategy docs and tests

Why this matters:

- `config.py` builds the flattener config and query compiler schema from the
  normalized model.
- `strategy.py` uses the same normalized config for ingest, planning, and
  maintenance ops.
- `spec.json` drives collection/index planning, so field and collection names in
  the spec must still describe what the Python implementation will materialize.

If one of these surfaces drifts, the docs may look correct while activation,
ingest, query planning, or index materialization silently use different names.

## Mappings Drive The Search Side

The full dual-collection example needs `transform.mappings`.

Without mappings:

- the strategy can still activate successfully
- the base `compositions_rps` documents still ingest correctly
- the search collection and Atlas Search artifacts can still be planned
- but no meaningful slim `compositions_search` sidecar is produced

With mappings:

- the flattener knows which fields to project into `sn`
- the search-side document shape is derived from that mapping
- the Atlas Search definition can be generated from the same mapping

Recommended rule: treat the Atlas Search definition as derived from
`transform.mappings`, not as a hand-maintained artifact.

The packaged `bundles/searchIndex/searchIndex.json` file is still useful as a
seed or snapshot, but it should reflect the same mappings-driven workflow rather
than become a separate source of truth.

Generate the current definition from the active config with:

```bash
kehrnel strategy build-search-index \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual \
  --out .kehrnel/search-index.json
```

## Full Example Overlay

For the packaged reference sample, keep the defaults and add only the mappings
overlay needed to build the slim search sidecar:

```json
{
  "transform": {
    "mappings": "file://samples/reference/projection_mappings.json"
  }
}
```

That `file://...` URI is resolved relative to the strategy root, so the path is
interpreted under `src/kehrnel/engine/strategies/openehr/rps_dual/`.

Typical activation flow:

```bash
cat > .kehrnel/rps-dual.config.json <<EOF
{
  "transform": {
    "mappings": "file://samples/reference/projection_mappings.json"
  }
}
EOF

kehrnel core env activate \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual \
  --config .kehrnel/rps-dual.config.json \
  --allow-plaintext-bindings \
  --bindings .kehrnel/bindings.mongo.yaml
```

This is usually the best way to reproduce the full sample workflow: defaults for
everything else, one explicit mappings overlay for the search side.

## Common Variants

Disable the search collection entirely:

```json
{
  "collections": {
    "search": {
      "enabled": false
    }
  }
}
```

Rename the main collections:

```json
{
  "collections": {
    "compositions": {
      "name": "compositions_rps_prod"
    },
    "search": {
      "name": "compositions_search_prod"
    }
  }
}
```

Use literal coding for easier debugging:

```json
{
  "transform": {
    "apply_shortcuts": false,
    "coding": {
      "arcodes": {
        "strategy": "literal"
      },
      "atcodes": {
        "strategy": "literal",
        "store_original": true
      }
    }
  }
}
```

Change the stored field labels:

```json
{
  "fields": {
    "document": {
      "cn": "canonical_nodes",
      "sn": "search_nodes"
    }
  }
}
```

Start with defaults, verify that baseline, then introduce one variant at a time.
That makes it much easier to see whether a change affects ingest, search-side
projection, query compilation, or planned indexes.

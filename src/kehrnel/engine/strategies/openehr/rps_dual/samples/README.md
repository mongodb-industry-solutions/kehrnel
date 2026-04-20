# RPS Dual Sample Pack

This folder is reserved for neutral, strategy-owned sample data.

Sample assets committed here should not expose:

- upstream project labels
- organization-specific names
- real patient or practitioner identifiers
- real source system identifiers

Use `export_pack.py` to build a sanitized sample pack from a private source.

The exporter is intentionally generic:

- source template names are passed at runtime
- replacement tokens are passed at runtime
- MongoDB connection strings are passed at runtime

This keeps the repository free of upstream-specific labels while still making it
easy to generate a reproducible local sample pack for tutorials, smoke tests,
and CLI walkthroughs.

## What The Envelopes Are

The files under `reference/envelopes/*.ndjson` are ingest-ready wrapper
documents.

Each line is one composition envelope with:

- `canonicalJSON`: the masked canonical openEHR composition
- `ehr_id`: the masked EHR identifier used by ingestion and query examples
- `template_id` and `template_name`: the neutral template identifier
- `composition_version`: the version used to populate document field `v`
- `time_committed`: the timestamp used to populate document field `time_c`
- `archetype_node_id` and `composition_date`: helper metadata for examples

Use `reference/canonical/` when you want raw masked compositions.
Use `reference/envelopes/` when you want to ingest the packaged sample dataset
through the real RPS Dual ingestion flow.

## What the exporter does

- rewrites the composition `template_id`
- applies token replacements across canonical JSON and OPT files
- masks wrapper `ehr_id`
- masks composition `uid.value`
- masks `DV_IDENTIFIER.id` values
- masks `PARTY_IDENTIFIED.name` values
- normalizes `FEEDER_AUDIT_DETAILS.system_id` to `sample.source`
- writes canonical JSON files, NDJSON envelopes, rewritten OPTs, and a manifest

## Example

```bash
python -m kehrnel.engine.strategies.openehr.rps_dual.samples.export_pack \
  --mongo-uri "$MONGODB_URI" \
  --source-db private_source_db \
  --source-collection samples \
  --sample "Legacy Immunization Template|sample_immunization_v0.5|/secure/templates/immunization.opt" \
  --sample "Legacy Laboratory Template|sample_laboratory_v0.4|/secure/templates/laboratory.opt" \
  --replace-token "Legacy=Sample" \
  --replace-token "legacy=sample" \
  --limit 25 \
  --out-dir ./src/kehrnel/engine/strategies/openehr/rps_dual/samples/generated
```

The generated output layout is:

```text
generated/
  manifest.json
  projection_mappings.json
  search_index.definition.json
  canonical/
    sample_immunization_v0_5/
    sample_laboratory_v0_4/
  envelopes/
    sample_immunization_v0_5.ndjson
    sample_laboratory_v0_4.ndjson
  templates/
    sample_immunization_v0_5.opt
    sample_laboratory_v0_4.opt
```

## Packaged example contract

The committed `reference/` sample pack is intended to be runnable as-is in CLI
and documentation examples. It therefore includes:

- canonical JSON samples under `reference/canonical/`
- template-specific NDJSON envelopes under `reference/envelopes/`
- `reference/envelopes/all.ndjson` for one-shot batch ingestion
- neutral OPTs under `reference/templates/`
- `reference/projection_mappings.json` for the slim search projection
- `reference/search_index.definition.json` as the generated Atlas Search seed
- hand-authored AQL examples under `reference/queries/`

The projection mappings and search index definition are derived from the same
RPS Dual mappings workflow used by ingestion, so the sample search collection
and index seed stay aligned.

The AQL examples are maintained alongside the packaged sample dataset and are
validated in tests by compiling them against the bundled dictionaries and the
packaged envelopes after flattening.

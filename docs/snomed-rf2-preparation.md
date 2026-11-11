# SNOMED RF2 preparation and ingestion

`snomedct.mongodb` 0.7.0 adds an RF2 Snapshot preparation path. RF2 and supported concept JSON feed the same existing canonical normalizer, JSON loader, term sidecar and index operations. Preparation and ingestion are separate, explicit steps.

## Prepare locally

Use a licensed Snapshot directory or ZIP containing the chosen edition and required extension dependencies. The converter streams component rows into temporary SQLite storage, joins them by ID, validates the supplied inferred taxonomy and writes concept-centric JSON. It preserves descriptions, language acceptability, role groups, concrete values, OWL axioms and available mapping members with their rules and provenance. It does not invent concept IDs, classify OWL or infer missing mappings.

```bash
# Complete validation. Temporary files are removed; no output is retained.
python -m kehrnel.engine.domains.snomedct.rf2 ./Snapshot \
  --release-id 20261001 --language en --language es

# Prepare a new licensed derivative and its receipt.
python -m kehrnel.engine.domains.snomedct.rf2 ./Snapshot \
  --release-id 20261001 --language en --language es \
  --output ./.kehrnel/snomedct/releases/rf2-20261001.json \
  --license-acknowledged
```

The installed command `kehrnel-snomed-rf2` has the same arguments. Python callers use `kehrnel.engine.domains.snomedct.rf2.prepare_rf2`; omit `output` for a complete dry run.

The output receipt is JSON named `<output>.receipt`. It includes source file checksums, source and output digests, component counts, active languages, projected term count and hierarchy validation. Existing files are never replaced. Complete output files are published atomically with exclusive creation. Failure to publish the receipt rolls back the newly created concept JSON.

## Validate and review

Missing dependencies, malformed rows, conflicting component duplicates, effective dates newer than the selected release, unsafe archive paths and inferred hierarchy cycles stop preparation. Identical duplicate rows are counted once. Full and Delta imports are unsupported; only Snapshot files are read. Unsupported Snapshot files are inventoried with a warning. Missing language refsets produce an explicit preferred-term fallback warning.

Only active supplied inferred **is-a** relationships build parents, children and ancestor arrays. Stated relationships and OWL expressions are retained, but cannot substitute for a classified release. Counts describe the supplied files and configured projection languages; they do not certify completeness of a national edition.

## Tenant API and Data Lab

`POST /api/domains/snomedct/releases/prepare-rf2` accepts:

```json
{"path":"Snapshot","release_id":"20261001","languages":["en","es"],"dry_run":true}
```

To write a derivative, set `dry_run: false`, provide a new `output_file_name` ending in `.json`, and acknowledge authorization with `license_acknowledged: true`. Source and output paths stay inside the active strategy's configured `source.local_dir`. Administrative access and environment write controls apply.

The capability response advertises `snomed_prepare_rf2`, the preparation contract and accepted source formats. Healthcare Data Lab enables its RF2 controls from that response. The public developer path prepares requests; the authenticated terminology workbench executes them in the selected environment.

Inline staging accepts JSON or RF2 ZIP up to 5 MB. Full distributions should be staged on the server or use an existing signed artifact-store binding. Preparation runs in a worker thread but remains a synchronous request. Use local Python preparation when gateway timeouts are short. Temporary disk must accommodate component joins and the JSON derivative; full-edition scale needs deployment qualification. No durable preparation queue is advertised in this version.

## Shared ingestion path

1. Inspect the prepared JSON with `releases/inspect`.
2. Compare it with the selected MongoDB release through `releases/diff`.
3. Dry-run `releases/ingest`.
4. Acknowledge authorization and ingest using the existing batch/upsert behavior.
5. Rebuild the term sidecar and ensure indexes through the existing operations.
6. Read `readiness` for actual concept/term counts and optional retrieval bindings.

Canonical identity remains `releaseId|conceptId`. Default ingestion rebuilds the sidecar. Keep replacement/deletion off unless explicitly selected by the operator. Term projections support terminology retrieval and grounding; clinical source records and reviewed clinical facts remain separate data products.

Synthetic tests cover directory/ZIP parity, deterministic digests, preferred-language projection, inferred closure, stated/inferred separation, role groups, concrete values, preserved map/OWL members, licensing, tenant path isolation and existing loader/ingestion compatibility. They contain no official release.

Conditional mapping execution, FHIR/legacy source adapters and a shared cross-source clinical fact index remain subsequent work. Existing bounded ECL and terminology-server limits still apply; preparation success does not imply full FHIR conformance or independent clinical validation.

The reusable training fixture is domain-owned at `src/kehrnel/engine/domains/snomedct/samples/rf2-synthetic`, with a dataset version and checksummed source manifest. It is shared independently of a persistence strategy and is included in the installed package.

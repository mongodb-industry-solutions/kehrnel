# DICOM querying examples

These requests use the native `dicom.tiered_store` 0.3.0 pack. They exercise synthetic metadata only; there are no real patient records or pixels. `dicom_query_catalog` publishes the same example requests and active-field discovery for Healthcare Data Lab and agent tools.

The new configurable-rule journey is documented in `docs/dicom-rule-playground.md`. Run `PYTHONPATH=src python examples/sdk/dicom_rule_journey.py` for a no-write demonstration, or call its `journey(ctx, persist=True)` with disposable storage/index bindings. `dicom_rule_catalog` discovers profiles and limits; `dicom_rule_receive` and `dicom_rule_process` separate reception from cumulative consolidation. Their frozen series generations differ from the compatibility instance-level research examples below.

Use a fresh demonstration environment with default centers, a database binding, and `projections.enabled=true`. The activation recipe deliberately contains no credentials; configure resolver-backed bindings through the existing runtime. Keep Search disabled for these regular-index examples.

1. Apply the strategy's collection/index plan.
2. Run `dicom_generate_synthetic` with `seed.json`. It writes two studies, two series and 24 instances.
3. Execute viewer queries in `queries/`; request selected headers with `headers.json`.
4. Run `dicom_rebuild_projections` with `research-build.json`. Follow each returned `page.nextCursor` until null, passing it as `cursor` on the next request. No Search index is needed.
5. Execute the manufacturer and manufacturer/thickness requests. Default expectations are 12 and 2 instance rows respectively. These are partial instance projections, not distinct studies or complete cohorts.

```sh
kehrnel run dicom_generate_synthetic --env imaging --domain dicom --payload examples/dicom/seed.json
kehrnel run dicom_query_catalog --env imaging --domain dicom
kehrnel run dicom_query --env imaging --domain dicom --payload examples/dicom/queries/patient-day.json
kehrnel run dicom_get_headers --env imaging --domain dicom --payload examples/dicom/headers.json
kehrnel run dicom_rebuild_projections --env imaging --domain dicom --payload examples/dicom/research-build.json
kehrnel run dicom_query --env imaging --domain dicom --payload examples/dicom/queries/manufacturer-thickness.json
```

The generic HTTP `/run` body is `{"domain":"dicom","operation":"dicom_query","payload":<request>}`. `/compile_query` and `/query` accept `{"domain":"dicom","query":<request>}`. The SDK equivalent is exercised by `examples/sdk/dicom_query_journey.py`, whose `run_journey(ctx, seed=True)` applies the plan, seeds, backfills and executes all nine examples with bound adapters. Running the script directly compiles the examples without connecting to a database.

Header batches preserve source DICOM attributes and report missing tags. They are capped at 100 SOP UIDs, 32 selected tags and a configured response-byte budget. Retry `page.deferredSopUids` using a smaller batch or fewer fields. Study/series descriptions use exact matching by default; prefix browsing is limited to one source day.

All IDs and expected counts belong to the default synthetic seed. Discovery marks syntax/policy availability; it does not check whether the seed exists. Do not execute seed operations in a customer production database.

See `docs/dicom-implementation-plan.md` for the Kehrnel/Healthcare Data Lab phases and acceptance criteria.

The provider-neutral catalog starter is demonstrated by `examples/sdk/dicom_builder_journey.py`. See `docs/dicom-builder-handoff.md` for exact discovery, explicit preview activation and the remaining canonical Builder work.

## Domain-owned source samples

The frozen rule playground sources live under `src/kehrnel/engine/domains/dicom/samples`; they are shared domain assets, not strategy fixtures. Run `PYTHONPATH=src python examples/sdk/dicom_domain_samples.py` to load them through a local SDK preview. The optional `--samples-dir` selects a catalog directory client-side. See `docs/dicom-domain-samples.md` for source versions, integrity and future public imaging packages.

# CDISC example data

`catalog.json` references small, public clinical and preclinical examples. The
source files are intentionally not vendored into Kehrnel. Every URL is pinned
to a source revision and every downloaded file is verified with SHA-256.

- `cdisc-pilot-sdtm-dm` is the official CDISC clinical pilot DM example.
- `phuse-ffu-send` is the compact PhUSE FFU SEND example used by the guided single-study journey.
- `phuse-nimble-send` is an independent 100-animal rat study with three dose groups.
- `phuse-instem-send` is an independent 241-animal rat study with a large pathology and laboratory corpus.
- `phuse-pointcross-send` is an independent 150-animal rat study with main and recovery cohorts.
- `phuse-pds-send` is the deep safety vertical: a 124-animal Sprague-Dawley
  study with both sexes, terminal and recovery cohorts, 25 SEND domains,
  laboratory reference fields, toxicokinetics, organ weights, and RELREC.

All five SEND studies come from the public PhUSE SENDConform repository and
retain its MIT license, pinned revision, source URL, and per-file checksum.

Fetch either example with:

```bash
python scripts/fetch_cdisc_examples.py cdisc-pilot-sdtm-dm /tmp/cdisc-clinical
python scripts/fetch_cdisc_examples.py phuse-ffu-send /tmp/cdisc-preclinical
python scripts/fetch_cdisc_examples.py phuse-nimble-send /tmp/cdisc-nimble
python scripts/fetch_cdisc_examples.py phuse-pds-send /tmp/cdisc-pds
```

For a local, deployment-neutral solution handoff, exercise the complete
fetch-to-publish workflow and export the resulting evidence package with:

```bash
uv run --extra cdisc --extra api python scripts/export_cdisc_example_evidence.py \
  phuse-pds-send /tmp/PDS2014-solution-evidence.json --acknowledge-terms
```

The command uses transient local strategy storage. It does not bypass checksum
verification, validation, publication, projection, or the governed package
export operation.

An activated strategy exposes the same catalog through
`cdisc_list_examples`. After showing the attribution and terms, a client can
call `cdisc_ingest_example` with `acknowledgeTerms: true`. Setting `validate`
and `publish` to true creates the shortest complete learning journey: fetch,
checksum verification, artifact retention, XPT/Define ingestion, validation,
publication, and projection rebuild. Publication is rejected when validation
is disabled.

The catalog is product metadata, not a claim that the upstream data is
regulatory-conformant. Display the source attribution and license/terms link in
Healthcare Data Lab before downloading.

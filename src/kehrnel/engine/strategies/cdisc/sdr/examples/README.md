# CDISC example data

`catalog.json` references small, public clinical and preclinical examples. The
source files are intentionally not vendored into Kehrnel. Every URL is pinned
to a source revision and every downloaded file is verified with SHA-256.

- `cdisc-pilot-sdtm-dm` is the official CDISC clinical pilot DM example.
- `phuse-ffu-send` is a PhUSE SEND example distributed under the MIT license.

Fetch either example with:

```bash
python scripts/fetch_cdisc_examples.py cdisc-pilot-sdtm-dm /tmp/cdisc-clinical
python scripts/fetch_cdisc_examples.py phuse-ffu-send /tmp/cdisc-preclinical
```

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

# Domain-owned DICOM samples

DICOM source datasets belong to the **domain**, independently of a persistence strategy. A study/series/instance export can exercise different data models, projection profiles and viewers. Its source version and provenance must survive those changes.

## Directory ownership

The bundled source directory is:

```text
src/kehrnel/engine/domains/dicom/
  sample_data.py
  samples/
    catalog.json
    synthetic-imaging/
      1.0.0/
        manifest.json
        mixed/{studies.json, instances.json}
        ct/{studies.json, instances.json}
        mr/{studies.json, instances.json}
        mg/{studies.json, instances.json}
        legacy/{studies.json, instances.json}
```

These are strategy-neutral **source documents** in the customer's study-with-embedded-series and instance-header/ContentIndex shape. They contain no `_kehrnel` projection documents, Search index definitions or activation settings. The parameterized synthetic generator remains an operation of the current strategy; the frozen source fixtures no longer depend on executing it.

| Asset | Owner / location |
| --- | --- |
| Original source metadata, source manifest, licenses and sample identities | DICOM domain sample package |
| Extraction rules, persistence transformations, projection schema and index definitions | Persistence strategy |
| Generated collections and query results | Activation/project; pinned to both source and projection versions |
| Frozen public showcase images, query evidence and release manifest | Healthcare Data Lab publisher release, referencing domain source IDs |
| Customer exports and binary locators | Customer-controlled local storage; never automatically promoted into a public package |

The existing root `samples/dicom` customer exports remain separate and untouched. A future local package can use the same catalog format, but placing files in that folder does not register or authorize them automatically. Neither strategy activation nor HTTP discovery scans arbitrary local directories.

## Available source packages

`dicom.synthetic-imaging`, version `1.0.0`, provides five variants. Each has two studies, two embedded series and twelve instances. `mixed` covers illustrative CT/MR; `ct`, `mr` and `mg` retain the earlier playground variations; `legacy` retains sparse identifiers and object class metadata. These are small regression/playground fixtures, not representative modality cohorts or clinically valid image payloads. Their illustrative ContentIndex has no corresponding binary.

The migration preserves all five existing demo source fingerprints, including BSON dates and DICOM numeric strings. A DS or IS source value remains a DICOM value with its VR; rules determine whether and how to derive a typed query field.

The catalog identifies datasets by domain, ID, version and manifest SHA-256. Each manifest records source provenance, rights, pixel availability, variants, counts and per-file hashes. A changed source is a **new dataset version**. Projection configuration changes have their own versions; changing a projection does not rewrite its source dataset.

The native `dicom_rule_catalog` exposes the source manifest alongside the strategy's rules. `dicom_rule_demo` selects an existing preset, loads its frozen domain source and returns `sampleDataset` with its exact reference and variant. The strategy pins the manifest digest, so replacing a same-version fixture cannot silently alter the demonstration. Existing preset requests and source data remain compatible.

## Loading a directory

Local SDK callers can load a known package from a domain sample root:

```python
from pathlib import Path
from kehrnel.engine.domains.dicom.sample_data import describe_sample, load_sample

metadata = describe_sample("dicom.synthetic-imaging", "1.0.0")
source = load_sample(
    "dicom.synthetic-imaging", "1.0.0", variant="mixed",
    expected_digest=metadata["reference"]["digest"],
)

# For a separately registered local package, pass its catalog directory:
# source = load_sample(dataset_id, version, root=Path("/local/dicom-samples"),
#                      variant=variant, expected_digest=pinned_digest)
```

The loader verifies the manifest and source-file hashes, rejects paths/symlinks outside the package directory, preserves MongoDB Extended JSON types, enforces a combined 12 MiB / 500-document bound and returns fresh source objects. These are small sample loading limits, not an ingestion design for the production corpus. HTTP requests cannot supply `root` or a local path; the existing native source/profile payload is the remote ingestion contract.

Run `PYTHONPATH=src python examples/sdk/dicom_domain_samples.py` to load the domain source and send it through the native SDK preview. `--samples-dir` selects a local catalog directory only in this client-side example. It does not connect to MongoDB or write collections.

## Public visual dataset expansion

Use separate domain packages for licensed public sources, rather than moving them beneath `tiered_store`. Candidate packages include Lung-PET-CT-Dx (CT/PT), PROSTATEx (MR), LIDC-IDRI (CT/radiographs), and CBIS-DDSM (mammography). A candidate is not advertised as installed before its metadata, licensing and integrity are verified.

Each acquired package should retain collection/release identifiers, DOI/source URLs, the exact applicable license and attribution, original study/series/SOP/frame identifiers, download hashes and any documented de-identification limitations. Record derived previews separately with their source image/frame references. Large DICOM binaries belong in local/object storage outside the small Python package, with immutable references in the domain manifest.

Only link patients or modalities where the source supplies a trustworthy identity relation. A representative modality image can illustrate a story but cannot masquerade as an image from a matching metadata result. Public collection metadata summaries do not establish that all underlying instance headers were inspected.

Healthcare Data Lab then publishes a frozen visual showcase release with selected cases, thumbnails/frames, cohort metadata and verified query results. The developer workbench consumes the same domain sources through Kehrnel and lets developers change mappings/rules, create projections and inspect query plans. The current synthetic rule playground remains a developer prototype; domain sample packaging alone does not deliver the public imaging application.

## Validation

Source fidelity, independent consumers, version pins, checksum corruption, local directory portability, containment and read limits are tested in `tests/unit/test_dicom_domain_samples.py`. Existing rule, canonical resolution and HTTP/CLI contracts are exercised alongside them. Refresh strategy/catalog references after changing the strategy's source reference; source manifests have their own independent hashes.

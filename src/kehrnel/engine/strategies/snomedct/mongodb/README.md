# SNOMED CT on MongoDB (`snomedct.mongodb`)

Strategy pack for storing official SNOMED CT JSON releases in MongoDB.

The pack creates four tenant-scoped data products:

- Canonical concepts: one document per SNOMED CT concept and release.
- Term sidecar: one document per searchable description and language.
- ValueSet registry: immutable URL/version definitions pinned to a release.
- Grounding reviews: reviewer decisions with materialized ancestor paths.

The canonical collection is mandatory. It supports lookup, basic ECL/subsumption,
release inspection, and release diff workflows. The sidecar is configurable but
default-on because terminology search, NLP grounding, and benchmark retrieval
depend on term-level projections.

## Main Ops

- `snomed_list_releases`: list licensed JSON files staged in the configured local folder.
- `snomed_inspect_release`: stream-count and inspect a release file.
- `snomed_diff_release`: compare official JSON against the canonical MongoDB collection.
- `snomed_ingest_release`: normalize and upsert canonical concept documents.
- `snomed_rebuild_sidecar`: regenerate term documents from canonical concepts.
- `snomed_search`: lexical search over the term sidecar.
- `snomed_suggest` and `snomed_concept_history`: authoring and release navigation.
- `snomed_ground_note`: candidate grounding for extracted clinical mentions.
- `snomed_save_grounding_review` and `snomed_query_grounded_corpus`: governed review and concept/ancestor queries.
- `snomed_benchmark_retrieval`: bounded hit@k and mean-reciprocal-rank evidence.

MongoDB connectivity comes from Kehrnel activation bindings. The customer obtains
licensed SNOMED CT JSON through official channels and places it in `source.local_dir`.
Do not put MongoDB URIs or SNOMED distribution credentials in the manifest or defaults.

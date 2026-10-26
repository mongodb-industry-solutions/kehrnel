# SNOMED CT on MongoDB (`snomedct.mongodb`)

Strategy pack for storing official SNOMED CT JSON releases in MongoDB.

The pack creates six tenant-scoped data products:

- Canonical concepts: one document per SNOMED CT concept and release.
- Term sidecar: one document per searchable description and language.
- ValueSet registry: immutable URL/version definitions pinned to a release.
- Grounding reviews: reviewer decisions with materialized ancestor paths.
- Gold notes: tenant-authored benchmark fixtures with expected facts.
- Usage events: product telemetry (search executed, result selected, feedback).

The canonical collection is mandatory. It supports lookup, basic ECL/subsumption,
semantic scope resolution, release inspection, and release diff workflows. The
sidecar is configurable but default-on because terminology search, clinical note
mapping, and benchmark retrieval depend on term-level projections.

## Main Ops

- `snomed_list_releases`: list licensed JSON files staged in the configured local folder.
- `snomed_inspect_release`: stream-count and inspect a release file.
- `snomed_diff_release`: compare official JSON against the canonical MongoDB collection.
- `snomed_ingest_release`: normalize and upsert canonical concept documents.
- `snomed_rebuild_sidecar`: regenerate term documents from canonical concepts.
- `snomed_resolve_scope`: resolve an ECL and/or area constraint into a cached concept-id scope.
- `snomed_search`: tiered lexical search over the term sidecar, optionally scoped.
- `snomed_suggest` and `snomed_concept_history`: authoring and release navigation.
- `snomed_hybrid_search`: lexical, vector, or fused retrieval (`mode`).
- `snomed_ground_note`: candidate grounding for extracted clinical mentions.
- `snomed_nlp_map`: map a whole note to candidates with context cues and an evidence graph.
- `snomed_llm_ground`: guarded adjudication over candidates retrieval already returned.
- `snomed_save_grounding_review` and `snomed_query_grounded_corpus`: governed review and concept/ancestor queries.
- `snomed_list_gold_notes`, `snomed_put_gold_note`, `snomed_delete_gold_note`: the benchmark corpus.
- `snomed_benchmark_retrieval`: bounded hit@k and mean-reciprocal-rank evidence, optionally graded against gold notes.
- `snomed_record_usage_event`: product telemetry.

## Optional Adapters

Lexical retrieval needs only MongoDB. Vector retrieval, cross-encoder reranking,
and LLM extraction are bound per environment and degrade gracefully when absent:

- `retrieval.vector_index` empty, or the vector branch unavailable → `mode: hybrid`
  reports `strategy: "lexical-fallback"` with a reason.
- No `rerank` binding → fused results keep reciprocal-rank order and report `reranked: false`.
- No `llm` binding → note mapping uses deterministic extraction and
  `snomed_llm_ground` fails closed with `SNOMED_LLM_UNAVAILABLE`.

LLM extraction returns mentions and their context only. It never returns a
SNOMED CT code, and adjudication may only choose among retrieved candidates.

MongoDB connectivity comes from Kehrnel activation bindings. The customer obtains
licensed SNOMED CT JSON through official channels and places it in `source.local_dir`.
Do not put MongoDB URIs, gateway keys, or SNOMED distribution credentials in the
manifest or defaults.

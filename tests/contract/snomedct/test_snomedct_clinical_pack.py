from types import SimpleNamespace

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.snomedct import clinical
from kehrnel.engine.domains.snomedct.retrieval import (
    build_search_query_variants,
    reciprocal_rank_fusion,
    rerank_navigator_results,
)
from kehrnel.engine.domains.snomedct.scope import SCOPE_CACHE, resolve_scope
from kehrnel.engine.strategies.snomedct.mongodb.strategy import SNOMEDCTMongoDBStrategy

PACK_DIR = "src/kehrnel/engine/strategies/snomedct/mongodb"

SIDECAR_ROWS = [
    {
        "conceptId": "73211009",
        "term": "Diabetes mellitus",
        "displayTerm": "Diabetes mellitus",
        "preferredTerm": "Diabetes mellitus",
        "fsn": "Diabetes mellitus (disorder)",
        "semanticTag": "disorder",
        "languageCode": "en",
        "ancestorIds": ["404684003"],
        "preferred": True,
        "termRank": 54,
    },
    {
        "conceptId": "44054006",
        "term": "Diabetes mellitus type 2",
        "displayTerm": "Diabetes mellitus type 2",
        "preferredTerm": "Diabetes mellitus type 2",
        "fsn": "Diabetes mellitus type 2 (disorder)",
        "semanticTag": "disorder",
        "languageCode": "en",
        "ancestorIds": ["73211009", "404684003"],
        "preferred": True,
        "termRank": 54,
    },
]


class _SidecarStorage:
    """Aggregation double that records the pipeline, hint, and call count."""

    def __init__(self, rows=None):
        self.rows = list(rows if rows is not None else SIDECAR_ROWS)
        self.collection = None
        self.pipeline = None
        self.hint = None
        self.calls = 0

    async def aggregate(self, collection, pipeline, allow_disk_use=True, hint=None):
        self.collection = collection
        self.pipeline = pipeline
        self.hint = hint
        self.calls += 1
        if any("$facet" in stage for stage in pipeline):
            # Answer a batched grounding request by bucketing the same rows.
            facet = next(stage["$facet"] for stage in pipeline if "$facet" in stage)
            return [{name: [dict(row) for row in self.rows] for name in facet}]
        return [dict(row) for row in self.rows]


class _LegacySidecarStorage:
    """Aggregation double without hint support, as an older adapter would be."""

    def __init__(self, rows=None):
        self.rows = list(rows if rows is not None else SIDECAR_ROWS)

    async def aggregate(self, collection, pipeline):
        return [dict(row) for row in self.rows]


class _Collection:
    def __init__(self):
        self.documents = []

    async def insert_one(self, document):
        self.documents.append(dict(document))
        return SimpleNamespace(inserted_id=len(self.documents))

    async def count_documents(self, query):
        return len(
            [
                item
                for item in self.documents
                if all(item.get(key) == value for key, value in (query or {}).items())
            ]
        )

    async def find_one(self, query, projection=None, sort=None):
        for item in self.documents:
            if all(item.get(key) == value for key, value in (query or {}).items()):
                return {key: value for key, value in item.items() if key != "_id"}
        return None

    async def update_one(self, query, update, upsert=False):
        for index, item in enumerate(self.documents):
            if all(item.get(key) == value for key, value in query.items()):
                self.documents[index] = {**item, **(update.get("$set") or {})}
                return SimpleNamespace(modified_count=1, upserted_id=None)
        if upsert:
            self.documents.append({**query, **(update.get("$set") or {})})
        return SimpleNamespace(modified_count=0, upserted_id="new")

    async def delete_one(self, query):
        before = len(self.documents)
        self.documents = [
            item for item in self.documents if not all(item.get(key) == value for key, value in query.items())
        ]
        return SimpleNamespace(deleted_count=before - len(self.documents))

    def find(self, query=None, projection=None):
        query = query or {}
        matches = [
            {key: value for key, value in item.items() if key != "_id"}
            for item in self.documents
            if all(
                item.get(key) in value["$in"] if isinstance(value, dict) and "$in" in value else item.get(key) == value
                for key, value in query.items()
            )
        ]
        return _Cursor(matches)


class _Cursor:
    def __init__(self, rows):
        self.rows = list(rows)

    def sort(self, keys):
        for key, direction in reversed(keys):
            self.rows.sort(key=lambda item: item.get(key) or "", reverse=direction < 0)
        return self

    def limit(self, count):
        self.rows = self.rows[:count]
        return self

    async def to_list(self, length=None):
        return list(self.rows[:length] if length else self.rows)

    def __aiter__(self):
        self._index = 0
        return self

    async def __anext__(self):
        if self._index >= len(self.rows):
            raise StopAsyncIteration
        row = self.rows[self._index]
        self._index += 1
        return row


class _Database:
    def __init__(self, concepts=None):
        self.collections = {
            "snomed_concepts": _Collection(),
            "snomed_gold_notes": _Collection(),
            "snomed_usage_events": _Collection(),
        }
        for concept in concepts or []:
            self.collections["snomed_concepts"].documents.append(concept)

    def __getitem__(self, name):
        return self.collections.setdefault(name, _Collection())


def _context(manifest, *, adapters=None):
    return StrategyContext(
        environment_id="test",
        config=manifest.default_config,
        manifest=manifest,
        adapters=adapters or {},
    )


# ---------------------------------------------------------------------------
# Extraction and context
# ---------------------------------------------------------------------------


def test_clinical_extraction_reads_negation_family_and_plan_context():
    note = (
        "Assessment: type 2 diabetes mellitus.\n"
        "Family history: mother with type 2 diabetes mellitus.\n"
        "Plan: continue metformin.\n"
        "No evidence of diabetic nephropathy."
    )
    extracted = clinical.extract_candidate_phrases(note, max_phrases=20)
    by_phrase = {entry["phrase"].lower(): entry for entry in extracted["phrases"]}

    assert any("diabetes" in phrase for phrase in by_phrase)
    assert {entry["contextType"] for entry in extracted["phrases"]} >= {"current", "family-history", "negated", "plan"}

    family_entries = [entry for entry in extracted["phrases"] if entry["contextType"] == "family-history"]
    assert family_entries and all(entry["experiencer"] == "family" for entry in family_entries)

    negated = [entry for entry in extracted["phrases"] if entry["contextType"] == "negated"]
    assert negated and all(entry["assertion"] == "absent" for entry in negated)


def test_clinical_review_outcome_accepts_exact_match_and_abstains_without_candidates():
    entry = {
        "phrase": "diabetes mellitus",
        "normalizedPhrase": "diabetes mellitus",
        "tokenCount": 2,
        "contextType": "current",
        "assertion": "present",
        "experiencer": "patient",
        "evidenceSignals": [],
    }
    candidate = {
        **SIDECAR_ROWS[0],
        "matchedText": "Diabetes mellitus",
        "matchTier": 1,
        "score": 100,
    }

    accepted = clinical.build_review_outcome(entry, [candidate])
    abstained = clinical.build_review_outcome(entry, [])

    assert accepted["status"] == "accepted"
    assert accepted["evidenceType"] == "direct"
    assert "exact-clinical-match" in accepted["decisionReasons"]
    assert abstained["status"] == "abstain"
    assert abstained["topCandidate"] is None


def test_clinical_review_outcome_penalises_unattributed_laterality():
    entry = {
        "phrase": "fracture",
        "normalizedPhrase": "fracture",
        "tokenCount": 1,
        "contextType": "current",
        "assertion": "present",
        "experiencer": "patient",
        "evidenceSignals": [],
    }
    candidate = {
        "conceptId": "1",
        "term": "Fracture of left femur",
        "matchedText": "Fracture of left femur",
        "matchTier": 4,
        "score": 80,
        "semanticTag": "disorder",
        "termRank": 30,
        "conceptActive": True,
    }

    outcome = clinical.build_review_outcome(entry, [candidate])

    assert outcome["status"] != "accepted"
    assert "unattributed-laterality" in outcome["decisionReasons"]


def test_clinical_principal_diagnosis_prefers_present_patient_findings():
    entries = [
        {
            "phrase": "diabetes mellitus",
            "section": "Assessment",
            "assertion": "present",
            "experiencer": "patient",
            "evidenceType": "direct",
            "status": "accepted",
            "confidencePct": 92,
            "topCandidate": {"conceptId": "73211009", "term": "Diabetes mellitus", "semanticTag": "disorder", "active": True},
        },
        {
            "phrase": "asthma",
            "section": "Family history",
            "assertion": "family-history",
            "experiencer": "family",
            "evidenceType": "inferred",
            "status": "review",
            "confidencePct": 80,
            "topCandidate": {"conceptId": "187687003", "term": "Asthma", "semanticTag": "disorder", "active": True},
        },
    ]

    candidates = clinical.compute_primary_diagnosis_candidates(entries, target_context="Condition.code", limit=3)

    assert candidates[0]["conceptId"] == "73211009"
    assert all(candidate["conceptId"] != "187687003" for candidate in candidates)


def test_clinical_evidence_graph_links_mentions_to_their_cues():
    text = "No evidence of diabetic nephropathy."
    extracted = clinical.extract_candidate_phrases(text, max_phrases=5)
    reviewed = [
        clinical.build_review_outcome(
            {**entry, "matches": [], "status": "review"},
            [],
        )
        for entry in extracted["phrases"]
    ]

    graph = clinical.build_evidence_graph(
        document_id="doc-1",
        language_code="en",
        release_id="20260601",
        sections=extracted["sections"],
        results=reviewed,
    )

    assert graph["mentions"]
    assert graph["cueObjects"]
    mention = graph["mentions"][0]
    assert mention["derivedContextType"] == "negated"
    assert mention["appliedCueIds"]


# ---------------------------------------------------------------------------
# Retrieval helpers
# ---------------------------------------------------------------------------


def test_search_query_variants_cover_common_misspellings():
    variants = build_search_query_variants("diabetis mellitus")

    assert "diabetis mellitus" in variants
    assert "diabetes mellitus" in variants


def test_rerank_prefers_exact_term_over_contains_match():
    rows = [
        {"conceptId": "1", "term": "Diabetes mellitus type 2", "termRank": 10},
        {"conceptId": "2", "term": "Diabetes mellitus", "termRank": 10},
    ]

    ranked = rerank_navigator_results(query="diabetes mellitus", results=rows, limit=5)

    assert [row["conceptId"] for row in ranked] == ["2", "1"]
    assert ranked[0]["matchTier"] < ranked[1]["matchTier"]


def test_reciprocal_rank_fusion_promotes_concepts_found_by_both_engines():
    lexical = [{"conceptId": "a"}, {"conceptId": "b"}]
    vector = [{"conceptId": "b"}, {"conceptId": "c"}]

    fused = reciprocal_rank_fusion(lexical=lexical, vector=vector, k=60)

    assert fused[0]["conceptId"] == "b"
    assert set(fused[0]["foundBy"]) == {"lexical", "vector"}
    assert fused[0]["rrfScore"] > fused[1]["rrfScore"]


# ---------------------------------------------------------------------------
# Semantic scope
# ---------------------------------------------------------------------------


class _ScopeStorage:
    def __init__(self, rows):
        self.rows = rows

    async def aggregate(self, collection, pipeline):
        return list(self.rows)


@pytest.mark.asyncio
async def test_semantic_scope_caches_area_expansion_and_reports_timings():
    SCOPE_CACHE._entries.clear()  # noqa: SLF001 - test isolation for a process-wide cache
    rows = [{"conceptId": "73211009"}, {"conceptId": "44054006"}]

    async def aggregate(collection, pipeline):
        return rows

    first = await resolve_scope(
        cfg={"search": {}},
        env_id="scope-test",
        release_id="20260601",
        expression=None,
        area_concept_id="404684003",
        concepts_collection="snomed_concepts",
        compile_ecl=lambda cfg, query: {},
        aggregate=aggregate,
    )
    second = await resolve_scope(
        cfg={"search": {}},
        env_id="scope-test",
        release_id="20260601",
        expression=None,
        area_concept_id="404684003",
        concepts_collection="snomed_concepts",
        compile_ecl=lambda cfg, query: {},
        aggregate=aggregate,
    )

    assert first["conceptIdScope"] == ["44054006", "73211009"]
    assert first["scope"]["mode"] == "area"
    assert first["scope"]["cache"]["areaClauseHit"] is False
    assert second["scope"]["cache"]["areaClauseHit"] is True
    assert second["scope"]["cache"]["warm"] is True


@pytest.mark.asyncio
async def test_semantic_scope_rejects_a_star_expression():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = _context(manifest, adapters={"storage": _ScopeStorage([])})

    with pytest.raises(KehrnelError) as excinfo:
        await strategy.snomed_resolve_scope(ctx, {"ecl": "*"})

    assert excinfo.value.code == "INVALID_INPUT"


# ---------------------------------------------------------------------------
# Note mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nlp_map_grounds_deterministic_mentions_and_reports_stats():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    config = {**manifest.default_config, "clinical": {"llm_extraction": False}}
    ctx = StrategyContext(
        environment_id="test",
        config=config,
        manifest=manifest,
        adapters={"storage": _SidecarStorage()},
    )

    result = await strategy.snomed_nlp_map(
        ctx,
        {"text": "Assessment: diabetes mellitus.", "language": "en", "extractor": "deterministic"},
    )

    assert result["extractor"] == "deterministic"
    assert result["retrieved"] is True
    assert result["results"]
    assert result["evidenceGraph"]["mentions"]
    assert result["stats"]["candidatePhrases"] >= 1
    assert result["principalDiagnosisCandidates"]


@pytest.mark.asyncio
async def test_nlp_map_extract_only_skips_retrieval():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    storage = _SidecarStorage()
    config = {**manifest.default_config, "clinical": {"llm_extraction": False}}
    ctx = StrategyContext(
        environment_id="test",
        config=config,
        manifest=manifest,
        adapters={"storage": storage},
    )

    result = await strategy.snomed_nlp_map(
        ctx,
        {"text": "Assessment: diabetes mellitus.", "language": "en", "extractor": "deterministic", "extract_only": True},
    )

    assert result["extractOnly"] is True
    assert result["retrieved"] is False
    assert storage.pipeline is None
    assert all(entry["status"] == "abstain" for entry in result["results"])


@pytest.mark.asyncio
async def test_nlp_map_falls_back_to_deterministic_when_no_llm_is_bound():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = StrategyContext(
        environment_id="test",
        config=manifest.default_config,
        manifest=manifest,
        adapters={"storage": _SidecarStorage()},
    )

    result = await strategy.snomed_nlp_map(ctx, {"text": "Assessment: diabetes mellitus.", "language": "en"})

    assert result["extractor"] == "deterministic"
    assert result["extractorError"] is None


# ---------------------------------------------------------------------------
# LLM adjudication
# ---------------------------------------------------------------------------


class _FakeLlm:
    def __init__(self, payload):
        self.payload = payload
        self.prompts = []

    async def complete(self, prompt, system=None):
        self.prompts.append(prompt)
        return {"text": self.payload, "model": "test-model", "usage": {"inputTokens": 1, "outputTokens": 1, "totalTokens": 2}}


@pytest.mark.asyncio
async def test_llm_ground_drops_concept_ids_the_model_invented():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    llm = _FakeLlm(
        '{"summary":"ok","primaryConceptId":"99999999","decisions":['
        '{"mentionId":"m1","conceptId":"73211009","confidence":0.9,"rationale":"exact"},'
        '{"mentionId":"m1","conceptId":"88888888","confidence":0.9,"rationale":"invented"}]}'
    )
    ctx = _context(manifest, adapters={"llm": llm})

    result = await strategy.snomed_llm_ground(
        ctx,
        {
            "language": "en",
            "mentions": [
                {
                    "mentionId": "m1",
                    "phrase": "diabetes mellitus",
                    "candidates": [{"conceptId": "73211009", "term": "Diabetes mellitus"}],
                }
            ],
        },
    )

    assert [decision["conceptId"] for decision in result["decisions"]] == ["73211009"]
    assert result["primaryConceptId"] == "none"


@pytest.mark.asyncio
async def test_llm_ground_reports_unavailable_without_an_adapter():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = _context(manifest)

    with pytest.raises(KehrnelError) as excinfo:
        await strategy.snomed_llm_ground(ctx, {"mentions": [{"mentionId": "m1"}]})

    assert excinfo.value.code == "SNOMED_LLM_UNAVAILABLE"


# ---------------------------------------------------------------------------
# Gold notes and usage events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gold_notes_round_trip_and_grade_a_benchmark():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = _context(manifest, adapters={"storage": SimpleNamespace(db=_Database())})

    saved = await strategy.snomed_put_gold_note(
        ctx,
        {
            "id": "gold-1",
            "title": "Diabetes",
            "languageCode": "en",
            "text": "Patient has diabetes mellitus",
            "facts": [{"phrase": "diabetes mellitus", "conceptId": "73211009", "assertion": "present", "subject": "patient"}],
        },
    )
    listed = await strategy.snomed_list_gold_notes(ctx, {})

    assert saved["saved"]["id"] == "gold-1"
    assert [note["id"] for note in listed["goldNotes"]] == ["gold-1"]

    async def fake_search(_ctx, _payload):
        return {"matches": [{"conceptId": "73211009"}]}

    strategy.snomed_search = fake_search  # type: ignore[assignment]
    benchmark = await strategy.snomed_benchmark_retrieval(ctx, {"gold_note_ids": ["gold-1"]})

    assert benchmark["summary"]["cases"] == 1
    assert benchmark["summary"]["hitRate"] == 1.0

    removed = await strategy.snomed_delete_gold_note(ctx, {"id": "gold-1"})
    assert removed["removed"] is True


@pytest.mark.asyncio
async def test_gold_note_requires_a_fact():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = _context(manifest, adapters={"storage": SimpleNamespace(db=_Database())})

    with pytest.raises(KehrnelError) as excinfo:
        await strategy.snomed_put_gold_note(ctx, {"id": "x", "text": "note", "facts": []})

    assert excinfo.value.code == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_usage_event_is_hashed_and_recorded():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    db = _Database()
    ctx = _context(manifest, adapters={"storage": SimpleNamespace(db=db)})

    result = await strategy.snomed_record_usage_event(
        ctx,
        {"event_type": "explorer.search.submitted", "query": "Diabetes Mellitus", "concept_id": "73211009"},
    )

    assert result["recorded"] is True
    stored = db["snomed_usage_events"].documents[0]
    assert stored["queryPreview"] == "Diabetes Mellitus"
    assert stored["queryHash"] and "Diabetes Mellitus" != stored["queryHash"]
    assert stored["conceptIds"] == ["73211009"]


@pytest.mark.asyncio
async def test_usage_event_telemetry_failure_does_not_break_the_request():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = _context(manifest)

    result = await strategy.snomed_record_usage_event(ctx, {"event_type": "explorer.search.submitted"})

    assert result["recorded"] is True


# ---------------------------------------------------------------------------
# Retrieval modes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hybrid_search_falls_back_to_lexical_without_a_vector_index():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = _context(manifest, adapters={"storage": _SidecarStorage()})

    result = await strategy.snomed_hybrid_search(ctx, {"q": "diabetes mellitus", "mode": "hybrid", "language": "en"})

    assert result["mode"] == "hybrid"
    assert result["strategy"] == "lexical-fallback"
    assert result["matches"]
    assert result["explain"]["vector"]["enabled"] is False


@pytest.mark.asyncio
async def test_hybrid_search_rejects_an_unknown_mode():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = _context(manifest, adapters={"storage": _SidecarStorage()})

    with pytest.raises(KehrnelError) as excinfo:
        await strategy.snomed_hybrid_search(ctx, {"q": "diabetes", "mode": "quantum"})

    assert excinfo.value.code == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_lexical_hybrid_search_keeps_the_lookahead_row_for_has_more():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    storage = _SidecarStorage()
    ctx = _context(manifest, adapters={"storage": storage})

    result = await strategy.snomed_hybrid_search(ctx, {"q": "diabetes", "mode": "lexical", "language": "en", "limit": 1})

    assert {"$limit": 2} in storage.pipeline
    assert result["page"]["limit"] == 1
    assert result["page"]["returned"] == 1
    assert result["page"]["hasMore"] is True
    assert result["page"]["nextOffset"] == 1


@pytest.mark.asyncio
async def test_search_reranks_a_pool_wider_than_the_page():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    storage = _SidecarStorage()
    ctx = _context(manifest, adapters={"storage": storage})

    result = await strategy.snomed_search(ctx, {"q": "diabetes mellitus", "language": "en", "limit": 2})

    assert result["matches"][0]["conceptId"] == "73211009"
    assert result["matches"][0]["matchTier"] < result["matches"][1]["matchTier"]
    assert result["explain"]["page"]["limit"] == 2


@pytest.mark.asyncio
async def test_search_hints_the_index_that_can_evaluate_the_regex():
    """The lexical predicate must use the index whose last key is normalizedTerm.

    Without the hint the planner can pick term_semantic_lookup, whose keys do not
    include normalizedTerm, and then every document in the release/language range
    is fetched and filtered. Measured at demo scale: 6,784 vs 75,000 documents.
    """
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    storage = _SidecarStorage()
    ctx = _context(manifest, adapters={"storage": storage})

    await strategy.snomed_search(ctx, {"q": "diabetes mellitus", "language": "en", "limit": 5})

    assert storage.hint == "term_release_language"


@pytest.mark.asyncio
async def test_hybrid_search_hint_is_dropped_when_a_better_index_exists():
    """semanticTagKey and areaTags have dedicated indexes; hinting past them is worse."""
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    storage = _SidecarStorage()
    ctx = _context(manifest, adapters={"storage": storage})

    await strategy.snomed_hybrid_search(
        ctx, {"q": "diabetes", "language": "en", "semantic_tag_key": "disorder", "limit": 5}
    )
    assert storage.hint is None

    storage.calls = 0
    await strategy.snomed_hybrid_search(ctx, {"q": "diabetes", "language": "en", "limit": 5})
    assert storage.hint == "term_release_language"


@pytest.mark.asyncio
async def test_adapters_without_hint_support_still_work():
    """The hint is a performance hint; an adapter that cannot take one must not break."""
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = _context(manifest, adapters={"storage": _LegacySidecarStorage()})

    result = await strategy.snomed_search(ctx, {"q": "diabetes mellitus", "language": "en", "limit": 5})

    assert result["matches"]


@pytest.mark.asyncio
async def test_a_missing_hinted_index_falls_back_to_the_unhinted_plan():
    """A deployment that has not created the index yet must not fail the query."""
    from kehrnel.engine.strategies.snomedct.mongodb.strategy import _is_missing_hint_index_error

    class _MissingIndex(Exception):
        code = 2
        details = "hint provided does not correspond to an existing index"

    assert _is_missing_hint_index_error(_MissingIndex())
    assert not _is_missing_hint_index_error(ValueError("something else"))

    class _FallingBackStorage(_SidecarStorage):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        async def aggregate(self, collection, pipeline, allow_disk_use=True, hint=None):
            self.attempts += 1
            if hint:
                raise _MissingIndex()
            return await super().aggregate(collection, pipeline, allow_disk_use, hint)

    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    storage = _FallingBackStorage()
    ctx = _context(manifest, adapters={"storage": storage})

    result = await strategy.snomed_search(ctx, {"q": "diabetes mellitus", "language": "en", "limit": 5})

    assert storage.attempts == 2
    assert result["matches"]


@pytest.mark.asyncio
async def test_note_mapping_grounds_every_phrase_in_one_batched_scan():
    """N phrases must cost one aggregation, not N."""
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    storage = _SidecarStorage()
    config = {**manifest.default_config, "clinical": {"llm_extraction": False}}
    ctx = StrategyContext(
        environment_id="test",
        config=config,
        manifest=manifest,
        adapters={"storage": storage},
    )

    result = await strategy.snomed_nlp_map(
        ctx,
        {
            "text": "Assessment: diabetes mellitus. Plan: continue diabetes mellitus type 2.",
            "language": "en",
            "extractor": "deterministic",
        },
    )

    assert storage.calls == 1
    assert storage.hint == "term_release_language"
    assert any(entry.get("strategy") == "tiered-lexical-batched" for entry in result["results"])
    assert all(entry["span"]["text"] for entry in result["results"])


@pytest.mark.asyncio
async def test_note_mapping_falls_back_to_per_phrase_search_when_batching_fails():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)

    class _FacetRejectingStorage(_SidecarStorage):
        async def aggregate(self, collection, pipeline, allow_disk_use=True, hint=None):
            if any("$facet" in stage for stage in pipeline):
                raise RuntimeError("$facet exceeded memory limit")
            return await super().aggregate(collection, pipeline, allow_disk_use, hint)

    storage = _FacetRejectingStorage()
    config = {**manifest.default_config, "clinical": {"llm_extraction": False}}
    ctx = StrategyContext(
        environment_id="test",
        config=config,
        manifest=manifest,
        adapters={"storage": storage},
    )

    result = await strategy.snomed_nlp_map(
        ctx,
        {
            "text": "Assessment: diabetes mellitus. Plan: continue diabetes mellitus type 2.",
            "language": "en",
            "extractor": "deterministic",
        },
    )

    assert result["results"]
    assert all(entry.get("strategy") in {"tiered-lexical", "skipped-anchor"} for entry in result["results"])


@pytest.mark.asyncio
async def test_readiness_reports_retrieval_and_gold_note_state():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    db = _Database()
    ctx = _context(manifest, adapters={"storage": SimpleNamespace(db=db)})

    result = await strategy.snomed_readiness(ctx, {})

    assert result["retrieval"]["lexical"]["strategy"] == "normalized_regex"
    assert result["retrieval"]["vector"]["configured"] is False
    assert result["retrieval"]["rerank"]["configured"] is False
    assert result["goldNotes"]["collection"] == "snomed_gold_notes"
    assert result["features"]["gold_notes"] is True

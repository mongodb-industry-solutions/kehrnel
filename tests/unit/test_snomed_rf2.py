"""Unit tests for the RF2 -> derived concept JSON converter.

All fixtures here are SYNTHETIC. Concept ids are invented placeholders in a
private test range and no real SNOMED CT release content is reproduced. Only the
structural metadata identifiers (IS-A, FSN/synonym type ids, acceptability ids)
appear, because those define the RF2 file format itself.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from kehrnel.engine.domains.snomedct.model import normalize_concept
from kehrnel.engine.domains.snomedct.rf2 import (
    ACCEPTABILITY_ACCEPTABLE_ID,
    ACCEPTABILITY_PREFERRED_ID,
    CHARACTERISTIC_INFERRED_ID,
    DEFINITION_STATUS_PRIMITIVE_ID,
    IS_A_TYPE_ID,
    TYPE_FSN_ID,
    TYPE_SYNONYM_ID,
    HierarchyIndex,
    Rf2Error,
    build_hierarchy_index,
    compute_transitive_ancestors,
    convert_rf2_to_json,
)
from kehrnel.engine.domains.snomedct.stream_json import iter_json_objects_from_top_array

# --- synthetic constants -------------------------------------------------- #

MODULE = "900000000000207008"  # structural module identifier
CASE_INSENSITIVE = "900000000000448009"
LANG_REFSET = "900000000000509007"
STATED_CHARACTERISTIC = "900000000000010007"
ADDITIONAL_CHARACTERISTIC = "900000000000227009"
MODIFIER = "900000000000451002"
EFFECTIVE = "20240101"

# An invented non-IS-A attribute type id (not a real SNOMED attribute).
ATTR_TYPE = "700000000000001001"

CONCEPT_HEADER = "id\teffectiveTime\tactive\tmoduleId\tdefinitionStatusId"
DESCRIPTION_HEADER = (
    "id\teffectiveTime\tactive\tmoduleId\tconceptId\tlanguageCode\ttypeId\tterm\tcaseSignificanceId"
)
RELATIONSHIP_HEADER = (
    "id\teffectiveTime\tactive\tmoduleId\tsourceId\tdestinationId\t"
    "relationshipGroup\ttypeId\tcharacteristicTypeId\tmodifierId"
)
LANGUAGE_HEADER = (
    "id\teffectiveTime\tactive\tmoduleId\trefsetId\treferencedComponentId\tacceptabilityId"
)


# --- fixture builders ------------------------------------------------------ #


def concept_row(concept_id: str, *, active: str = "1", definition_status: str = DEFINITION_STATUS_PRIMITIVE_ID) -> str:
    return "\t".join([concept_id, EFFECTIVE, active, MODULE, definition_status])


def description_row(
    description_id: str,
    concept_id: str,
    term: str,
    *,
    type_id: str = TYPE_SYNONYM_ID,
    active: str = "1",
    language: str = "en",
) -> str:
    return "\t".join(
        [description_id, EFFECTIVE, active, MODULE, concept_id, language, type_id, term, CASE_INSENSITIVE]
    )


def relationship_row(
    relationship_id: str,
    source_id: str,
    destination_id: str,
    *,
    type_id: str = IS_A_TYPE_ID,
    active: str = "1",
    characteristic: str = CHARACTERISTIC_INFERRED_ID,
    group: str = "0",
) -> str:
    return "\t".join(
        [
            relationship_id,
            EFFECTIVE,
            active,
            MODULE,
            source_id,
            destination_id,
            group,
            type_id,
            characteristic,
            MODIFIER,
        ]
    )


def language_row(
    member_id: str,
    description_id: str,
    acceptability_id: str,
    *,
    active: str = "1",
    refset_id: str = LANG_REFSET,
) -> str:
    return "\t".join([member_id, EFFECTIVE, active, MODULE, refset_id, description_id, acceptability_id])


def write_release(
    root: Path,
    *,
    concepts: list[str],
    descriptions: list[str] | None = None,
    relationships: list[str] | None = None,
    languages: list[str] | None = None,
) -> Path:
    """Write a synthetic RF2 Snapshot tree and return its directory."""
    terminology = root / "Snapshot" / "Terminology"
    refset = root / "Snapshot" / "Refset" / "Language"
    terminology.mkdir(parents=True, exist_ok=True)
    refset.mkdir(parents=True, exist_ok=True)

    (terminology / "sct2_Concept_Snapshot_TEST_20240101.txt").write_text(
        "\n".join([CONCEPT_HEADER, *concepts]) + "\n", encoding="utf-8"
    )
    (terminology / "sct2_Description_Snapshot-en_TEST_20240101.txt").write_text(
        "\n".join([DESCRIPTION_HEADER, *(descriptions or [])]) + "\n", encoding="utf-8"
    )
    (terminology / "sct2_Relationship_Snapshot_TEST_20240101.txt").write_text(
        "\n".join([RELATIONSHIP_HEADER, *(relationships or [])]) + "\n", encoding="utf-8"
    )
    (refset / "der2_cRefset_LanguageSnapshot-en_TEST_20240101.txt").write_text(
        "\n".join([LANGUAGE_HEADER, *(languages or [])]) + "\n", encoding="utf-8"
    )
    return root


def convert(root: Path, out: Path, **kwargs) -> dict[str, dict]:
    """Run the converter and return the emitted concepts keyed by conceptId."""
    report = convert_rf2_to_json(root, out, **kwargs)
    assert report["ok"] is True
    concepts = {item["conceptId"]: item for item in iter_json_objects_from_top_array(out)}
    # The file must also parse as a plain top-level JSON array.
    parsed = json.loads(out.read_text(encoding="utf-8"))
    assert isinstance(parsed, list)
    assert len(parsed) == len(concepts)
    return concepts


# --- 1. transitive closure over a deep chain ------------------------------- #


def test_transitive_closure_is_complete_over_deep_chain(tmp_path: Path):
    """A >= 4-level chain: every concept must list ALL ancestors above it.

    This is the test that protects ECL `<<X` expansion, which matches a flat
    `{"inferredAncestorIds": X}` -- a partial closure silently under-returns.
    """
    chain = ["100000001", "100000002", "100000003", "100000004", "100000005", "100000006"]
    concepts = [concept_row(cid) for cid in chain]
    relationships = [
        relationship_row(f"2{index:08d}", child, parent)
        for index, (child, parent) in enumerate(zip(chain[1:], chain[:-1]), start=1)
    ]
    root = write_release(tmp_path / "rel", concepts=concepts, relationships=relationships)

    emitted = convert(root, tmp_path / "out.json")

    assert set(emitted) == set(chain)
    # Depth check: the chain is 6 deep, so the leaf has 5 ancestors.
    assert emitted[chain[0]]["inferredAncestorIds"] == []
    for depth, concept_id in enumerate(chain):
        expected_ancestors = set(chain[:depth])
        assert set(emitted[concept_id]["inferredAncestorIds"]) == expected_ancestors, (
            f"{concept_id} at depth {depth} has an incomplete closure"
        )
    assert len(emitted[chain[-1]]["inferredAncestorIds"]) == 5

    # Direct edges stay direct.
    assert emitted[chain[-1]]["inferredParentIds"] == [chain[-2]]
    assert emitted[chain[0]]["inferredChildIds"] == [chain[1]]
    assert emitted[chain[-1]]["inferredChildIds"] == []

    # Simulate the Mongo `<<` match: root must subsume the whole chain.
    descendants = {cid for cid, doc in emitted.items() if chain[0] in doc["inferredAncestorIds"]}
    assert descendants == set(chain[1:])


# --- 2. diamond / multi-parent union --------------------------------------- #


def test_diamond_concept_gets_union_of_both_branches(tmp_path: Path):
    """A concept with two parents inherits the UNION of both ancestor branches."""
    root_id = "110000001"
    left_top, left_mid = "110000002", "110000003"
    right_top, right_mid = "110000004", "110000005"
    diamond = "110000006"
    deep = "110000007"

    concepts = [concept_row(c) for c in (root_id, left_top, left_mid, right_top, right_mid, diamond, deep)]
    relationships = [
        relationship_row("210000001", left_top, root_id),
        relationship_row("210000002", right_top, root_id),
        relationship_row("210000003", left_mid, left_top),
        relationship_row("210000004", right_mid, right_top),
        # Two independent parents -> the diamond.
        relationship_row("210000005", diamond, left_mid),
        relationship_row("210000006", diamond, right_mid),
        relationship_row("210000007", deep, diamond),
    ]
    release = write_release(tmp_path / "rel", concepts=concepts, relationships=relationships)

    emitted = convert(release, tmp_path / "out.json")

    assert sorted(emitted[diamond]["inferredParentIds"]) == sorted([left_mid, right_mid])
    assert set(emitted[diamond]["inferredAncestorIds"]) == {
        left_mid,
        left_top,
        right_mid,
        right_top,
        root_id,
    }
    # And the union propagates further down.
    assert set(emitted[deep]["inferredAncestorIds"]) == {
        diamond,
        left_mid,
        left_top,
        right_mid,
        right_top,
        root_id,
    }
    # No duplicates in the closure.
    ancestors = emitted[deep]["inferredAncestorIds"]
    assert len(ancestors) == len(set(ancestors))


# --- 3. inactive rows excluded --------------------------------------------- #


def test_inactive_rows_are_excluded(tmp_path: Path):
    """Inactive concept, description and relationship rows must all be dropped."""
    active_parent = "120000001"
    active_child = "120000002"
    retired_concept = "120000003"
    grandchild = "120000004"

    concepts = [
        concept_row(active_parent),
        concept_row(active_child),
        concept_row(retired_concept, active="0"),
        concept_row(grandchild),
    ]
    descriptions = [
        description_row("320000001", active_child, "active term"),
        description_row("320000002", active_child, "retired term", active="0"),
    ]
    relationships = [
        relationship_row("220000001", active_child, active_parent),
        # Inactive IS-A: must not create a hierarchy edge or appear in relationships.
        relationship_row("220000002", grandchild, active_child, active="0"),
    ]
    release = write_release(
        tmp_path / "rel", concepts=concepts, descriptions=descriptions, relationships=relationships
    )

    emitted = convert(release, tmp_path / "out.json")

    # Inactive concept is gone entirely.
    assert retired_concept not in emitted
    assert set(emitted) == {active_parent, active_child, grandchild}

    # Inactive description dropped; active one kept.
    terms = [d["term"] for d in emitted[active_child]["descriptions"]]
    assert terms == ["active term"]

    # Inactive relationship dropped from both the hierarchy and the row list.
    assert emitted[grandchild]["inferredParentIds"] == []
    assert emitted[grandchild]["inferredAncestorIds"] == []
    assert emitted[grandchild]["relationships"] == []
    assert emitted[active_child]["inferredChildIds"] == []
    assert emitted[active_child]["inferredParentIds"] == [active_parent]


def test_include_inactive_concepts_flag_emits_retired_rows(tmp_path: Path):
    """Opt-in flag emits inactive concepts, still flagged active=False."""
    release = write_release(
        tmp_path / "rel",
        concepts=[concept_row("120100001"), concept_row("120100002", active="0")],
    )
    emitted = convert(release, tmp_path / "out.json", include_inactive_concepts=True)
    assert set(emitted) == {"120100001", "120100002"}
    assert emitted["120100002"]["active"] is False


# --- 4. non-IS-A relationships ---------------------------------------------- #


def test_non_isa_relationships_do_not_create_hierarchy_edges(tmp_path: Path):
    """Attribute relationships appear in `relationships` but never in the hierarchy."""
    parent = "130000001"
    child = "130000002"
    target = "130000003"

    concepts = [concept_row(c) for c in (parent, child, target)]
    relationships = [
        relationship_row("230000001", child, parent),
        # Non-IS-A attribute relationship.
        relationship_row("230000002", child, target, type_id=ATTR_TYPE),
        # IS-A but STATED, not inferred -> must not enter the inferred hierarchy.
        relationship_row("230000003", child, target, characteristic=STATED_CHARACTERISTIC),
        # IS-A but 'additional' characteristic -> also excluded.
        relationship_row("230000004", child, target, characteristic=ADDITIONAL_CHARACTERISTIC),
    ]
    release = write_release(tmp_path / "rel", concepts=concepts, relationships=relationships)

    emitted = convert(release, tmp_path / "out.json")

    child_doc = emitted[child]
    # Hierarchy only reflects the single active inferred IS-A.
    assert child_doc["inferredParentIds"] == [parent]
    assert child_doc["inferredAncestorIds"] == [parent]
    assert target not in child_doc["inferredAncestorIds"]
    assert emitted[target]["inferredChildIds"] == []

    # ...but all four active rows survive in `relationships`.
    rel_ids = sorted(r["relationshipId"] for r in child_doc["relationships"])
    assert rel_ids == ["230000001", "230000002", "230000003", "230000004"]
    attribute = next(r for r in child_doc["relationships"] if r["relationshipId"] == "230000002")
    assert attribute["typeId"] == ATTR_TYPE
    assert attribute["destinationId"] == target
    assert attribute["characteristicTypeId"] == CHARACTERISTIC_INFERRED_ID
    assert attribute["modifierId"] == MODIFIER
    assert attribute["active"] is True
    assert attribute["effectiveTime"] == EFFECTIVE
    assert attribute["moduleId"] == MODULE


# --- 5. acceptabilityMap attachment ----------------------------------------- #


def test_acceptability_map_attaches_to_the_correct_description(tmp_path: Path):
    """Language refset rows land on the description whose id == referencedComponentId."""
    concept_id = "140000001"
    other_id = "140000002"
    second_refset = "900000000000508004"

    concepts = [concept_row(concept_id), concept_row(other_id)]
    descriptions = [
        description_row("340000001", concept_id, "Preferred thing (test)", type_id=TYPE_FSN_ID),
        description_row("340000002", concept_id, "preferred thing"),
        description_row("340000003", concept_id, "acceptable thing"),
        description_row("340000004", other_id, "unrelated thing"),
    ]
    languages = [
        language_row("440000001", "340000001", ACCEPTABILITY_PREFERRED_ID),
        language_row("440000002", "340000002", ACCEPTABILITY_PREFERRED_ID),
        language_row("440000003", "340000003", ACCEPTABILITY_ACCEPTABLE_ID),
        # Same description, a second language refset -> both keys present.
        language_row("440000004", "340000002", ACCEPTABILITY_ACCEPTABLE_ID, refset_id=second_refset),
        # Inactive refset member -> ignored.
        language_row("440000005", "340000004", ACCEPTABILITY_PREFERRED_ID, active="0"),
        # Dangling reference to a description that does not exist -> ignored.
        language_row("440000006", "349999999", ACCEPTABILITY_PREFERRED_ID),
    ]
    release = write_release(
        tmp_path / "rel",
        concepts=concepts,
        descriptions=descriptions,
        relationships=[],
        languages=languages,
    )

    emitted = convert(release, tmp_path / "out.json")
    by_id = {d["descriptionId"]: d for d in emitted[concept_id]["descriptions"]}

    assert by_id["340000001"]["acceptabilityMap"] == {LANG_REFSET: ACCEPTABILITY_PREFERRED_ID}
    assert by_id["340000002"]["acceptabilityMap"] == {
        LANG_REFSET: ACCEPTABILITY_PREFERRED_ID,
        second_refset: ACCEPTABILITY_ACCEPTABLE_ID,
    }
    assert by_id["340000003"]["acceptabilityMap"] == {LANG_REFSET: ACCEPTABILITY_ACCEPTABLE_ID}

    # The inactive member must not have attached to the other concept's description.
    other_desc = emitted[other_id]["descriptions"][0]
    assert other_desc["descriptionId"] == "340000004"
    assert other_desc["acceptabilityMap"] == {}


# --- 6. FSN vs synonym + normalize_concept round-trip ----------------------- #


def test_fsn_synonym_mapping_round_trips_through_normalize_concept(tmp_path: Path):
    """Type ids map correctly and the output survives `normalize_concept` intact."""
    parent = "150000001"
    concept_id = "150000002"

    concepts = [concept_row(parent), concept_row(concept_id)]
    descriptions = [
        description_row("350000001", concept_id, "Synthetic finding (finding)", type_id=TYPE_FSN_ID),
        description_row("350000002", concept_id, "Synthetic finding", type_id=TYPE_SYNONYM_ID),
        description_row("350000003", parent, "Synthetic root (finding)", type_id=TYPE_FSN_ID),
    ]
    relationships = [
        relationship_row("250000001", concept_id, parent),
        relationship_row("250000002", concept_id, parent, type_id=ATTR_TYPE),
    ]
    languages = [
        language_row("450000001", "350000001", ACCEPTABILITY_PREFERRED_ID),
        language_row("450000002", "350000002", ACCEPTABILITY_PREFERRED_ID),
    ]
    release = write_release(
        tmp_path / "rel",
        concepts=concepts,
        descriptions=descriptions,
        relationships=relationships,
        languages=languages,
    )

    emitted = convert(release, tmp_path / "out.json")
    doc = emitted[concept_id]

    types = {d["descriptionId"]: d["typeId"] for d in doc["descriptions"]}
    assert types["350000001"] == TYPE_FSN_ID
    assert types["350000002"] == TYPE_SYNONYM_ID
    for description in doc["descriptions"]:
        assert description["languageCode"] == "en"
        assert description["conceptId"] == concept_id
        assert description["active"] is True

    # --- round-trip through the canonical normalizer, without loss ---
    normalized = normalize_concept(doc, release_id="20240101", release_label="synthetic-test")

    assert normalized["conceptId"] == concept_id
    assert normalized["active"] is True
    assert normalized["effectiveTime"] == EFFECTIVE
    assert normalized["moduleId"] == MODULE
    assert normalized["definitionStatusId"] == DEFINITION_STATUS_PRIMITIVE_ID
    # The hierarchy arrays must survive stringify_list unchanged.
    assert normalized["inferredParentIds"] == doc["inferredParentIds"] == [parent]
    assert normalized["inferredAncestorIds"] == doc["inferredAncestorIds"] == [parent]
    assert normalized["inferredChildIds"] == doc["inferredChildIds"] == []
    assert normalized["memberOfRefsetIds"] == []
    assert normalized["concreteRelationships"] == []
    assert len(normalized["descriptions"]) == 2
    assert len(normalized["relationships"]) == 2

    norm_types = {d["descriptionId"]: d["typeId"] for d in normalized["descriptions"]}
    assert norm_types == types
    norm_terms = {d["descriptionId"]: d["term"] for d in normalized["descriptions"]}
    assert norm_terms == {d["descriptionId"]: d["term"] for d in doc["descriptions"]}
    norm_accept = {d["descriptionId"]: d["acceptabilityMap"] for d in normalized["descriptions"]}
    assert norm_accept["350000001"] == {LANG_REFSET: ACCEPTABILITY_PREFERRED_ID}
    assert norm_accept["350000002"] == {LANG_REFSET: ACCEPTABILITY_PREFERRED_ID}

    # normalize_concept derives attribute keys from the emitted relationships.
    assert f"{ATTR_TYPE}|{parent}" in normalized["relationshipAttributeKeys"]
    assert f"{IS_A_TYPE_ID}|{parent}" in normalized["relationshipAttributeKeys"]
    assert normalized["releaseId"] == "20240101"
    assert normalized["releaseLabel"] == "synthetic-test"


# --- 7. cycle safety --------------------------------------------------------- #


def test_cyclic_isa_graph_terminates(tmp_path: Path):
    """A deliberately cyclic IS-A graph must terminate, not hang or recurse forever."""
    root_id = "160000001"
    a, b, c = "160000002", "160000003", "160000004"
    leaf = "160000005"

    concepts = [concept_row(x) for x in (root_id, a, b, c, leaf)]
    relationships = [
        relationship_row("260000001", a, root_id),
        # 3-node cycle: a -> b -> c -> a
        relationship_row("260000002", b, a),
        relationship_row("260000003", c, b),
        relationship_row("260000004", a, c),
        relationship_row("260000005", leaf, c),
        # Self-loop must also be tolerated.
        relationship_row("260000006", leaf, leaf),
    ]
    release = write_release(tmp_path / "rel", concepts=concepts, relationships=relationships)

    emitted = convert(release, tmp_path / "out.json")

    # Terminated, and every cycle member sees the whole cycle plus the root.
    for member in (a, b, c):
        ancestors = set(emitted[member]["inferredAncestorIds"])
        assert root_id in ancestors
        assert {a, b, c} - {member} <= ancestors
        # A concept is never its own ancestor.
        assert member not in ancestors
    leaf_ancestors = set(emitted[leaf]["inferredAncestorIds"])
    assert {a, b, c, root_id} <= leaf_ancestors
    assert leaf not in leaf_ancestors


def test_compute_transitive_ancestors_handles_pure_cycle():
    """The closure helper alone terminates on a cycle with no entry point."""
    index = HierarchyIndex()
    index.add_edge("1", "2")
    index.add_edge("2", "3")
    index.add_edge("3", "1")

    ancestors = compute_transitive_ancestors(index)

    for node in ("1", "2", "3"):
        assert ancestors[node] == {"1", "2", "3"} - {node}


def test_compute_transitive_ancestors_deep_chain_is_not_recursive():
    """A 5000-deep chain must not blow the Python recursion limit."""
    index = HierarchyIndex()
    depth = 5000
    for i in range(depth - 1):
        index.add_edge(str(i + 1), str(i))

    ancestors = compute_transitive_ancestors(index)

    assert len(ancestors[str(depth - 1)]) == depth - 1
    assert ancestors[str(1)] == {"0"}


def test_build_hierarchy_index_filters_rows():
    """Only active + inferred + IS-A rows become edges."""
    rows = [
        {"sourceId": "2", "destinationId": "1", "typeId": IS_A_TYPE_ID, "characteristicTypeId": CHARACTERISTIC_INFERRED_ID, "active": "1"},
        {"sourceId": "3", "destinationId": "1", "typeId": IS_A_TYPE_ID, "characteristicTypeId": CHARACTERISTIC_INFERRED_ID, "active": "0"},
        {"sourceId": "4", "destinationId": "1", "typeId": ATTR_TYPE, "characteristicTypeId": CHARACTERISTIC_INFERRED_ID, "active": "1"},
        {"sourceId": "5", "destinationId": "1", "typeId": IS_A_TYPE_ID, "characteristicTypeId": STATED_CHARACTERISTIC, "active": "1"},
    ]
    index = build_hierarchy_index(rows)
    assert index.children_of("1") == {"2"}
    assert index.parents_of("2") == {"1"}
    assert index.parents_of("3") == set()
    assert index.parents_of("4") == set()
    assert index.parents_of("5") == set()


# --- 8. zip input ------------------------------------------------------------ #


def test_zip_input_is_supported(tmp_path: Path):
    """A release .zip is read directly, with files located by glob inside it."""
    parent = "170000001"
    child = "170000002"
    grandchild = "170000003"

    archive = tmp_path / "SnomedCT_SyntheticRelease.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        base = "SnomedCT_SyntheticRelease/Snapshot"
        zf.writestr(
            f"{base}/Terminology/sct2_Concept_Snapshot_TEST_20240101.txt",
            "\n".join([CONCEPT_HEADER, concept_row(parent), concept_row(child), concept_row(grandchild)]) + "\n",
        )
        zf.writestr(
            f"{base}/Terminology/sct2_Description_Snapshot-en_TEST_20240101.txt",
            "\n".join(
                [
                    DESCRIPTION_HEADER,
                    description_row("370000001", child, "Zipped concept (test)", type_id=TYPE_FSN_ID),
                ]
            )
            + "\n",
        )
        zf.writestr(
            f"{base}/Terminology/sct2_Relationship_Snapshot_TEST_20240101.txt",
            "\n".join(
                [
                    RELATIONSHIP_HEADER,
                    relationship_row("270000001", child, parent),
                    relationship_row("270000002", grandchild, child),
                ]
            )
            + "\n",
        )
        zf.writestr(
            f"{base}/Refset/Language/der2_cRefset_LanguageSnapshot-en_TEST_20240101.txt",
            "\n".join([LANGUAGE_HEADER, language_row("470000001", "370000001", ACCEPTABILITY_PREFERRED_ID)]) + "\n",
        )

    emitted = convert(archive, tmp_path / "out.json")

    assert set(emitted) == {parent, child, grandchild}
    assert set(emitted[grandchild]["inferredAncestorIds"]) == {child, parent}
    description = emitted[child]["descriptions"][0]
    assert description["term"] == "Zipped concept (test)"
    assert description["typeId"] == TYPE_FSN_ID
    assert description["acceptabilityMap"] == {LANG_REFSET: ACCEPTABILITY_PREFERRED_ID}


# --- misc hardening ---------------------------------------------------------- #


def test_columns_resolved_by_name_not_position(tmp_path: Path):
    """Reordered (but correctly named) header columns still convert correctly."""
    root = tmp_path / "rel"
    terminology = root / "Snapshot" / "Terminology"
    terminology.mkdir(parents=True)
    (terminology / "sct2_Concept_Snapshot_TEST_20240101.txt").write_text(
        "definitionStatusId\tactive\tid\tmoduleId\teffectiveTime\n"
        f"{DEFINITION_STATUS_PRIMITIVE_ID}\t1\t180000001\t{MODULE}\t{EFFECTIVE}\n",
        encoding="utf-8",
    )
    (terminology / "sct2_Description_Snapshot_TEST_20240101.txt").write_text(
        "term\tid\tconceptId\tactive\ttypeId\tlanguageCode\tmoduleId\teffectiveTime\tcaseSignificanceId\n"
        f"Reordered term\t380000001\t180000001\t1\t{TYPE_FSN_ID}\ten\t{MODULE}\t{EFFECTIVE}\t{CASE_INSENSITIVE}\n",
        encoding="utf-8",
    )
    (terminology / "sct2_Relationship_Snapshot_TEST_20240101.txt").write_text(
        RELATIONSHIP_HEADER + "\n", encoding="utf-8"
    )

    emitted = convert(root, tmp_path / "out.json")

    doc = emitted["180000001"]
    assert doc["moduleId"] == MODULE
    assert doc["effectiveTime"] == EFFECTIVE
    assert doc["definitionStatusId"] == DEFINITION_STATUS_PRIMITIVE_ID
    assert doc["descriptions"][0]["term"] == "Reordered term"
    assert doc["descriptions"][0]["typeId"] == TYPE_FSN_ID


def test_missing_required_file_raises(tmp_path: Path):
    """A source without a Concept Snapshot file is rejected with a clear error."""
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(Rf2Error, match="Concept"):
        convert_rf2_to_json(empty, tmp_path / "out.json")


def test_output_contract_shape_and_streamability(tmp_path: Path):
    """Every emitted object carries exactly the keys the ingest contract expects."""
    release = write_release(
        tmp_path / "rel",
        concepts=[concept_row("190000001"), concept_row("190000002")],
        descriptions=[description_row("390000001", "190000002", "Shape check (test)", type_id=TYPE_FSN_ID)],
        relationships=[relationship_row("290000001", "190000002", "190000001")],
        languages=[language_row("490000001", "390000001", ACCEPTABILITY_PREFERRED_ID)],
    )
    out = tmp_path / "out.json"
    report = convert_rf2_to_json(release, out)

    assert report["stats"]["conceptsWritten"] == 2
    assert report["stats"]["isaEdges"] == 1
    assert report["stats"]["acceptabilityEntries"] == 1

    expected_keys = {
        "conceptId",
        "active",
        "effectiveTime",
        "moduleId",
        "definitionStatusId",
        "descriptions",
        "relationships",
        "concreteRelationships",
        "inferredParentIds",
        "inferredAncestorIds",
        "inferredChildIds",
        "memberOfRefsetIds",
    }
    streamed = list(iter_json_objects_from_top_array(out))
    assert len(streamed) == 2
    for concept in streamed:
        assert set(concept) == expected_keys
        assert isinstance(concept["conceptId"], str)
        assert isinstance(concept["effectiveTime"], str)
        assert concept["concreteRelationships"] == []
        assert concept["memberOfRefsetIds"] == []
        for description in concept["descriptions"]:
            assert set(description) == {
                "descriptionId",
                "conceptId",
                "active",
                "effectiveTime",
                "moduleId",
                "typeId",
                "languageCode",
                "term",
                "caseSignificanceId",
                "acceptabilityMap",
            }
        for relationship in concept["relationships"]:
            assert set(relationship) == {
                "relationshipId",
                "sourceId",
                "destinationId",
                "typeId",
                "moduleId",
                "characteristicTypeId",
                "modifierId",
                "active",
                "effectiveTime",
            }


def test_compact_output_is_still_streamable(tmp_path: Path):
    """`--compact` output must remain readable by the brace-depth scanner."""
    release = write_release(
        tmp_path / "rel",
        concepts=[concept_row("191000001"), concept_row("191000002")],
        relationships=[relationship_row("291000001", "191000002", "191000001")],
    )
    out = tmp_path / "compact.json"
    convert_rf2_to_json(release, out, indent=None)

    text = out.read_text(encoding="utf-8")
    assert "\n  {" not in text
    streamed = [c["conceptId"] for c in iter_json_objects_from_top_array(out)]
    assert streamed == ["191000001", "191000002"]


def test_cli_converts_release_and_exits_zero(tmp_path: Path):
    """The CLI entry point succeeds with exit code 0 on both output modes."""
    CliRunner = pytest.importorskip(
        "typer.testing",
        reason="typer is an optional CLI dependency; the converter library itself needs no typer.",
    ).CliRunner

    import kehrnel.cli.snomed_rf2 as cli

    runner = CliRunner()
    release = write_release(
        tmp_path / "rel",
        concepts=[concept_row("200000001"), concept_row("200000002")],
        descriptions=[description_row("400000001", "200000002", "CLI concept (test)", type_id=TYPE_FSN_ID)],
        relationships=[relationship_row("500000001", "200000002", "200000001")],
        languages=[language_row("600000001", "400000001", ACCEPTABILITY_PREFERRED_ID)],
    )

    out = tmp_path / "cli.json"
    result = runner.invoke(cli.app, [str(release), "--out", str(out), "--json"])
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report["ok"] is True
    assert report["stats"]["conceptsWritten"] == 2
    emitted = {c["conceptId"]: c for c in iter_json_objects_from_top_array(out)}
    assert emitted["200000002"]["inferredAncestorIds"] == ["200000001"]

    # Default (table) rendering must also report success.
    pretty_out = tmp_path / "cli_pretty.json"
    pretty = runner.invoke(cli.app, [str(release), "--out", str(pretty_out)])
    assert pretty.exit_code == 0, pretty.output
    assert pretty_out.exists()


def test_cli_reports_missing_files_as_error(tmp_path: Path):
    """A source with no RF2 files exits non-zero with a readable message."""
    CliRunner = pytest.importorskip(
        "typer.testing",
        reason="typer is an optional CLI dependency; the converter library itself needs no typer.",
    ).CliRunner

    import kehrnel.cli.snomed_rf2 as cli

    empty = tmp_path / "empty"
    empty.mkdir()
    result = CliRunner().invoke(cli.app, [str(empty), "--out", str(tmp_path / "x.json")])
    assert result.exit_code == 2
    rendered = (result.output or "") + (str(result.exception) if result.exception else "")
    assert "Concept" in rendered


def test_empty_release_emits_empty_array(tmp_path: Path):
    """A release with no concept rows still produces a valid empty JSON array."""
    release = write_release(tmp_path / "rel", concepts=[])
    out = tmp_path / "out.json"
    convert_rf2_to_json(release, out)
    assert json.loads(out.read_text(encoding="utf-8")) == []
    assert list(iter_json_objects_from_top_array(out)) == []

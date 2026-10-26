from pathlib import Path

import pytest

from kehrnel.engine.domains.openehr.templates import (
    OptSemanticCatalogCompiler,
    OptSemanticCatalogError,
    compile_opt_semantic_catalog,
    compile_opt_semantic_catalog_xml,
)


SAMPLE_LAB_OPT = Path(
    "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference/"
    "templates/sample_laboratory_v0_4.opt"
)
TUMOUR_SUMMARY_OPT = Path("samples/templates/T-IGR-TUMOUR-SUMMARY.opt")
PMSI_OPT = Path("samples/templates/T-IGR-PMSI-EXTRACT.opt")
HADS_OPT = Path("samples/templates/hads_v0.opt")
ALL_BUNDLED_OPTS = sorted(
    [
        *Path("samples/templates").glob("*.opt"),
        *Path(
            "src/kehrnel/engine/strategies/openehr/rps_dual/samples/"
            "reference/templates"
        ).glob("*.opt"),
    ]
)


def _field(catalog, archetype_id: str, node_id: str):
    matches = [
        field
        for field in catalog.fields
        if field.containing_archetype.archetype_id == archetype_id
        and field.node_id == node_id
    ]
    assert len(matches) == 1
    return matches[0]


def test_catalog_extracts_archetype_scoped_field_paths_and_value_types():
    catalog = compile_opt_semantic_catalog(SAMPLE_LAB_OPT)

    assert catalog.contract == "openehr-opt-semantic-catalog/v1"
    assert catalog.template.template_id == "sample_laboratory_v0.4"
    assert catalog.template.root_archetype_id == "openEHR-EHR-COMPOSITION.report-result.v1"
    assert catalog.verify_digest()

    result = _field(
        catalog,
        "openEHR-EHR-CLUSTER.laboratory_test_analyte.v1",
        "at0001",
    )
    assert result.label == "Test result"
    assert result.description.startswith("The value of the analyte result")
    assert result.archetype_relative_aql_path == "/items[at0001]"
    assert result.composition_aql_path == (
        "/content[openEHR-EHR-SECTION.diagnostic_reports.v0]"
        "/items[openEHR-EHR-OBSERVATION.laboratory_test_result.v1]"
        "/data[at0001]/events[at0002]/data[at0003]"
        "/items[openEHR-EHR-CLUSTER.laboratory_test_analyte.v1]"
        "/items[at0001]"
    )
    assert result.absolute_aql_path == (
        "[openEHR-EHR-COMPOSITION.report-result.v1]"
        + result.composition_aql_path
    )
    assert [variant.rm_type for variant in result.value_variants] == [
        "DV_INTERVAL<DV_QUANTITY>",
        "DV_QUANTITY",
        "DV_TEXT",
    ]
    quantity = next(
        variant for variant in result.value_variants if variant.rm_type == "DV_QUANTITY"
    )
    assert [(path.facet, path.aql_path_suffix) for path in quantity.query_paths] == [
        ("magnitude", "/value/magnitude"),
        ("units", "/value/units"),
    ]
    assert [item.archetype_id for item in result.containment] == [
        "openEHR-EHR-COMPOSITION.report-result.v1",
        "openEHR-EHR-SECTION.diagnostic_reports.v0",
        "openEHR-EHR-OBSERVATION.laboratory_test_result.v1",
        "openEHR-EHR-CLUSTER.laboratory_test_analyte.v1",
    ]
    assert result.correlation_anchor.rm_type == "CLUSTER"
    assert result.correlation_anchor.archetype_id == (
        "openEHR-EHR-CLUSTER.laboratory_test_analyte.v1"
    )
    assert result.correlation_anchor.reason == "nearest_repeating_archetype"


def test_same_node_code_in_distinct_archetypes_is_never_conflated():
    catalog = compile_opt_semantic_catalog(SAMPLE_LAB_OPT)

    author_specialty = _field(
        catalog, "openEHR-EHR-CLUSTER.xds_metadata.v0", "at0001"
    )
    authorisation_date = _field(
        catalog, "openEHR-EHR-CLUSTER.admin_salut.v0", "at0001"
    )
    test_result = _field(
        catalog,
        "openEHR-EHR-CLUSTER.laboratory_test_analyte.v1",
        "at0001",
    )

    assert author_specialty.label == "Author specialty"
    assert authorisation_date.label == "Document authorisation date"
    assert test_result.label == "Test result"
    assert len(
        {
            author_specialty.field_id,
            authorisation_date.field_id,
            test_result.field_id,
        }
    ) == 3
    assert len(
        {
            author_specialty.absolute_aql_path,
            authorisation_date.absolute_aql_path,
            test_result.absolute_aql_path,
        }
    ) == 3


def test_catalog_captures_opt_annotations_and_snomed_term_bindings():
    lab_catalog = compile_opt_semantic_catalog(SAMPLE_LAB_OPT)
    analyte = _field(
        lab_catalog,
        "openEHR-EHR-CLUSTER.laboratory_test_analyte.v1",
        "at0001",
    )
    assert {(item.key, item.value) for item in analyte.annotations} == {
        ("fhir_mapping", "Observation.value[x]"),
        ("hl7v2_mapping", "OBX.2, OBX.5, OBX.6, OBX.7, OBX.8"),
    }
    assert len(analyte.annotations) == 2

    tumour_catalog = compile_opt_semantic_catalog(TUMOUR_SUMMARY_OPT)
    laterality = next(
        field
        for field in tumour_catalog.fields
        if field.node_id == "at0002"
        and any(binding.terminology == "SNOMED-CT" for binding in field.term_bindings)
    )
    assert laterality.label == "Laterality"
    assert [binding.code for binding in laterality.term_bindings] == ["272741003"]
    assert laterality.term_bindings[0].raw_code == "[SNOMED-CT::272741003]"


def test_catalog_includes_localized_archetype_terms():
    catalog = compile_opt_semantic_catalog(HADS_OPT)
    field = _field(
        catalog,
        "openEHR-EHR-CLUSTER.document_metadata.v0",
        "at0001",
    )

    spanish = next(item for item in field.localized_terms if item.language == "es")
    assert spanish.text == "Fecha/hora de autorización"


def test_catalog_is_byte_stable_for_path_and_raw_xml_inputs():
    first = compile_opt_semantic_catalog(SAMPLE_LAB_OPT)
    second = compile_opt_semantic_catalog_xml(SAMPLE_LAB_OPT.read_bytes())

    assert first == second
    assert first.catalog_digest == second.catalog_digest
    assert first.model_dump(mode="json", by_alias=True) == second.model_dump(
        mode="json", by_alias=True
    )


def test_field_identity_is_scoped_to_the_exact_opt_source():
    original_xml = SAMPLE_LAB_OPT.read_text(encoding="utf-8")
    changed_xml = original_xml.replace(
        "The value of the analyte result",
        "The governed value of the analyte result",
        1,
    )
    original = compile_opt_semantic_catalog_xml(original_xml)
    changed = compile_opt_semantic_catalog_xml(changed_xml)

    original_result = _field(
        original,
        "openEHR-EHR-CLUSTER.laboratory_test_analyte.v1",
        "at0001",
    )
    changed_result = _field(
        changed,
        "openEHR-EHR-CLUSTER.laboratory_test_analyte.v1",
        "at0001",
    )

    assert original_result.absolute_aql_path == changed_result.absolute_aql_path
    assert original.template.source_digest != changed.template.source_digest
    assert original_result.field_id != changed_result.field_id
    assert original_result.semantic_key == changed_result.semantic_key


def test_catalog_separates_allowed_code_constraints_from_node_term_bindings():
    lab_catalog = compile_opt_semantic_catalog(SAMPLE_LAB_OPT)
    test_name = next(field for field in lab_catalog.fields if field.label == "Test name")
    constraint = test_name.value_variants[0].coding_constraints[0]
    assert constraint.constraint_kind == "code_list"
    assert constraint.terminology_id == "local_terms"
    assert constraint.allowed_codes == ["LABE"]
    assert test_name.term_bindings == []

    pmsi_catalog = compile_opt_semantic_catalog(PMSI_OPT)
    cim10 = next(field for field in pmsi_catalog.fields if field.label == "CIM-10 Code")
    constraint = cim10.value_variants[0].coding_constraints[0]
    assert constraint.constraint_kind == "value_set"
    assert constraint.value_set_uri == "http://fhir.igr.fr/CodeSystem/CIM-10-FR"
    assert constraint.allowed_codes == []


@pytest.mark.parametrize(
    ("opt_path", "field_label", "expected_options"),
    [
        (
            HADS_OPT,
            "Q1. I feel nervous or 'wound up'",
            [
                (0, "local", "at0116"),
                (1, "local", "at0117"),
                (2, "local", "at0118"),
                (3, "local", "at0119"),
            ],
        ),
        (
            Path("samples/templates/neuroqol_v0.opt"),
            "Q1. Because of my illness, some people avoid me",
            [
                (1, "local", "at1001"),
                (2, "local", "at1002"),
                (3, "local", "at1003"),
                (4, "local", "at1004"),
                (5, "local", "at1005"),
            ],
        ),
        (
            Path("samples/templates/whoqol_v0.opt"),
            "1. How would you rate your quality of life?",
            [
                (1, "local", "at0005"),
                (2, "local", "at0006"),
                (3, "local", "at0007"),
                (4, "local", "at0008"),
                (5, "local", "at0009"),
            ],
        ),
    ],
)
def test_catalog_extracts_governed_ordinal_options(
    opt_path,
    field_label,
    expected_options,
):
    catalog = compile_opt_semantic_catalog(opt_path)
    field = next(item for item in catalog.fields if item.label == field_label)
    variant = next(
        item for item in field.value_variants if item.rm_type == "DV_ORDINAL"
    )

    assert [
        (item.ordinal, item.terminology_id, item.code)
        for item in variant.ordinal_options
    ] == expected_options
    assert ("terminology", "/value/symbol/defining_code/terminology_id/value") in {
        (item.facet, item.aql_path_suffix) for item in variant.query_paths
    }
    wire = variant.to_wire()
    assert wire["ordinalOptions"][0]["terminologyId"] == "local"


@pytest.mark.parametrize(
    ("opt_path", "variant_count", "option_count"),
    [
        (HADS_OPT, 16, 62),
        (Path("samples/templates/neuroqol_v0.opt"), 9, 44),
        (Path("samples/templates/whoqol_v0.opt"), 38, 176),
    ],
)
def test_every_ordinal_option_in_bundled_assessment_opts_is_catalogued(
    opt_path,
    variant_count,
    option_count,
):
    catalog = compile_opt_semantic_catalog(opt_path)
    variants = [
        variant
        for field in catalog.fields
        for variant in field.value_variants
        if variant.rm_type == "DV_ORDINAL"
    ]

    assert len(variants) == variant_count
    assert sum(len(variant.ordinal_options) for variant in variants) == option_count
    assert all(variant.ordinal_options for variant in variants)
    assert all(
        option.terminology_id and option.code
        for variant in variants
        for option in variant.ordinal_options
    )
    assert catalog.verify_digest()


@pytest.mark.parametrize(
    ("original", "replacement", "expected"),
    [
        ("<value>0</value>", "<value>not-an-integer</value>", "DV_ORDINAL value"),
        (
            "<code_string>at0116</code_string>",
            "<code_string></code_string>",
            "Malformed DV_ORDINAL option",
        ),
    ],
)
def test_malformed_ordinal_option_fails_closed(original, replacement, expected):
    xml = HADS_OPT.read_text(encoding="utf-8")
    assert original in xml

    with pytest.raises(OptSemanticCatalogError, match=expected):
        compile_opt_semantic_catalog_xml(xml.replace(original, replacement, 1))


@pytest.mark.parametrize("opt_path", ALL_BUNDLED_OPTS, ids=lambda path: path.name)
def test_every_bundled_opt_compiles_to_a_nonempty_verified_catalog(opt_path):
    catalog = compile_opt_semantic_catalog(opt_path)

    assert catalog.template.template_id
    assert catalog.template.root_archetype_id
    assert catalog.archetypes
    assert catalog.fields
    assert catalog.verify_digest()


def test_compiler_is_reusable_and_wire_contract_uses_stable_camel_case_keys():
    compiler = OptSemanticCatalogCompiler.from_path(SAMPLE_LAB_OPT)
    first = compiler.compile()
    second = compiler.compile()

    assert first == second
    wire = first.to_wire()
    assert wire["contract"] == "openehr-opt-semantic-catalog/v1"
    assert wire["catalogDigest"].startswith("sha256:")
    assert wire["template"]["rootArchetypeId"] == (
        "openEHR-EHR-COMPOSITION.report-result.v1"
    )
    field = wire["fields"][0]
    assert field["semanticKey"].startswith("openehr-semantic-key:sha256:")
    assert "containingArchetype" in field
    assert "absoluteAqlPath" in field
    assert "compositionAqlPath" in field
    assert "archetypeRelativeAqlPath" in field
    assert "valueVariants" in field
    assert "correlationAnchor" in field


def test_repeated_same_archetype_is_disambiguated_by_explicit_name_constraint():
    catalog = compile_opt_semantic_catalog(Path("samples/templates/neuroqol_v0.opt"))
    metadata_scopes = [
        item
        for item in catalog.archetypes
        if item.archetype_id == "openEHR-EHR-CLUSTER.document_metadata.v0"
    ]

    assert len(metadata_scopes) == 2
    assert len({item.scope_id for item in metadata_scopes}) == 2
    assert any(
        "name/value='Document metadata'" in item.absolute_aql_path
        for item in metadata_scopes
    )
    assert any(
        "name/value='Document metadata (2)'" in item.absolute_aql_path
        for item in metadata_scopes
    )


@pytest.mark.parametrize(
    "xml, expected",
    [
        (b"", "empty"),
        (b"<not-template />", "root element"),
        (b"<template", "Invalid OPT XML"),
    ],
)
def test_invalid_opt_input_fails_closed(xml: bytes, expected: str):
    with pytest.raises(OptSemanticCatalogError, match=expected):
        compile_opt_semantic_catalog_xml(xml)


def test_invalid_opt_cardinality_lexemes_fail_closed():
    xml = SAMPLE_LAB_OPT.read_text(encoding="utf-8")
    invalid_boolean = xml.replace(
        "<upper_unbounded>false</upper_unbounded>",
        "<upper_unbounded>sometimes</upper_unbounded>",
        1,
    )
    with pytest.raises(OptSemanticCatalogError, match="Invalid OPT boolean"):
        compile_opt_semantic_catalog_xml(invalid_boolean)

    invalid_integer = xml.replace("<lower>1</lower>", "<lower>many</lower>", 1)
    with pytest.raises(OptSemanticCatalogError, match="Invalid OPT integer"):
        compile_opt_semantic_catalog_xml(invalid_integer)


def test_opt_parser_rejects_document_type_and_entity_declarations():
    xml = b'<!DOCTYPE template [<!ENTITY x "unsafe">]><template>&x;</template>'

    with pytest.raises(OptSemanticCatalogError, match="entity declarations"):
        compile_opt_semantic_catalog_xml(xml)


def test_opt_parser_caps_total_xml_elements(monkeypatch):
    import kehrnel.engine.domains.openehr.templates.semantic_catalog as catalog_module

    monkeypatch.setattr(catalog_module, "MAX_OPT_XML_ELEMENTS", 2)
    xml = (
        '<template xmlns="http://schemas.openehr.org/v1">'
        "<template_id><value>bounded</value></template_id>"
        "</template>"
    )

    with pytest.raises(OptSemanticCatalogError, match="element XML traversal limit"):
        compile_opt_semantic_catalog_xml(xml)


def test_opt_parser_caps_xml_depth(monkeypatch):
    import kehrnel.engine.domains.openehr.templates.semantic_catalog as catalog_module

    monkeypatch.setattr(catalog_module, "MAX_OPT_XML_DEPTH", 3)
    xml = (
        '<template xmlns="http://schemas.openehr.org/v1">'
        "<level1><level2><level3 /></level2></level1>"
        "</template>"
    )

    with pytest.raises(OptSemanticCatalogError, match="level XML depth limit"):
        compile_opt_semantic_catalog_xml(xml)


def test_semantic_catalog_caps_emitted_fields(monkeypatch):
    import kehrnel.engine.domains.openehr.templates.semantic_catalog as catalog_module

    monkeypatch.setattr(catalog_module, "MAX_CATALOG_FIELDS", 0)

    with pytest.raises(OptSemanticCatalogError, match="field semantic catalog limit"):
        compile_opt_semantic_catalog(SAMPLE_LAB_OPT)


def test_invalid_opt_interval_is_normalized_to_catalog_error():
    xml = SAMPLE_LAB_OPT.read_text(encoding="utf-8")
    impossible_interval = xml.replace("<lower>1</lower>", "<lower>2</lower>", 1)

    with pytest.raises(OptSemanticCatalogError, match="semantic constraints are invalid"):
        compile_opt_semantic_catalog_xml(impossible_interval)

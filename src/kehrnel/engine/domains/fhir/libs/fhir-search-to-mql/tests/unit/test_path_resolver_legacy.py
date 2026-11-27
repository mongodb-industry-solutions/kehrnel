"""Legacy R4 / R5 CodeableReference path expansion for denormalization."""

from fhir_search_to_mql.denormalizer.path_resolver import (
    expand_legacy_fhir_paths,
    resolve_path,
)


def test_expand_concept_arm_adds_flat_codeable_concept_branch():
    expr = "serviceType[*].concept.coding[*].code"
    expanded = expand_legacy_fhir_paths(expr)
    assert "serviceType[*].coding[*].code" in expanded


def test_expand_nested_reference_branch():
    expr = "code.reference.reference"
    expanded = expand_legacy_fhir_paths(expr)
    assert "code.reference" in expanded


def test_resolve_r4_service_type_shape():
    resource = {
        "resourceType": "Slot",
        "serviceType": [
            {
                "coding": [
                    {"system": "http://terminology.hl7.org/CodeSystem/service-type", "code": "533"}
                ]
            }
        ],
    }
    codes = resolve_path(resource, "serviceType[*].concept.coding[*].code")
    assert codes == ["533"]


def test_resolve_r5_service_type_shape():
    resource = {
        "resourceType": "Slot",
        "serviceType": [
            {
                "concept": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "11429006"}]
                }
            }
        ],
    }
    codes = resolve_path(resource, "serviceType[*].concept.coding[*].code")
    assert codes == ["11429006"]


def test_resolve_servicerequest_code_r4_and_r5():
    r4 = {"code": {"coding": [{"code": "71388002"}]}}
    r5 = {"code": {"concept": {"coding": [{"code": "71388002"}]}}}
    path = "code.concept.coding[*].code"
    assert resolve_path(r4, path) == ["71388002"]
    assert resolve_path(r5, path) == ["71388002"]

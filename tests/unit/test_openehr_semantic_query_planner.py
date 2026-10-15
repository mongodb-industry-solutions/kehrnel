from pathlib import Path

import pytest
from pydantic import ValidationError

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.openehr.semantic_query import (
    OpenEhrSemanticQuery,
    OpenEhrSemanticQueryPlan,
    OpenEhrSemanticQueryPlanner,
    SemanticQueryPlanningError,
    TerminologyExpansionReceipt,
    create_terminology_expansion_receipt,
)
from kehrnel.engine.domains.openehr.templates import compile_opt_semantic_catalog
from kehrnel.engine.strategies.openehr.rps_dual.strategy import (
    DEFAULTS_PATH,
    MANIFEST,
    RPSDualStrategy,
    load_json,
)


SAMPLE_LAB_OPT = Path(
    "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference/"
    "templates/sample_laboratory_v0_4.opt"
)
NEUROQOL_OPT = Path("samples/templates/neuroqol_v0.opt")
TUMOUR_OPT = Path("samples/templates/T-IGR-TUMOUR-SUMMARY.opt")
IMMUNIZATION_OPT = Path(
    "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference/"
    "templates/sample_immunization_list_v0_5.opt"
)


class _FakeCollection:
    def __init__(self, documents):
        self.documents = documents

    async def find_one(self, query=None, projection=None):
        if query and "_id" in query:
            return self.documents.get(query["_id"])
        return next(iter(self.documents.values()), None)


class _FakeDatabase(dict):
    pass


class _FakeStorage:
    def __init__(self, database):
        self.db = database

    async def find_one(self, collection, query):
        return await self.db[collection].find_one(query)


def _semantic_dictionary_storage():
    return _FakeStorage(
        _FakeDatabase(
            {
                "_codes": _FakeCollection(
                    {
                        "ar_code": {
                            "_id": "ar_code",
                            "at": {"at0001": -1, "at0002": -2, "at0003": -3},
                            "openEHR-EHR-COMPOSITION": {
                                "report-result": {"v1": 1}
                            },
                            "openEHR-EHR-EVALUATION": {
                                "problem_diagnosis": {"v1": 4}
                            },
                        }
                    }
                ),
                "_shortcuts": _FakeCollection(
                    {
                        "shortcuts": {
                            "_id": "shortcuts",
                            "items": {
                                "archetype_details": "ad",
                                "code_string": "cs",
                                "data": "d",
                                "defining_code": "df",
                                "items": "i",
                                "template_id": "ti",
                                "terminology_id": "tm",
                                "value": "v",
                            },
                        }
                    }
                ),
            }
        )
    )


@pytest.fixture(scope="module")
def catalog():
    return compile_opt_semantic_catalog(SAMPLE_LAB_OPT)


def _field(catalog, label: str):
    matches = [field for field in catalog.fields if field.label == label]
    assert len(matches) == 1
    return matches[0]


def _template_ref(catalog):
    return {
        "templateId": catalog.template.template_id,
        "sourceDigest": catalog.template.source_digest,
        "catalogDigest": catalog.catalog_digest,
    }


def _heart_failure_query(catalog, *, correlation_group="problem-instance"):
    diagnosis = _field(catalog, "Diagnosis")
    onset = _field(catalog, "Date/time clinically recognised")
    expansion = create_terminology_expansion_receipt(
        expansion_id="snomed-heart-failure-20260131",
        provider_id="tenant-snomed",
        system_uri="http://snomed.info/sct",
        terminology_id="SNOMED-CT",
        version="20260131",
        expression="<< 84114007",
        codes=["84114007", "195111005"],
    )
    return {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "select": ["ehr_id", "composition_uid"],
        "predicates": [
            {
                "semanticUnitId": diagnosis.field_id,
                "facet": "code",
                "operator": "matches",
                "terminologyExpansionRef": expansion.expansion_id,
                "correlationGroup": correlation_group,
            },
            {
                "semanticUnitId": onset.field_id,
                "facet": "value",
                "operator": "gte",
                "value": "2025-01-01",
                "correlationGroup": correlation_group,
            },
        ],
        "terminologyExpansions": [expansion.to_wire()],
        "limit": 50,
    }


def test_planner_generates_archetype_scoped_aql_from_opt_semantics(catalog):
    plan = OpenEhrSemanticQueryPlanner(catalog).plan(_heart_failure_query(catalog))

    assert plan.verify_digest()
    assert (
        "COMPOSITION c[openEHR-EHR-COMPOSITION.report-result.v1] "
        "CONTAINS EVALUATION a0[openEHR-EHR-EVALUATION.problem_diagnosis.v1]"
        in plan.aql
    )
    assert (
        "a0/data[at0001]/items[at0002]/value/defining_code/code_string "
        "MATCHES {'195111005', '84114007'}"
        in plan.aql
    )
    assert (
        "a0/data[at0001]/items[at0002]/value/defining_code/"
        "terminology_id/value = $terminology0"
        in plan.aql
    )
    assert "a0/data[at0001]/items[at0003]/value/value >= $p1" in plan.aql
    assert (
        "c/archetype_details/template_id/value = $templateId" in plan.aql
    )
    assert plan.parameters == {
        "templateId": "sample_laboratory_v0.4",
        "terminology0": "SNOMED-CT",
        "p1": "2025-01-01",
    }
    assert plan.terminology_expansion_refs == [
        "snomed-heart-failure-20260131"
    ]
    assert plan.terminology_expansion_receipts[0].provider_id == "tenant-snomed"
    assert plan.terminology_expansion_receipts[0].version == "20260131"
    assert plan.execution_request.to_strategy_payload() == {
        "domain": "openEHR",
        "raw_aql": plan.aql,
        "params": plan.parameters,
        "debug": True,
        "require_exact_path_correlation": True,
    }
    assert plan.requested_limit == 50
    assert plan.execution_limit == 51
    assert plan.offset == 0
    assert plan.execution_request.requested_limit == 50
    assert plan.execution_request.execution_limit == 51
    assert plan.execution_request.offset == 0
    assert "ORDER BY c/uid/value ASC LIMIT 51" in plan.aql
    required_projection_paths = {
        path
        for field in plan.resolved_fields
        for path in field.projection_paths
    }
    assert (
        "/content[openEHR-EHR-SECTION.problem_list.v0]/items"
        "[openEHR-EHR-EVALUATION.problem_diagnosis.v1]/archetype_node_id"
        in required_projection_paths
    )
    assert (
        "/content[openEHR-EHR-SECTION.problem_list.v0]/items"
        "[openEHR-EHR-EVALUATION.problem_diagnosis.v1]/data[at0001]/"
        "items[at0002]/value/defining_code/terminology_id/value"
        in required_projection_paths
    )


@pytest.mark.parametrize(
    ("tamper", "message"),
    [
        ("aql", "executionRequest.rawAql must equal plan.aql"),
        ("parameters", "executionRequest.params must equal plan.parameters"),
        (
            "requested_limit",
            "executionRequest.requestedLimit must equal plan.requestedLimit",
        ),
        ("lookahead", "executionLimit must reserve exactly one look-ahead row"),
        ("offset", "executionRequest.offset must equal plan.offset"),
    ],
)
def test_query_plan_rejects_execution_request_drift(catalog, tamper, message):
    plan = OpenEhrSemanticQueryPlanner(catalog).plan(
        _heart_failure_query(catalog)
    )
    payload = plan.model_dump()

    if tamper == "aql":
        payload["execution_request"]["raw_aql"] += " "
    elif tamper == "parameters":
        payload["execution_request"]["params"] = {"templateId": "other"}
    elif tamper == "requested_limit":
        payload["execution_request"]["requested_limit"] = 49
    elif tamper == "lookahead":
        payload["execution_limit"] = 52
        payload["execution_request"]["execution_limit"] = 52
    elif tamper == "offset":
        payload["execution_request"]["offset"] = 1

    with pytest.raises(ValidationError, match=message):
        OpenEhrSemanticQueryPlan.model_validate(payload)


@pytest.mark.asyncio
async def test_generated_aql_compiles_with_the_existing_rps_dual_strategy(catalog):
    semantic_plan = OpenEhrSemanticQueryPlanner(catalog).plan(
        _heart_failure_query(catalog)
    )
    strategy = RPSDualStrategy(MANIFEST)
    context = StrategyContext(
        environment_id="semantic-query-test",
        config=load_json(DEFAULTS_PATH),
        adapters={"storage": _semantic_dictionary_storage()},
    )

    mongo_plan = await strategy.compile_query(
        context,
        "openEHR",
        semantic_plan.execution_request.to_strategy_payload(),
    )

    assert mongo_plan.engine == "text_search_dual"
    assert mongo_plan.plan["scope"] == "cross_patient"
    assert mongo_plan.plan["pipeline"]
    assert "$search" in mongo_plan.plan["pipeline"][0]
    assert mongo_plan.plan["pipeline"][0]["$search"]["sort"] == {
        "comp_id": 1
    }
    assert mongo_plan.plan["pipeline"][-1] == {
        "$limit": semantic_plan.execution_limit
    }
    assert mongo_plan.explain["stage0"] == "$search"
    assert mongo_plan.explain["pagination"]["pageSize"] == (
        semantic_plan.execution_limit
    )
    assert mongo_plan.explain["builder"]["chosen"] == (
        "search_pipeline_builder"
    )
    assert mongo_plan.explain["builder"]["compiler_engine"] == (
        "search_pipeline_builder"
    )
    assert mongo_plan.explain["builder"]["execution_engine"] == (
        "text_search_dual"
    )
    ast = mongo_plan.explain["ast"]
    assert ast["contains"]["contains"]["predicate"]["value"] == (
        "openEHR-EHR-EVALUATION.problem_diagnosis.v1"
    )
    where_text = repr(ast["where"])
    assert "195111005" in where_text
    assert "84114007" in where_text
    assert "2025-01-01" in where_text
    exact_matches = [
        stage["$match"]["$expr"]
        for stage in mongo_plan.plan["pipeline"][1:]
        if "$expr" in stage.get("$match", {})
    ]
    assert exact_matches
    exact_text = repr(exact_matches)
    assert "$$anchor.pi" in exact_text
    assert "$$candidate1.pi" in exact_text


def test_planner_requires_correlation_group_for_repeated_archetype(catalog):
    query = _heart_failure_query(catalog, correlation_group=None)

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "CORRELATION_GROUP_REQUIRED"


def test_planner_fails_closed_on_catalog_drift(catalog):
    query = _heart_failure_query(catalog)
    query["templateRef"]["catalogDigest"] = "sha256:" + "f" * 64

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "TEMPLATE_REF_MISMATCH"
    assert "catalogDigest" in caught.value.details["mismatches"]


def test_planner_rejects_unproven_multi_archetype_query(catalog):
    query = _heart_failure_query(catalog)
    author_specialty = _field(catalog, "Author specialty")
    query["projections"] = [
        {
            "semanticUnitId": author_specialty.field_id,
            "facet": "value",
            "alias": "author_specialty",
        }
    ]

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "MULTI_ARCHETYPE_QUERY_NOT_YET_SUPPORTED"


def test_planner_rejects_one_of_several_same_archetype_placements():
    catalog = compile_opt_semantic_catalog(NEUROQOL_OPT)
    field = next(
        item
        for item in catalog.fields
        if item.label == "Authorisation date/time"
        and "name/value='Document metadata (2)'"
        in item.containing_archetype.absolute_aql_path
    )
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [
            {
                "semanticUnitId": field.field_id,
                "facet": "value",
                "operator": "exists",
            }
        ],
    }

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "AMBIGUOUS_ARCHETYPE_PLACEMENT_NOT_SUPPORTED"
    assert len(caught.value.details["placements"]) == 2


def test_terminology_receipt_detects_tampering():
    receipt = create_terminology_expansion_receipt(
        expansion_id="snomed-heart-failure-20260131",
        provider_id="tenant-snomed",
        system_uri="http://snomed.info/sct",
        terminology_id="SNOMED-CT",
        version="20260131",
        expression="<< 84114007",
        codes=["84114007"],
    ).to_wire()
    receipt["codes"] = ["195111005", "84114007"]

    with pytest.raises(ValidationError, match="digest does not match"):
        TerminologyExpansionReceipt.model_validate(receipt)


def test_planner_reports_missing_expansion_before_opt_value_set_validation():
    catalog = compile_opt_semantic_catalog(TUMOUR_OPT)
    severity = _field(catalog, "Severity")
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [
            {
                "semanticUnitId": severity.field_id,
                "facet": "code",
                "operator": "matches",
                "terminologyExpansionRef": "missing-expansion",
            }
        ],
    }

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "TERMINOLOGY_EXPANSION_NOT_FOUND"


def test_semantic_query_caps_predicate_cardinality(catalog):
    diagnosis = _field(catalog, "Diagnosis")
    predicate = {
        "semanticUnitId": diagnosis.field_id,
        "facet": "display",
        "operator": "eq",
        "value": "Heart failure",
    }
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [predicate] * 65,
    }

    with pytest.raises(ValidationError, match="at most 64"):
        OpenEhrSemanticQuery.model_validate(query)


def test_semantic_query_rejects_unpinned_patient_scope(catalog):
    query = _heart_failure_query(catalog)
    query["scope"] = "patient"

    with pytest.raises(ValidationError, match="patient scope requires ehrId"):
        OpenEhrSemanticQuery.model_validate(query)


def test_planner_fails_closed_for_patient_scope_until_exact_pipeline_is_supported(catalog):
    query = _heart_failure_query(catalog)
    query["scope"] = "patient"
    query["ehrId"] = "ehr-123"

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "PATIENT_SCOPE_NOT_YET_SUPPORTED"


def test_planner_validates_predicate_value_against_opt_datatype(catalog):
    result = _field(catalog, "Test result")
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [
            {
                "semanticUnitId": result.field_id,
                "facet": "magnitude",
                "rmType": "DV_QUANTITY",
                "operator": "gte",
                "value": "not-a-number",
            }
        ],
    }

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "PREDICATE_VALUE_TYPE_MISMATCH"


def test_planner_requires_unit_context_for_quantity_comparison(catalog):
    result = _field(catalog, "Test result")
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [
            {
                "semanticUnitId": result.field_id,
                "facet": "magnitude",
                "rmType": "DV_QUANTITY",
                "operator": "gte",
                "value": 5.0,
            }
        ],
    }

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "QUANTITY_UNIT_CONTEXT_REQUIRED"


def test_direct_code_predicate_requires_and_renders_terminology_id(catalog):
    diagnosis = _field(catalog, "Diagnosis")
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [
            {
                "semanticUnitId": diagnosis.field_id,
                "facet": "code",
                "operator": "eq",
                "value": "84114007",
            }
        ],
    }

    with pytest.raises(ValidationError, match="requires terminologyId"):
        OpenEhrSemanticQuery.model_validate(query)

    query["predicates"][0]["terminologyId"] = "SNOMED-CT"
    plan = OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert "terminology_id/value = $terminology0" in plan.aql
    assert "code_string = $p0" in plan.aql
    assert plan.parameters["terminology0"] == "SNOMED-CT"


@pytest.mark.parametrize("facet", ["code", "terminology"])
def test_coded_facets_reject_partial_like_matching(catalog, facet):
    diagnosis = _field(catalog, "Diagnosis")
    predicate = {
        "semanticUnitId": diagnosis.field_id,
        "facet": facet,
        "operator": "like",
        "value": "8411*",
    }
    if facet == "code":
        predicate["terminologyId"] = "SNOMED-CT"
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [predicate],
    }

    with pytest.raises(ValidationError, match="LIKE is not valid for coded facets"):
        OpenEhrSemanticQuery.model_validate(query)


def test_uri_shaped_openehr_terminology_identifier_is_a_safe_parameter():
    catalog = compile_opt_semantic_catalog(IMMUNIZATION_OPT)
    terminology_id = (
        "terminology://fhir.hl7.org/ValueSet/$expand?url="
        "https://sample.fhir.example.org/ValueSet/"
    )
    name = next(
        field
        for field in catalog.fields
        if field.label == "Name"
        and any(
            constraint.terminology_id == terminology_id
            for variant in field.value_variants
            for constraint in variant.coding_constraints
        )
    )
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [
            {
                "semanticUnitId": name.field_id,
                "facet": "code",
                "operator": "eq",
                "terminologyId": terminology_id,
                "value": "2.16.724.4.9.50.2",
            }
        ],
    }

    # The URI is valid contract data.  Planning this particular bundled field
    # remains intentionally blocked because its containing archetype appears in
    # two OPT placements; that is a separate correlation safeguard.
    semantic_query = OpenEhrSemanticQuery.model_validate(query)
    assert semantic_query.predicates[0].terminology_id == terminology_id

    receipt = create_terminology_expansion_receipt(
        expansion_id="fhir-immunization-name-20261001",
        provider_id="tenant-fhir",
        system_uri="https://sample.fhir.example.org/ValueSet/",
        terminology_id=terminology_id,
        version="20261001",
        expression="2.16.724.4.9.50.2",
        codes=["2.16.724.4.9.50.2"],
    )
    assert receipt.terminology_id == terminology_id


@pytest.mark.parametrize("terminology_id", [" SNOMED-CT", "SNOMED-CT\n"])
def test_terminology_identifier_rejects_ambiguous_or_control_text(
    catalog,
    terminology_id,
):
    diagnosis = _field(catalog, "Diagnosis")
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [
            {
                "semanticUnitId": diagnosis.field_id,
                "facet": "code",
                "operator": "eq",
                "terminologyId": terminology_id,
                "value": "84114007",
            }
        ],
    }

    with pytest.raises(ValidationError, match="terminologyId"):
        OpenEhrSemanticQuery.model_validate(query)


def test_planner_rejects_unrepresented_nested_repetition_correlation(catalog):
    test_name = _field(catalog, "Test name")
    comment = _field(catalog, "Specific test comment")
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [
            {
                "semanticUnitId": test_name.field_id,
                "facet": "display",
                "operator": "eq",
                "value": "Troponin",
                "correlationGroup": "same-event",
            },
            {
                "semanticUnitId": comment.field_id,
                "facet": "value",
                "operator": "like",
                "value": "urgent*",
                "correlationGroup": "same-event",
            },
        ],
    }

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "NESTED_CORRELATION_NOT_YET_SUPPORTED"


def test_planner_rejects_projection_from_an_unbound_repeated_event(catalog):
    test_name = _field(catalog, "Test name")
    comment = _field(catalog, "Specific test comment")
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [
            {
                "semanticUnitId": test_name.field_id,
                "facet": "display",
                "operator": "eq",
                "value": "Troponin",
            }
        ],
        "projections": [
            {
                "semanticUnitId": comment.field_id,
                "facet": "value",
                "alias": "comment",
            }
        ],
    }

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "NESTED_PROJECTION_CORRELATION_NOT_YET_SUPPORTED"


def test_semantic_query_reserves_builtin_projection_aliases(catalog):
    diagnosis = _field(catalog, "Diagnosis")
    query = _heart_failure_query(catalog)
    query["projections"] = [
        {
            "semanticUnitId": diagnosis.field_id,
            "facet": "display",
            "alias": "ehr_id",
        }
    ]

    with pytest.raises(ValidationError, match="reserved result fields"):
        OpenEhrSemanticQuery.model_validate(query)


def test_planner_rejects_malformed_temporal_value(catalog):
    onset = _field(catalog, "Date/time clinically recognised")
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [
            {
                "semanticUnitId": onset.field_id,
                "facet": "value",
                "operator": "gte",
                "value": "not-a-date",
            }
        ],
    }

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "PREDICATE_VALUE_LEXICAL_FORM_INVALID"


def test_planner_rejects_code_and_terminology_split_across_repeated_elements():
    catalog = compile_opt_semantic_catalog(IMMUNIZATION_OPT)
    reason = _field(catalog, "Reason")
    expansion = create_terminology_expansion_receipt(
        expansion_id="snomed-immunization-reason-20260131",
        provider_id="tenant-snomed",
        system_uri="http://snomed.info/sct",
        terminology_id="SNOMED-CT",
        version="20260131",
        expression="<< 171044003",
        codes=["171044003"],
    )
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [
            {
                "semanticUnitId": reason.field_id,
                "facet": "code",
                "operator": "matches",
                "terminologyExpansionRef": expansion.expansion_id,
            }
        ],
        "terminologyExpansions": [expansion.to_wire()],
    }

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "CODE_FIELD_CORRELATION_NOT_YET_SUPPORTED"


def test_planner_rejects_multiple_facets_of_a_repeated_element():
    catalog = compile_opt_semantic_catalog(IMMUNIZATION_OPT)
    reason = _field(catalog, "Reason")
    assert reason.occurrences.repeated
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [
            {
                "semanticUnitId": reason.field_id,
                "facet": "display",
                "operator": "eq",
                "value": "Travel",
                "correlationGroup": "same-reason",
            },
            {
                "semanticUnitId": reason.field_id,
                "facet": "terminology",
                "operator": "eq",
                "value": "SNOMED-CT",
                "correlationGroup": "same-reason",
            },
        ],
    }

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "NESTED_CORRELATION_NOT_YET_SUPPORTED"


@pytest.mark.parametrize(
    ("operator", "value"),
    [("neq", "2025-01-01"), ("not_exists", None)],
)
def test_planner_rejects_negative_operators_until_exact_semantics_exist(
    catalog,
    operator,
    value,
):
    onset = _field(catalog, "Date/time clinically recognised")
    predicate = {
        "semanticUnitId": onset.field_id,
        "facet": "value",
        "operator": operator,
    }
    if value is not None:
        predicate["value"] = value
    query = {
        "scope": "cross_patient",
        "templateRef": _template_ref(catalog),
        "predicates": [predicate],
    }

    with pytest.raises(SemanticQueryPlanningError) as caught:
        OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert caught.value.code == "NEGATIVE_OPERATOR_NOT_YET_SUPPORTED"


def test_planner_enforces_opt_allowed_code_and_terminology_constraints():
    catalog = compile_opt_semantic_catalog(TUMOUR_OPT)
    severity = _field(catalog, "Severity")

    def query_for(terminology_id, code):
        return {
            "scope": "cross_patient",
            "templateRef": _template_ref(catalog),
            "predicates": [
                {
                    "semanticUnitId": severity.field_id,
                    "facet": "code",
                    "operator": "eq",
                    "terminologyId": terminology_id,
                    "value": code,
                }
            ],
        }

    plan = OpenEhrSemanticQueryPlanner(catalog).plan(query_for("local", "at0047"))
    assert "terminology_id/value = $terminology0" in plan.aql

    for terminology_id, code in [("SNOMED-CT", "at0047"), ("local", "invalid")]:
        with pytest.raises(SemanticQueryPlanningError) as caught:
            OpenEhrSemanticQueryPlanner(catalog).plan(
                query_for(terminology_id, code)
            )
        assert caught.value.code == "CODE_CONSTRAINT_MISMATCH"

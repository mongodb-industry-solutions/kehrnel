import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.persistence.artifacts.base import ArtifactLocation
from kehrnel.strategy_sdk import StrategyBindings, StrategyHandle, validate_strategy_entrypoint
from kehrnel.engine.strategies.dicom.tiered_store import headers, model, query, synthetic
from kehrnel.engine.strategies.dicom.tiered_store.common import config, version
from kehrnel.engine.strategies.dicom.tiered_store.strategy import DICOMTieredStoreStrategy


class MemoryArtifacts:
    def __init__(self):
        self.objects = {}
        self.reads = 0

    async def put(self, key, content, *, media_type, metadata=None):
        if key in self.objects:
            assert self.objects[key] == content
        self.objects[key] = content
        return ArtifactLocation(key=key, uri="memory://" + key, provider="memory", size=len(content))

    async def get(self, key):
        self.reads += 1
        return self.objects[key]


@pytest.fixture
def ctx():
    return StrategyContext(environment_id="test", config={}, adapters={"artifact": MemoryArtifacts()})


def linked(ctx):
    cfg = config(ctx)
    example = synthetic.generate({})
    study, series = model.study(ctx, cfg, example["studies"][0])
    instance = model.instance(ctx, cfg, example["instances"][0], series[0])
    return cfg, example, study, series[0], instance


def test_native_pack_conforms_and_promotes_no_research_by_default():
    strategy = DICOMTieredStoreStrategy()
    assert validate_strategy_entrypoint(strategy.manifest).is_conformant
    assert not strategy.manifest.default_config["projections"]["enabled"]
    assert "dicom_query" in {op.name for op in strategy.manifest.ops}


@pytest.mark.asyncio
async def test_sdk_activates_and_generates_the_same_native_strategy(ctx):
    strategy = DICOMTieredStoreStrategy()
    handle = StrategyHandle(strategy.manifest, strategy)
    await handle.activate({}, StrategyBindings(), ctx)
    demo = await handle.dispatch("generate", ctx, {})
    assert demo["synthetic"] is True
    assert len(demo["studies"]) == 2
    assert demo["containsPixels"] is False


def test_patient_identity_is_center_scoped_and_sources_remain_unchanged(ctx):
    cfg, example, study, series, instance = linked(ctx)
    other, _ = model.study(ctx, cfg, example["studies"][1])
    assert study["_kehrnel"]["patientId"] == other["_kehrnel"]["patientId"]
    assert study["_kehrnel"]["patientKey"] != other["_kehrnel"]["patientKey"]
    assert {k: v for k, v in study.items() if k != "_kehrnel"} == example["studies"][0]
    assert {k: v for k, v in instance.items() if k != "_kehrnel"} == example["instances"][0]


def test_series_center_is_not_inherited_from_the_study(ctx):
    cfg = config(ctx)
    raw = synthetic.generate({})["studies"][0]
    raw["se"][0]["ci"] = "DEMO_B"
    study, series = model.study(ctx, cfg, raw)
    assert study["_kehrnel"]["centerId"] == "DEMO_A"
    assert series[0]["_kehrnel"]["centerId"] == "DEMO_B"


def test_scalar_uid_compatibility_preserves_source_and_records_warning(ctx):
    cfg, example, _, series, _ = linked(ctx)
    raw = example["instances"][0]
    raw["header"]["0020000E"]["Value"] = series["_id"]
    result = model.instance(ctx, cfg, raw, series)
    assert result["header"] == raw["header"]
    assert result["_kehrnel"]["normalizationWarnings"] == ["scalar_uid_value"]


@pytest.mark.parametrize("number,expected,missing", [("10", 10, 0), ("  +2 ", 2, 0), ("", None, 1), ("bad", None, 1), ("2147483648", None, 1)])
def test_numeric_instance_order_is_typed_without_rewriting_dicom_is(ctx, number, expected, missing):
    cfg, example, _, series, _ = linked(ctx)
    raw = example["instances"][0]
    raw["header"]["00200013"]["Value"] = [number]
    result = model.instance(ctx, cfg, raw, series)
    assert result["_kehrnel"]["instanceNumber"] == expected
    assert result["_kehrnel"]["orderMissing"] == missing
    assert result["header"]["00200013"]["Value"] == [number]


def test_hierarchy_conflict_is_rejected(ctx):
    cfg, example, _, series, _ = linked(ctx)
    raw = example["instances"][0]
    raw["header"]["0020000D"]["Value"] = ["2.25.999999"]
    with pytest.raises(KehrnelError) as error:
        model.instance(ctx, cfg, raw, series)
    assert error.value.code == "DICOM_HIERARCHY_CONFLICT"


@pytest.mark.asyncio
async def test_bundle_roundtrip_preserves_sequences_private_tags_empty_values_and_offsets(ctx):
    ctx.config = {"headers": {"mode": "bundle"}}
    cfg, example, _, series, _ = linked(ctx)
    raw = example["instances"][0]
    raw["header"]["00110010"] = {"vr": "LO", "Value": ["SYNTHETIC_PRIVATE_CREATOR"]}
    raw["header"]["0040A730"] = {"vr": "SQ", "Value": [{"0040A160": {"vr": "UT", "Value": ["Synthetic finding"]}}]}
    raw["header"]["00100020"] = {"vr": "LO", "Value": []}
    raw["header"]["7FE00010"] = {"vr": "OB", "BulkDataURI": "/synthetic/bulk"}
    raw["ContentIndex"] = {"v": 1, "parts": [{"frames": [{"n": 0, "offset": 123, "length": 64}]}]}
    instance = model.instance(ctx, cfg, raw, series)
    locators = await headers.bundle(ctx, cfg, [instance])
    assert "header" not in locators[0] and "ContentIndex" not in locators[0]
    assert await headers.original(ctx, cfg, locators[0]) == raw
    before = len(ctx.adapters["artifact"].objects)
    assert await headers.bundle(ctx, cfg, [instance]) == locators
    assert len(ctx.adapters["artifact"].objects) == before


@pytest.mark.asyncio
async def test_metadata_tampering_fails_integrity_check(ctx):
    ctx.config = {"headers": {"mode": "bundle"}}
    cfg, _, _, _, instance = linked(ctx)
    locator = (await headers.bundle(ctx, cfg, [instance]))[0]
    key = locator["_kehrnel"]["headerRef"]["key"]
    ctx.adapters["artifact"].objects[key] = b"corrupt"
    with pytest.raises(KehrnelError) as error:
        await headers.original(ctx, cfg, locator)
    assert error.value.code == "DICOM_METADATA_INTEGRITY_FAILED"


def test_config_and_request_scope_cannot_be_expanded(ctx):
    ctx.meta = {"authorized_centers": ["DEMO_A"]}
    with pytest.raises(KehrnelError) as error:
        query.compile_query(ctx, {"version": "dicom-query/v1", "resource": "studies", "centers": ["DEMO_B"]})
    assert error.value.status == 403
    with pytest.raises(KehrnelError):
        query.compile_query(ctx, {"version": "dicom-query/v1", "resource": "studies", "filter": {"patientId": {"$ne": None}}})


def test_study_modality_predicate_has_same_series_center_semantics(ctx):
    ctx.meta = {"authorized_centers": ["DEMO_A"]}
    plan = query.compile_query(ctx, {"version": "dicom-query/v1", "resource": "studies", "filter": {"modality": "MG"}})
    element = plan.plan["pipeline"][0]["$match"]["_kehrnel.seriesFacets"]["$elemMatch"]
    assert element == {"centerId": {"$in": ["DEMO_A"]}, "modality": "MG"}
    assert "_kehrnel.seriesFacets" not in plan.plan["pipeline"][-1]["$project"]


@pytest.mark.parametrize("query_input,code", [
    ({"resource": "instances"}, "DICOM_SERIES_SCOPE_REQUIRED"),
    ({"resource": "studies", "filter": {"condition": "diabetes"}}, "DICOM_CONDITION_EVIDENCE_REQUIRED"),
    ({"resource": "studies", "filter": {"day": "2026-02-30"}}, "DICOM_DATE_INVALID"),
    ({"resource": "studies", "filter": {"fromDay": "2026-02-01", "toDay": "2026-01-01"}}, "DICOM_QUERY_INVALID"),
    ({"resource": "research"}, "DICOM_PROJECTION_DISABLED"),
])
def test_explicit_query_limits(ctx, query_input, code):
    with pytest.raises(KehrnelError) as error:
        query.compile_query(ctx, {"version": "dicom-query/v1", **query_input})
    assert error.value.code == code


def test_promoted_tag_filter_uses_same_element_and_partial_coverage_is_explicit(ctx):
    ctx.config = {"projections": {"enabled": True}}
    with pytest.raises(KehrnelError) as error:
        query.compile_query(ctx, {"version": "dicom-query/v1", "resource": "research"})
    assert error.value.code == "DICOM_PROJECTION_COVERAGE_UNKNOWN"
    plan = query.compile_query(ctx, {"version": "dicom-query/v1", "resource": "research", "allowPartial": True,
                                    "filter": {"tag": {"path": "00080070", "equals": "SYNTHETIC_MANUFACTURER"}}})
    assert plan.plan["pipeline"][0]["$match"]["attrs"] == {"$elemMatch": {"p": "00080070", "s": "SYNTHETIC_MANUFACTURER"}}
    assert plan.explain["coverage"] == "partial"


def test_projection_versions_follow_config_and_never_change_source(ctx):
    ctx.config = {"projections": {"enabled": True}}
    cfg, example, _, _, instance = linked(ctx)
    projection = model.project(cfg, instance, example["instances"][0])
    assert projection["attrs"] == [{"p": "00080070", "s": "SYNTHETIC_MANUFACTURER"}, {"p": "00180050", "n": 0.5}]
    previous = version(cfg)
    ctx.config["projections"]["tags"] = [{"tag": "00080070", "type": "string"}]
    assert version(config(ctx)) != previous
    assert instance["header"] == example["instances"][0]["header"]


@pytest.mark.asyncio
async def test_default_plan_has_one_instance_index_and_no_search(ctx):
    plan = await DICOMTieredStoreStrategy().plan(ctx)
    assert len(plan.artifacts["indexes"]["dicom_instances"]) == 1
    assert plan.artifacts["search"] == []
    assert "dicom_research" not in plan.artifacts["collections"]
    assert not plan.artifacts["sharding"]["automatic"]


@pytest.mark.asyncio
async def test_optional_search_mapping_is_bounded_and_correlated(ctx):
    ctx.config = {"projections": {"enabled": True, "search_enabled": True}}
    plan = await DICOMTieredStoreStrategy().plan(ctx)
    mapping = plan.artifacts["search"][0]["definition"]["mappings"]
    assert mapping["dynamic"] is False
    assert mapping["fields"]["attrs"]["type"] == "embeddedDocuments"
    assert plan.artifacts["search"][0]["collection"] == "dicom_research"


@pytest.mark.asyncio
async def test_index_apply_does_not_claim_success_after_adapter_warning(ctx):
    class FailingAdmin:
        async def ensure_collection(self, name):
            pass
        async def ensure_indexes(self, name, specs):
            return {"created": [], "warnings": ["unavailable"]}
    ctx.adapters["index_admin"] = FailingAdmin()
    strategy = DICOMTieredStoreStrategy()
    with pytest.raises(KehrnelError) as error:
        await strategy.apply(ctx, await strategy.plan(ctx))
    assert error.value.code == "DICOM_INDEX_APPLY_FAILED"


@pytest.mark.asyncio
async def test_apply_accepts_http_json_plan_and_rejects_unconfigured_artifacts(ctx):
    import json
    from dataclasses import asdict
    class RecordingAdmin:
        async def ensure_collection(self, name):
            assert name.startswith("dicom_")
        async def ensure_indexes(self, name, specs):
            return {"created": [x["options"]["name"] for x in specs], "warnings": []}
    ctx.adapters["index_admin"] = RecordingAdmin()
    strategy = DICOMTieredStoreStrategy()
    wire = json.loads(json.dumps(asdict(await strategy.plan(ctx))))
    result = await strategy.apply(ctx, wire)
    assert "idx_dicom_instance_series_order" in result.created
    wire["artifacts"]["collections"].append("unconfigured")
    with pytest.raises(KehrnelError) as error:
        await strategy.apply(ctx, wire)
    assert error.value.code == "DICOM_APPLY_PLAN_STALE"


def test_nested_sequence_promotion_fails_explicitly(ctx):
    ctx.config = {"projections": {"enabled": True, "tags": [{"tag": "0040A730", "type": "string"}]}}
    cfg, example, _, _, instance = linked(ctx)
    raw = example["instances"][0]
    raw["header"]["0040A730"] = {"vr": "SQ", "Value": [{}]}
    with pytest.raises(KehrnelError) as error:
        model.project(cfg, instance, raw)
    assert error.value.code == "DICOM_PROJECTION_UNSUPPORTED"


def test_description_default_is_exact_and_prefix_is_literal(ctx):
    plan = query.compile_query(ctx, {"version": "dicom-query/v1", "resource": "studies",
        "filter": {"description": "  RM   BRAIN "}})
    assert plan.plan["pipeline"][0]["$match"]["_kehrnel.description"] == "rm brain"
    explicit = query.compile_query(ctx, {"version": "dicom-query/v1", "resource": "studies",
        "filter": {"description": "  RM   BRAIN ", "descriptionMode": "exact"}})
    assert explicit.plan["binding"] == plan.plan["binding"]
    prefix = query.compile_query(ctx, {"version": "dicom-query/v1", "resource": "series",
        "filter": {"description": "RM.*", "descriptionMode": "prefix", "day": "2026-01-01"}})
    assert prefix.plan["pipeline"][0]["$match"]["_kehrnel.description"] == {"$regex": r"^rm\.\*"}


def test_description_prefix_and_series_description_require_day_scope(ctx):
    with pytest.raises(KehrnelError) as error:
        query.compile_query(ctx, {"version": "dicom-query/v1", "resource": "studies",
            "filter": {"description": "RM brain", "descriptionMode": "prefix"}})
    assert error.value.code == "DICOM_DESCRIPTION_DATE_SCOPE_REQUIRED"
    with pytest.raises(KehrnelError) as error:
        query.compile_query(ctx, {"version": "dicom-query/v1", "resource": "studies",
            "filter": {"seriesDescription": "RM brain", "descriptionMode": "exact"}})
    assert error.value.code == "DICOM_DESCRIPTION_DATE_SCOPE_REQUIRED"


def test_description_word_matching_is_outside_the_query_contract(ctx):
    with pytest.raises(KehrnelError) as error:
        query.compile_query(ctx, {"version": "dicom-query/v1", "resource": "studies",
            "filter": {"description": "RM brain", "descriptionMode": "words", "day": "2026-01-01"}})
    assert error.value.code == "DICOM_QUERY_INVALID"


def test_synthetic_recipe_models_twelve_series_and_requested_mr_day():
    generated = synthetic.generate({"seriesPerStudy": 12, "instances": 2,
                                    "modalities": ["MR", "MR"], "day": "2026-10-26"})
    assert len(generated["instances"]) == 48
    assert all(len(study["se"]) == 12 for study in generated["studies"])
    assert all(study["dt"].date().isoformat() == "2026-10-26" for study in generated["studies"])
    assert len({x["_id"] for x in generated["instances"]}) == 48


def test_multi_tag_query_is_same_instance_and_same_value_per_tag(ctx):
    ctx.config['projections'] = {'enabled': True}
    plan = query.compile_query(ctx, {'version': 'dicom-query/v1', 'resource': 'research', 'allowPartial': True,
        'filter': {'tags': [{'path': '00080070', 'equals': 'VENDOR'}, {'path': '00180050', 'gte': 1, 'lt': 2}]}})
    assert plan.explain['tagPredicateCount'] == 2
    assert plan.explain['resultGrain'] == 'instance'
    assert plan.explain['tagCorrelation'] == 'same_instance; same_attribute_value_per_tag'


@pytest.mark.parametrize('filters,code', [
    ({'tag': {'path': '00080070', 'equals': 'X'}, 'tags': [{'path': '00180050', 'gte': 1}]}, 'DICOM_QUERY_INVALID'),
    ({'tags': [{'path': '00180050', 'gte': 1}, {'path': '00180050', 'lt': 2}]}, 'DICOM_TAG_PREDICATE_DUPLICATE'),
    ({'tags': [{'path': '00180050', 'gte': 2, 'lt': 1}]}, 'DICOM_QUERY_INVALID'),
    ({'tags': [{'path': '00180050', 'equals': '1'}]}, 'DICOM_QUERY_INVALID'),
    ({'tags': [{'path': '00080070', 'gte': 1}]}, 'DICOM_QUERY_INVALID'),
    ({'tags': [{'path': '00180080', 'equals': 1}]}, 'DICOM_TAG_NOT_PROMOTED'),
    ({'tags': []}, 'DICOM_QUERY_INVALID'),
    ({'tags': [{'path': '00080070', 'equals': 'X'}] * 17}, 'DICOM_QUERY_INVALID'),
])
def test_multi_tag_contract_rejects_ambiguous_or_unpromoted_predicates(ctx, filters, code):
    ctx.config['projections'] = {'enabled': True}
    with pytest.raises(KehrnelError) as error:
        query.compile_query(ctx, {'version': 'dicom-query/v1', 'resource': 'research', 'allowPartial': True, 'filter': filters})
    assert error.value.code == code


def test_tag_array_cannot_filter_unprojected_instance_headers(ctx):
    with pytest.raises(KehrnelError) as error:
        query.compile_query(ctx, {'version': 'dicom-query/v1', 'resource': 'series', 'filter': {'tags': [{'path': '00080070', 'equals': 'X'}]}})
    assert error.value.code == 'DICOM_TAG_QUERY_REQUIRES_PROJECTION'


@pytest.mark.asyncio
async def test_query_catalog_is_recomputed_from_authorization_and_promotion_policy(ctx):
    strategy = DICOMTieredStoreStrategy()
    before = await strategy.run_op(ctx, 'dicom_query_catalog', {})
    assert before['strategyVersion'] == strategy.manifest.version
    assert not next(x for x in before['resources'] if x['id'] == 'research')['enabled']
    assert not next(x for x in before['examples'] if x['id'] == 'manufacturer')['enabled']
    before['querySchema']['properties']['resource']['enum'].append('injected')
    ctx.meta = {'authorized_centers': ['DEMO_B']}
    ctx.config['projections'] = {'enabled': True, 'tags': [{'tag': '00080070', 'type': 'string'}]}
    after = await strategy.run_op(ctx, 'dicom_query_catalog', {})
    assert after['authorizedCenters'] == ['DEMO_B']
    assert 'injected' not in after['querySchema']['properties']['resource']['enum']
    assert not next(x for x in after['examples'] if x['id'] == 'patient-day')['enabled']
    assert next(x for x in after['examples'] if x['id'] == 'manufacturer')['enabled']
    assert not next(x for x in after['examples'] if x['id'] == 'manufacturer-thickness')['enabled']
    assert after['limits']['maxHeaderInstances'] == 100
    assert after['scaleValidated'] is False


@pytest.mark.asyncio
@pytest.mark.parametrize('change', [
    {'sopUids': ['2.25.1'] * 2}, {'sopUids': ['2.25.' + str(i) for i in range(101)]},
    {'tags': ['00080070', '00080070']}, {'tags': ['00080070'] * 33},
    {'tags': ['header.00080070']}, {'maxBytes': 999999999},
])
async def test_header_batch_contract_rejects_unbounded_or_arbitrary_requests(ctx, change):
    payload = {'seriesUid': '2.25.2001', 'sopUids': ['2.25.100001'], **change}
    with pytest.raises(KehrnelError) as error:
        await DICOMTieredStoreStrategy().run_op(ctx, 'dicom_get_headers', payload)
    assert error.value.code == 'DICOM_OPERATION_INVALID'

"""Opt-in localhost-only integration tests; never open a developer .env file."""
import os
import uuid
from urllib.parse import urlparse

import pytest
import pytest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.dicom.tiered_store.synthetic import generate
from kehrnel.engine.strategies.dicom.tiered_store.strategy import DICOMTieredStoreStrategy
from kehrnel.persistence.artifacts.filesystem import FileSystemArtifactStore
from kehrnel.persistence.mongodb.index_admin import MongoIndexAdminAdapter
from kehrnel.persistence.mongodb.storage import MongoStorageAdapter


@pytest_asyncio.fixture
async def ctx(tmp_path):
    uri = os.environ.get("KEHRNEL_DICOM_TEST_MONGODB_URI")
    if not uri:
        pytest.skip("Set KEHRNEL_DICOM_TEST_MONGODB_URI to a disposable localhost MongoDB.")
    assert urlparse(uri).hostname in {"localhost", "127.0.0.1", "::1"}, "DICOM integration tests require localhost."
    client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=5000)
    await client.admin.command("ping")
    name = "kehrnel_dicom_test_" + uuid.uuid4().hex
    db = client[name]
    adapter = MongoStorageAdapter(db)
    context = StrategyContext(environment_id="dicom-test", config={"database": name},
                              adapters={"storage": adapter, "index_admin": MongoIndexAdminAdapter(db),
                                        "artifact": FileSystemArtifactStore(tmp_path / "metadata")})
    try:
        yield context
    finally:
        # This exact unique test namespace is created by this fixture.
        await client.drop_database(name)
        client.close()


async def run(ctx, request):
    strategy = DICOMTieredStoreStrategy()
    return await strategy.run_op(ctx, "dicom_query", {"version": "dicom-query/v1", **request})


@pytest.mark.asyncio
async def test_core_migration_can_precede_instance_metadata(ctx):
    strategy = DICOMTieredStoreStrategy()
    demo = generate({})
    await strategy.apply(ctx, await strategy.plan(ctx))
    result = await strategy.ingest(ctx, {"studies": demo["studies"]})
    assert (result["studies"], result["series"], result["instances"]) == (2, 2, 0)
    missing = await strategy.run_op(ctx, "dicom_get_header", {"sopUid": demo["instances"][0]["_id"]})
    assert missing["status"] == "not_available"
    await strategy.ingest(ctx, {"instances": demo["instances"]})
    available = await strategy.run_op(ctx, "dicom_get_header", {"sopUid": demo["instances"][0]["_id"]})
    assert available["document"] == demo["instances"][0]


@pytest.mark.asyncio
async def test_numeric_order_cursor_and_real_index_scan(ctx):
    strategy = DICOMTieredStoreStrategy()
    await strategy.apply(ctx, await strategy.plan(ctx))
    demo = generate({})
    await strategy.ingest(ctx, {"studies": demo["studies"], "instances": list(reversed(demo["instances"]))})
    request = {"resource": "instances", "filter": {"seriesUid": "2.25.2001"}, "limit": 5}
    rows, cursor = [], None
    while True:
        result = await run(ctx, {**request, **({"cursor": cursor} if cursor else {})})
        rows += result["rows"]
        cursor = result["explain"]["page"]["nextCursor"]
        if not cursor:
            break
    assert [x["_kehrnel"]["instanceNumber"] for x in rows] == list(range(1, 13))
    plan = await strategy.compile_query(ctx, "dicom", {"version": "dicom-query/v1", **request})
    explanation = await ctx.adapters["storage"].db.command({
        "explain": {"aggregate": "dicom_instances", "pipeline": plan.plan["pipeline"], "cursor": {}},
        "verbosity": "executionStats"})
    assert "idx_dicom_instance_series_order" in str(explanation)
    assert "COLLSCAN" not in str(explanation)
    first = await run(ctx, request)
    with pytest.raises(KehrnelError) as error:
        await run(ctx, {**request, "filter": {"seriesUid": "2.25.2002"}, "cursor": first["explain"]["page"]["nextCursor"]})
    assert error.value.code == "DICOM_CURSOR_INVALID"


@pytest.mark.asyncio
async def test_patient_day_modality_and_center_filters(ctx):
    strategy = DICOMTieredStoreStrategy()
    demo = generate({})
    await strategy.ingest(ctx, {"studies": demo["studies"]})
    one = await run(ctx, {"resource": "studies", "centers": ["DEMO_A"],
                         "filter": {"patientId": "SYNTHETIC_PATIENT_1", "day": "2026-01-01", "modality": "MG"}})
    assert [x["_id"] for x in one["rows"]] == ["2.25.1001"]
    assert "pn" in one["rows"][0] and "se" not in one["rows"][0]
    interval = await run(ctx, {"resource": "studies", "filter": {"fromDay": "2026-01-01", "toDay": "2026-01-02"}})
    assert len(interval["rows"]) == 1
    ctx.meta = {"authorized_centers": ["DEMO_B"]}
    scoped = await run(ctx, {"resource": "series"})
    assert [x["_id"] for x in scoped["rows"]] == ["2.25.2002"]


@pytest.mark.asyncio
async def test_study_modality_does_not_match_a_series_from_another_center(ctx):
    strategy = DICOMTieredStoreStrategy()
    demo = generate({})
    demo["studies"][0]["se"].append({"si": "2.25.2999", "ci": "DEMO_B", "mo": "MR"})
    await strategy.ingest(ctx, {"studies": demo["studies"]})
    ctx.meta = {"authorized_centers": ["DEMO_A"]}
    result = await run(ctx, {"resource": "studies", "filter": {"modality": "MR"}})
    assert result["rows"] == []


@pytest.mark.asyncio
async def test_research_tag_and_value_must_match_same_array_element(ctx):
    strategy = DICOMTieredStoreStrategy()
    ctx.config["projections"] = {"enabled": True, "refresh_on_ingest": True, "tags": [{"tag": "00080070", "type": "string"}, {"tag": "00081030", "type": "string"}]}
    demo = generate({})
    demo["instances"][0]["header"]["00080070"]["Value"] = ["OTHER_MANUFACTURER"]
    demo["instances"][0]["header"]["00081030"] = {"vr": "LO", "Value": ["SYNTHETIC_MANUFACTURER"]}
    await strategy.apply(ctx, await strategy.plan(ctx))
    await strategy.ingest(ctx, {"studies": demo["studies"], "instances": demo["instances"]})
    result = await run(ctx, {"resource": "research", "allowPartial": True,
                            "filter": {"modality": "MG", "tag": {"path": "00080070", "equals": "SYNTHETIC_MANUFACTURER"}}})
    assert len(result["rows"]) == 11
    assert demo["instances"][0]["_id"] not in {x["_id"] for x in result["rows"]}


@pytest.mark.asyncio
async def test_bundle_mode_roundtrip_and_bounded_projection_backfill(ctx):
    strategy = DICOMTieredStoreStrategy()
    ctx.config["headers"] = {"mode": "bundle"}
    demo = generate({})
    await strategy.ingest(ctx, {"studies": demo["studies"], "instances": demo["instances"]})
    doc = await ctx.adapters["storage"].db.dicom_instances.find_one({"_id": demo["instances"][0]["_id"]})
    assert "header" not in doc and "headerRef" in doc["_kehrnel"]
    restored = await strategy.run_op(ctx, "dicom_get_header", {"sopUid": doc["_id"]})
    assert restored["document"] == demo["instances"][0]
    ctx.config["projections"] = {"enabled": True}
    cursor, processed = None, 0
    while True:
        response = await strategy.run_op(ctx, "dicom_rebuild_projections", {
            "seriesUid": "2.25.2001", "limit": 5, **({"cursor": cursor} if cursor else {})})
        processed += response["processed"]
        assert response["publishedCorpus"] is False
        cursor = response["page"]["nextCursor"]
        if not cursor:
            break
    assert processed == 12
    result = await run(ctx, {"resource": "research", "allowPartial": True,
                            "filter": {"tag": {"path": "00080070", "equals": "SYNTHETIC_MANUFACTURER"}}})
    assert len(result["rows"]) == 12


@pytest.mark.asyncio
async def test_idempotency_and_conflicting_uid_scope_fail_closed(ctx):
    strategy = DICOMTieredStoreStrategy()
    demo = generate({})
    batch = {"studies": demo["studies"], "instances": demo["instances"]}
    await strategy.ingest(ctx, batch)
    await strategy.ingest(ctx, batch)
    assert await ctx.adapters["storage"].db.dicom_instances.count_documents({}) == 24
    ctx.config["tenant_id"] = "different"
    with pytest.raises(KehrnelError) as error:
        await strategy.ingest(ctx, {"studies": demo["studies"]})
    assert error.value.code == "DICOM_UID_SCOPE_CONFLICT"
    ctx.config["tenant_id"] = "demo"
    assert len((await run(ctx, {"resource": "studies"}))["rows"]) == 2


@pytest.mark.asyncio
async def test_keyset_paging_handles_duplicate_and_missing_instance_numbers(ctx):
    strategy = DICOMTieredStoreStrategy()
    demo = generate({"instances": 4})
    demo["instances"][1]["header"]["00200013"]["Value"] = ["1"]
    demo["instances"][2]["header"].pop("00200013")
    demo["instances"][3]["header"].pop("00200013")
    await strategy.ingest(ctx, {"studies": demo["studies"], "instances": demo["instances"]})
    cursor, ordered = None, []
    while True:
        response = await run(ctx, {"resource": "instances", "filter": {"seriesUid": "2.25.2001"}, "limit": 1,
                                  **({"cursor": cursor} if cursor else {})})
        ordered.extend(x["_id"] for x in response["rows"])
        cursor = response["explain"]["page"]["nextCursor"]
        if not cursor:
            break
    assert ordered == ["2.25.100001", "2.25.100002", "2.25.100003", "2.25.100004"]


@pytest.mark.asyncio
async def test_same_scope_series_uid_cannot_move_to_a_different_study(ctx):
    strategy = DICOMTieredStoreStrategy()
    demo = generate({})
    await strategy.ingest(ctx, {"studies": demo["studies"]})
    changed = demo["studies"][0]
    changed["_id"] = "2.25.3999"
    with pytest.raises(KehrnelError) as error:
        await strategy.ingest(ctx, {"studies": [changed]})
    assert error.value.code == "DICOM_UID_SCOPE_CONFLICT"


@pytest.mark.asyncio
async def test_instance_arrival_does_not_rewrite_catalog_or_increment_a_retry(ctx):
    class RecordingStorage(MongoStorageAdapter):
        def __init__(self, db):
            super().__init__(db)
            self.written = []
        async def replace_one(self, collection, flt, doc, upsert=False):
            self.written.append(collection)
            return await super().replace_one(collection, flt, doc, upsert=upsert)
        async def update_one(self, collection, flt, update, upsert=False):
            self.written.append(collection)
            return await super().update_one(collection, flt, update, upsert=upsert)
    recorder = RecordingStorage(ctx.adapters["storage"].db)
    ctx.adapters["storage"] = recorder
    strategy = DICOMTieredStoreStrategy()
    await strategy.apply(ctx, await strategy.plan(ctx))
    demo = generate({"instances": 4})
    await strategy.ingest(ctx, {"studies": demo["studies"]})
    recorder.written.clear()
    batch = {"instances": demo["instances"][:2]}
    await strategy.ingest(ctx, batch)
    await strategy.ingest(ctx, batch)
    assert set(recorder.written) == {"dicom_instances"}
    rows = [x async for x in recorder.db.dicom_instances.find({})]
    assert len(rows) == 2
    assert all("patientId" not in x["_kehrnel"] and "day" not in x["_kehrnel"] for x in rows)
    summary = await strategy.run_op(ctx, "dicom_refresh_series_summary", {"seriesUid": "2.25.2001"})
    assert summary["summary"]["importedInstanceDocuments"] == 2
    assert summary["summary"]["sourceReportedCount"] == 4
    # Source refresh preserves the separate runtime count and its as-of time.
    await strategy.ingest(ctx, {"studies": demo["studies"]})
    parent = await recorder.db.dicom_series.find_one({"_id": "2.25.2001"})
    assert parent["_kehrnel"]["metadataSummary"]["importedInstanceDocuments"] == 2


@pytest.mark.asyncio
async def test_research_is_deferred_by_default_and_exact_replays_do_not_invalidate_it(ctx):
    strategy = DICOMTieredStoreStrategy()
    ctx.config["projections"] = {"enabled": True}
    demo = generate({"instances": 2})
    await strategy.ingest(ctx, {"studies": demo["studies"], "instances": demo["instances"]})
    assert await ctx.adapters["storage"].db.dicom_research.count_documents({}) == 0
    await strategy.run_op(ctx, "dicom_rebuild_projections", {"seriesUid": "2.25.2001"})
    assert await ctx.adapters["storage"].db.dicom_research.count_documents({}) == 2
    await strategy.ingest(ctx, {"instances": demo["instances"]})
    assert await ctx.adapters["storage"].db.dicom_research.count_documents({}) == 2
    demo["instances"][0]["header"]["00080070"]["Value"] = ["CHANGED_SYNTHETIC_MANUFACTURER"]
    await strategy.ingest(ctx, {"instances": [demo["instances"][0]]})
    assert await ctx.adapters["storage"].db.dicom_research.count_documents({}) == 1


@pytest.mark.asyncio
async def test_description_filters_and_same_series_modality_semantics(ctx):
    strategy = DICOMTieredStoreStrategy()
    demo = generate({})
    demo["studies"][0]["de"] = "RM examination of the BRAIN"
    demo["studies"][0]["se"][0]["de"] = "Synthetic Mammography"
    demo["studies"][0]["se"].append({"si": "2.25.2999", "ci": "DEMO_A", "mo": "MR", "de": "RM BRAIN without contrast"})
    demo["studies"][0]["se"].append({"si": "2.25.2998", "ci": "DEMO_A", "mo": "MG", "de": "RM.* literal"})
    await strategy.apply(ctx, await strategy.plan(ctx))
    await strategy.ingest(ctx, {"studies": demo["studies"]})
    default = await run(ctx, {"resource": "studies", "filter": {"description": "  rm Examination  OF the brain "}})
    assert [x["_id"] for x in default["rows"]] == ["2.25.1001"]
    partial = await run(ctx, {"resource": "studies", "filter": {"description": "brain"}})
    assert partial["rows"] == []
    prefix = await run(ctx, {"resource": "studies", "filter": {"description": "rm exam", "descriptionMode": "prefix", "day": "2026-01-01"}})
    assert [x["_id"] for x in prefix["rows"]] == ["2.25.1001"]
    interior = await run(ctx, {"resource": "studies", "filter": {"description": "brain", "descriptionMode": "prefix", "day": "2026-01-01"}})
    assert interior["rows"] == []
    exact = await run(ctx, {"resource": "series", "filter": {"description": "rm brain WITHOUT CONTRAST"}})
    assert [x["_id"] for x in exact["rows"]] == ["2.25.2999"]
    literal = await run(ctx, {"resource": "series", "filter": {"description": "rm.*", "descriptionMode": "prefix", "day": "2026-01-01"}})
    assert [x["_id"] for x in literal["rows"]] == ["2.25.2998"]
    correct = await run(ctx, {"resource": "studies", "filter": {"seriesDescription": "rm brain", "descriptionMode": "prefix", "modality": "MR", "day": "2026-01-01"}})
    wrong = await run(ctx, {"resource": "studies", "filter": {"seriesDescription": "rm brain", "descriptionMode": "prefix", "modality": "MG", "day": "2026-01-01"}})
    assert len(correct["rows"]) == 1
    assert wrong["rows"] == []


@pytest.mark.asyncio
async def test_multiple_research_tags_do_not_combine_different_instances_or_values(ctx):
    strategy = DICOMTieredStoreStrategy()
    ctx.config['projections'] = {'enabled': True, 'refresh_on_ingest': True}
    demo = generate({'instances': 3})
    # One instance matches manufacturer only; another matches thickness only.
    demo['instances'][0]['header']['00080070']['Value'] = ['VENDOR']
    demo['instances'][0]['header']['00180050']['Value'] = ['0.5', '2.5']
    demo['instances'][1]['header']['00080070']['Value'] = ['OTHER']
    demo['instances'][1]['header']['00180050']['Value'] = ['1.5']
    demo['instances'][2]['header']['00080070']['Value'] = ['VENDOR']
    demo['instances'][2]['header']['00180050']['Value'] = ['1.5']
    await strategy.apply(ctx, await strategy.plan(ctx))
    await strategy.ingest(ctx, {'studies': demo['studies'], 'instances': demo['instances']})
    response = await run(ctx, {'resource': 'research', 'allowPartial': True, 'filter': {'modality': 'MG', 'tags': [
        {'path': '00080070', 'equals': 'VENDOR'}, {'path': '00180050', 'gte': 1, 'lt': 2}]}})
    assert [row['_id'] for row in response['rows']] == ['2.25.100003']
    assert response['explain']['coverage'] == 'partial'
    assert response['explain']['resultGrain'] == 'instance'


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['inline', 'bundle'])
async def test_header_batch_selection_preserves_vr_order_scope_and_missing_state(ctx, mode):
    from bson import BSON
    strategy = DICOMTieredStoreStrategy()
    ctx.config['headers'] = {'mode': mode}
    demo = generate({'instances': 2})
    demo['instances'][0]['ContentIndex'] = {'v': 1, 'parts': [{'frames': [{'offset': 123}]}]}
    await strategy.ingest(ctx, {'studies': demo['studies'], 'instances': demo['instances']})
    response = await strategy.run_op(ctx, 'dicom_get_headers', {
        'seriesUid': '2.25.2001', 'sopUids': ['2.25.100002', '2.25.100001', '2.25.200001', '2.25.999999'],
        'tags': ['00080070', '00200013', '00180080'], 'includeContentIndex': True})
    assert [row['sopUid'] for row in response['rows']] == ['2.25.100002', '2.25.100001', '2.25.200001', '2.25.999999']
    first, second, foreign, missing = response['rows']
    assert first['header']['00200013'] == {'vr': 'IS', 'Value': ['2']}
    assert first['missingTags'] == ['00180080']
    assert second['ContentIndex'] == demo['instances'][0]['ContentIndex']
    assert second['integrityVerified'] is (mode == 'bundle')
    assert foreign['status'] == missing['status'] == 'not_available'
    assert response['page']['available'] == 2
    assert response['responseBytes'] == len(BSON.encode(response))
    ctx.meta = {'authorized_centers': ['DEMO_B']}
    with pytest.raises(KehrnelError) as error:
        await strategy.run_op(ctx, 'dicom_get_headers', {'seriesUid': '2.25.2001', 'sopUids': ['2.25.100001']})
    assert error.value.code == 'DICOM_SERIES_NOT_FOUND'


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['inline', 'bundle'])
async def test_header_batch_byte_bound_defers_without_truncating_an_attribute(ctx, mode):
    strategy = DICOMTieredStoreStrategy()
    ctx.config['headers'] = {'mode': mode, 'max_response_bytes': 65536}
    demo = generate({'instances': 3})
    demo['instances'][1]['header']['0040A160'] = {'vr': 'UT', 'Value': ['X' * 100000]}
    await strategy.ingest(ctx, {'studies': demo['studies'], 'instances': demo['instances']})
    request = {'seriesUid': '2.25.2001', 'sopUids': ['2.25.100001', '2.25.100002', '2.25.100003']}
    response = await strategy.run_op(ctx, 'dicom_get_headers', request)
    assert response['responseBytes'] <= 65536
    assert response['rows'][0]['status'] == 'available'
    assert [row['status'] for row in response['rows'][1:]] == ['deferred', 'deferred']
    assert response['page']['deferredSopUids'] == request['sopUids'][1:]
    # Selecting smaller fields permits the deferred metadata to be retrieved.
    smaller = await strategy.run_op(ctx, 'dicom_get_headers', {**request, 'tags': ['00200013']})
    assert smaller['page']['available'] == 3
    assert all(set(row['header']) == {'00200013'} for row in smaller['rows'])


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['inline', 'bundle'])
async def test_sdk_example_journey_executes_every_published_example(ctx, mode):
    import importlib.util
    from pathlib import Path
    spec = importlib.util.spec_from_file_location('dicom_sdk_journey', Path(__file__).parents[2] / 'examples/sdk/dicom_query_journey.py')
    example = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(example)
    ctx.config['projections'] = {'enabled': True}
    ctx.config['headers'] = {'mode': mode}
    response = await example.run_journey(ctx, seed=True)
    assert len(response['queries']) == 9
    for scenario in response['catalog']['examples']:
        assert len(response['queries'][scenario['id']]['rows']) == scenario['expectedRows']
    assert response['headers']['page']['available'] == 2
    assert all(set(row['header']) == {'00080070', '00180050', '00200013'} for row in response['headers']['rows'])

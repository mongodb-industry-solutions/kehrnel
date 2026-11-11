import uuid

import pytest
from bson import ObjectId
from bson.binary import Binary, UuidRepresentation

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import QueryPlan, StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.query.executor import execute


class FakeStorage:
    async def aggregate(self, collection, pipeline):
        ehr_id = uuid.UUID("001b7ad3-98da-4d4b-b3c0-7557e97dce22")
        return [
            {
                "ehr_id": Binary.from_uuid(ehr_id, uuid_representation=UuidRepresentation.STANDARD),
                "nested": {"composition_id": bytes(ehr_id.bytes)},
                "payload": Binary(b"clinical-bytes", subtype=0),
            }
        ]


@pytest.mark.asyncio
async def test_execute_normalizes_bson_uuid_result_values():
    ctx = StrategyContext(
        environment_id="env",
        config={"ids": {"ehr_id": "uuidbin", "composition_id": "uuidbin"}},
        adapters={"storage": FakeStorage()},
    )
    plan = QueryPlan(
        engine="mongo_pipeline",
        plan={"collection": "compositions", "pipeline": [{"$limit": 1}]},
    )

    result = await execute(ctx, plan)

    assert result.rows[0]["ehr_id"] == "001b7ad3-98da-4d4b-b3c0-7557e97dce22"
    assert result.rows[0]["nested"]["composition_id"] == "001b7ad3-98da-4d4b-b3c0-7557e97dce22"
    assert result.rows[0]["payload"] == Binary(b"clinical-bytes", subtype=0)


@pytest.mark.asyncio
async def test_execute_preserves_bson_uuid_result_values_for_default_string_ids():
    ehr_id = uuid.UUID("001b7ad3-98da-4d4b-b3c0-7557e97dce22")
    ctx = StrategyContext(
        environment_id="env",
        config={},
        adapters={"storage": FakeStorage()},
    )
    plan = QueryPlan(
        engine="mongo_pipeline",
        plan={"collection": "compositions", "pipeline": [{"$limit": 1}]},
    )

    result = await execute(ctx, plan)

    assert isinstance(result.rows[0]["ehr_id"], Binary)
    assert result.rows[0]["ehr_id"].subtype == 4
    assert result.rows[0]["nested"]["composition_id"] == ehr_id.bytes


@pytest.mark.asyncio
async def test_execute_normalizes_object_ids_for_json_response_rows():
    composition_id = ObjectId("66f8c790f37b6c0a97f98532")

    class ObjectIdStorage:
        async def aggregate(self, collection, pipeline):
            return [
                {
                    "composition_uid": composition_id,
                    "nested": {"source_id": composition_id},
                }
            ]

    ctx = StrategyContext(
        environment_id="env",
        config={},
        adapters={"storage": ObjectIdStorage()},
    )
    plan = QueryPlan(
        engine="mongo_pipeline",
        plan={"collection": "compositions", "pipeline": [{"$limit": 1}]},
    )

    result = await execute(ctx, plan)

    assert result.rows == [
        {
            "composition_uid": "66f8c790f37b6c0a97f98532",
            "nested": {"source_id": "66f8c790f37b6c0a97f98532"},
        }
    ]


@pytest.mark.asyncio
async def test_execute_propagates_storage_failure_instead_of_returning_empty_cohort():
    class FailingStorage:
        async def aggregate(self, collection, pipeline):
            raise RuntimeError("atlas index does not exist: secret-index-name")

    ctx = StrategyContext(
        environment_id="env",
        config={},
        adapters={"storage": FailingStorage()},
    )
    plan = QueryPlan(
        engine="text_search_dual",
        plan={"collection": "search", "pipeline": [{"$search": {}}]},
    )

    with pytest.raises(KehrnelError) as caught:
        await execute(ctx, plan)

    assert caught.value.code == "OPENEHR_QUERY_EXECUTION_FAILED"
    assert caught.value.status == 502
    assert caught.value.details == {
        "engine": "text_search_dual",
        "collection": "search",
        "errorType": "RuntimeError",
    }
    assert "secret-index-name" not in str(caught.value)


@pytest.mark.asyncio
async def test_execute_requires_storage_and_a_target_collection():
    ctx = StrategyContext(environment_id="env", config={}, adapters={})

    with pytest.raises(KehrnelError) as missing_storage:
        await execute(
            ctx,
            QueryPlan(
                engine="mongo_pipeline",
                plan={"collection": "compositions", "pipeline": []},
            ),
        )
    assert missing_storage.value.code == "OPENEHR_QUERY_STORAGE_UNAVAILABLE"

    with pytest.raises(KehrnelError) as missing_collection:
        await execute(
            ctx,
            QueryPlan(engine="mongo_pipeline", plan={"pipeline": []}),
        )
    assert missing_collection.value.code == "OPENEHR_QUERY_PLAN_INVALID"

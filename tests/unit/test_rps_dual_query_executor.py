import uuid

import pytest
from bson.binary import Binary, UuidRepresentation

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

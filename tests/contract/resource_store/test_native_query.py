"""Safety contract for the FHIR native MongoDB read workbench."""

from types import SimpleNamespace

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.resource_store.scripts import native_query
from kehrnel.engine.strategies.fhir.resource_store.scripts.native_query import (
    validate_native_query_payload,
)


def test_native_query_accepts_a_bounded_find_contract():
    result = validate_native_query_payload(
        {
            "resource_type": "Observation",
            "filter": {"status": "final", "valueQuantity.value": {"$gte": 5}},
            "projection": {"id": 1, "status": 1, "valueQuantity": 1},
            "sort": {"effectiveDateTime": -1},
            "limit": 25,
        }
    )

    assert result["resource_type"] == "Observation"
    assert result["limit"] == 25
    assert result["include_operational"] is False


@pytest.mark.parametrize("operator", ["$where", "$function", "$expr", "$accumulator"])
def test_native_query_rejects_unbounded_or_executable_operators(operator):
    with pytest.raises(KehrnelError) as exc_info:
        validate_native_query_payload(
            {
                "resource_type": "Patient",
                "filter": {operator: "unsafe"},
            }
        )

    assert exc_info.value.code == "FHIR_NATIVE_QUERY_INVALID"


def test_native_query_enforces_result_bounds():
    with pytest.raises(KehrnelError):
        validate_native_query_payload({"resource_type": "Patient", "limit": 201})


@pytest.mark.asyncio
async def test_native_query_reads_one_collection_and_hides_operational_fields(
    monkeypatch,
):
    class Cursor:
        def sort(self, value):
            assert value == [("id", 1)]
            return self

        def skip(self, value):
            assert value == 0
            return self

        def limit(self, value):
            assert value == 1
            return self

        def max_time_ms(self, value):
            assert value == native_query.MAX_TIME_MS
            return self

        def __iter__(self):
            return iter(
                [
                    {
                        "resourceType": "Patient",
                        "id": "p1",
                        "_search": {"gender": ["female"]},
                        "_kehrnel": {"storage_schema_version": "1"},
                    }
                ]
            )

    class Collection:
        def find(self, query_filter, projection):
            assert query_filter == {"active": True}
            assert projection is None
            return Cursor()

        def count_documents(self, query_filter, **kwargs):
            assert query_filter == {"active": True}
            assert kwargs == {"maxTimeMS": native_query.MAX_TIME_MS}
            return 1

    closed = []
    mql_context = SimpleNamespace(
        config_loader=object(),
        db={"Patient": Collection()},
    )
    monkeypatch.setattr(
        native_query.bridge, "resolve_strategy_config", lambda _ctx: {"search": {}}
    )
    monkeypatch.setattr(
        native_query.bridge,
        "resolve_mongo",
        lambda _ctx: ("mongodb://example", "fhir", ""),
    )
    monkeypatch.setattr(
        native_query.bridge, "build_mql_context", lambda *_args: mql_context
    )
    monkeypatch.setattr(
        native_query.bridge, "close_mql_context", lambda value: closed.append(value)
    )
    monkeypatch.setattr(
        native_query,
        "resolve_resource_capabilities",
        lambda *_args: SimpleNamespace(storable={"Patient"}),
    )
    ctx = StrategyContext(environment_id="env", config={}, bindings={}, manifest=None)

    result = await native_query.fhir_native_query(
        ctx,
        {
            "resource_type": "Patient",
            "filter": {"active": True},
            "sort": {"id": 1},
            "limit": 1,
        },
    )

    assert result["collection"] == "Patient"
    assert result["rows"] == [{"resourceType": "Patient", "id": "p1"}]
    assert closed == [mql_context]

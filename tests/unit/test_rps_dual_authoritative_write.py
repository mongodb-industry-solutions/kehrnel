from __future__ import annotations

from datetime import datetime, timezone
from importlib import import_module
from types import SimpleNamespace

import pytest

from kehrnel.api.domains.openehr.composition.models import Composition
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy


strategy_module = import_module(
    "kehrnel.engine.strategies.openehr.rps_dual.strategy"
)


def _context(*, composition_id_encoding: str | None = None) -> StrategyContext:
    config = {"database": "tenant_a_openehr"}
    if composition_id_encoding is not None:
        config["ids"] = {"composition_id": composition_id_encoding}
    return StrategyContext(
        environment_id="tenant-a",
        config=config,
        bindings={"db": {"database": "tenant_a_openehr"}},
        adapters={"storage": SimpleNamespace(db=object())},
    )


def _composition() -> dict:
    return {
        "_type": "COMPOSITION",
        "archetype_details": {"template_id": {"value": "vital-signs"}},
        "name": {"_type": "DV_TEXT", "value": "Vital signs"},
    }


@pytest.mark.asyncio
async def test_authoritative_create_uses_native_versioned_service(monkeypatch):
    strategy = RPSDualStrategy()
    flattener = object()
    captured = {}

    async def fake_flattener(ctx):
        return flattener

    async def fake_add(**kwargs):
        captured.update(kwargs)
        uid = f"{kwargs['composition_object_id']}::my-openehr-server::1"
        return Composition(
            _id=uid,
            time_created=datetime.now(timezone.utc),
            data={"_type": "COMPOSITION", "uid": {"value": uid}},
        )

    monkeypatch.setattr(strategy, "_build_flattener_for_context", fake_flattener)
    monkeypatch.setattr(strategy_module, "add_composition", fake_add)

    result = await strategy.execute_authoritative_write(
        _context(composition_id_encoding="objectid"),
        {
            "behavior": "create",
            "idempotencyKey": "create-vitals-001",
            "requesterRef": "clinical-app",
            "requestPayload": {
                "ehrId": "ehr-1",
                "composition": _composition(),
            },
        },
    )

    assert result["changeType"] == "creation"
    assert result["acknowledgement"] == "durable_authoritative"
    assert result["versionUid"].endswith("::my-openehr-server::1")
    assert captured["ehr_id"] == "ehr-1"
    assert captured["flattener"] is flattener
    assert captured["config"].database == "tenant_a_openehr"
    assert captured["config"].flatten_compositions == "compositions_rps"
    assert captured["committer_name"] == "clinical-app"
    assert len(captured["composition_object_id"]) == 24
    assert all(character in "0123456789abcdef" for character in captured["composition_object_id"])


@pytest.mark.asyncio
async def test_authoritative_correction_requires_and_forwards_concurrency_token(
    monkeypatch,
):
    strategy = RPSDualStrategy()
    captured = {}

    async def fake_flattener(ctx):
        return object()

    async def fake_update(**kwargs):
        captured.update(kwargs)
        uid = "composition-1::my-openehr-server::2"
        return Composition(
            _id=uid,
            time_created=datetime.now(timezone.utc),
            data={"_type": "COMPOSITION", "uid": {"value": uid}},
        )

    monkeypatch.setattr(strategy, "_build_flattener_for_context", fake_flattener)
    monkeypatch.setattr(strategy_module, "update_composition", fake_update)

    result = await strategy.execute_authoritative_write(
        _context(),
        {
            "behavior": "update",
            "idempotencyKey": "correct-vitals-001",
            "requesterRef": "clinical-app",
            "requestPayload": {
                "ehrId": "ehr-1",
                "precedingVersionUid": "composition-1::my-openehr-server::1",
                "ifMatch": '"composition-1::my-openehr-server::1"',
                "composition": _composition(),
            },
        },
    )

    assert result["changeType"] == "modification"
    assert result["version"] == "2"
    assert captured["preceding_version_uid"].endswith("::1")
    assert captured["if_match"] == '"composition-1::my-openehr-server::1"'
    assert captured["idempotency_key"] == "correct-vitals-001"


@pytest.mark.asyncio
async def test_authoritative_write_cannot_fall_back_to_ingest():
    with pytest.raises(KehrnelError) as exc:
        await RPSDualStrategy().execute_authoritative_write(
            _context(),
            {
                "behavior": "create",
                "requesterRef": "clinical-app",
                "requestPayload": {
                    "ehrId": "ehr-1",
                    "composition": _composition(),
                },
            },
        )

    assert exc.value.code == "AUTHORITATIVE_WRITE_IDEMPOTENCY_REQUIRED"

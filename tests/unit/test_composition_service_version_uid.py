from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from kehrnel.api.domains.openehr.composition.models import CompositionCreate
from kehrnel.api.domains.openehr.composition import service


class _CaptureFlattener:
    def __init__(self):
        self.raw_docs = []
        self.flush_calls = 0

    def transform_composition(self, raw_doc):
        self.raw_docs.append(deepcopy(raw_doc))
        return {"cn": [{"p": "1", "data": {"name": {"value": "Example"}}}]}, {"sn": [{"p": "1"}]}

    async def flush_codes_to_db(self):
        self.flush_calls += 1


class _FakeCursor:
    def __init__(self, documents):
        self._documents = documents

    async def to_list(self, length=None):
        if length is None:
            return list(self._documents)
        return list(self._documents)[:length]


class _SummaryCollection:
    def __init__(self, documents):
        self.documents = list(documents)
        self.calls = []

    def find(self, query, projection):
        self.calls.append({"query": deepcopy(query), "projection": deepcopy(projection)})
        wanted = set((query.get("_id") or {}).get("$in", []))
        return _FakeCursor([doc for doc in self.documents if doc.get("_id") in wanted])


class _SummaryDb:
    def __init__(self, documents):
        self._collections = {"compositions": _SummaryCollection(documents)}

    def __getitem__(self, name):
        return self._collections[name]


def _composition_payload(uid_value: str) -> dict:
    return {
        "_type": "COMPOSITION",
        "uid": {
            "_type": "OBJECT_VERSION_ID",
            "value": uid_value,
        },
        "archetype_details": {
            "template_id": {
                "value": "test-template",
            }
        },
        "name": {
            "_type": "DV_TEXT",
            "value": "Example composition",
        },
    }


@pytest.mark.asyncio
async def test_add_composition_rewrites_canonical_uid_before_flattening(monkeypatch):
    flattener = _CaptureFlattener()
    captured = {}

    async def fake_find_ehr_by_id(ehr_id, db):
        return {"_id": {"value": ehr_id}}

    async def fake_insert(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(service, "find_ehr_by_id", fake_find_ehr_by_id)
    monkeypatch.setattr(service, "insert_composition_contribution_and_update_ehr", fake_insert)
    monkeypatch.setattr(service.uuid, "uuid4", lambda: "comp-create")

    created = await service.add_composition(
        ehr_id="ehr-1",
        composition_create=CompositionCreate.model_validate(
            _composition_payload("external-source::ehrbase.ehrbase.org::1")
        ),
        db=object(),
        config=SimpleNamespace(),
        flattener=flattener,
    )

    assert created.uid == "comp-create::my-openehr-server::1"
    assert created.data["uid"]["value"] == created.uid
    assert flattener.raw_docs[0]["canonicalJSON"]["uid"]["value"] == created.uid
    assert flattener.raw_docs[0]["time_committed"] == created.time_created
    assert captured["composition_doc"]["data"]["uid"]["value"] == created.uid
    assert flattener.flush_calls == 1


@pytest.mark.asyncio
async def test_bulk_add_compositions_batches_valid_items_and_reports_invalid_ones(monkeypatch):
    flattener = _CaptureFlattener()
    captured = {}

    async def fake_find_ehr_by_id(ehr_id, db):
        return {"_id": {"value": ehr_id}}

    async def fake_insert_many(**kwargs):
        captured.update(kwargs)

    uuid_values = iter(["bulk-comp-1", "bulk-audit-1", "bulk-comp-2", "bulk-audit-2"])

    monkeypatch.setattr(service, "find_ehr_by_id", fake_find_ehr_by_id)
    monkeypatch.setattr(service, "insert_compositions_contributions_and_update_ehr", fake_insert_many)
    monkeypatch.setattr(service.uuid, "uuid4", lambda: next(uuid_values))

    result = await service.bulk_add_compositions(
        ehr_id="ehr-1",
        items=[
            SimpleNamespace(composition=_composition_payload("external-source::ehrbase.ehrbase.org::1")),
            SimpleNamespace(composition={"_type": "COMPOSITION"}),
            {"composition": _composition_payload("external-source::ehrbase.ehrbase.org::2")},
        ],
        db=object(),
        config=SimpleNamespace(),
        flattener=flattener,
        merge_search_docs=True,
    )

    assert result.createdCount == 2
    assert [item.uid for item in result.created] == [
        "bulk-comp-1::my-openehr-server::1",
        "bulk-comp-2::my-openehr-server::1",
    ]
    assert [item.index for item in result.failed] == [1]
    assert captured["ehr_id"] == "ehr-1"
    assert len(captured["composition_docs"]) == 2
    assert [doc["data"]["uid"]["value"] for doc in captured["composition_docs"]] == [
        "bulk-comp-1::my-openehr-server::1",
        "bulk-comp-2::my-openehr-server::1",
    ]
    assert captured["merge_search_docs"] is True
    assert flattener.flush_calls == 1


@pytest.mark.asyncio
async def test_update_composition_rewrites_uid_and_reflattens(monkeypatch):
    flattener = _CaptureFlattener()
    captured = {}

    async def fake_retrieve_composition(**kwargs):
        return SimpleNamespace(uid=kwargs["uid_based_id"])

    async def fake_insert(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(service, "retrieve_composition", fake_retrieve_composition)
    monkeypatch.setattr(service, "insert_composition_contribution_and_update_ehr", fake_insert)

    updated = await service.update_composition(
        ehr_id="ehr-1",
        preceding_version_uid="comp-1::my-openehr-server::1",
        if_match='"comp-1::my-openehr-server::1"',
        new_composition_data=CompositionCreate.model_validate(
            _composition_payload("external-source::ehrbase.ehrbase.org::4")
        ),
        db=object(),
        config=SimpleNamespace(),
        flattener=flattener,
        merge_search_docs=True,
    )

    assert updated.uid == "comp-1::my-openehr-server::2"
    assert updated.data["uid"]["value"] == updated.uid
    assert flattener.raw_docs[0]["canonicalJSON"]["uid"]["value"] == updated.uid
    assert flattener.raw_docs[0]["composition_version"] == "2"
    assert flattener.raw_docs[0]["time_committed"] == updated.time_created
    assert captured["composition_doc"]["data"]["uid"]["value"] == updated.uid
    assert captured["merge_search_docs"] is True
    assert flattener.flush_calls == 1


@pytest.mark.asyncio
async def test_delete_composition_passes_config_and_target_uid(monkeypatch):
    captured = {}
    config = SimpleNamespace(
        contributions="contributions",
        ehr="ehr",
        flatten_compositions="compositions_rps",
        search_compositions="compositions_search",
        merge_search_docs=False,
    )

    async def fake_retrieve_ehr_by_id(ehr_id, db):
        return {"_id": {"value": ehr_id}}

    async def fake_retrieve_composition(**kwargs):
        return SimpleNamespace(uid=kwargs["uid_based_id"])

    async def fake_find_deletion_contribution_for_version(preceding_version_uid, db):
        return None

    async def fake_add_deletion_contribution_and_update_ehr(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(service, "retrieve_ehr_by_id", fake_retrieve_ehr_by_id)
    monkeypatch.setattr(service, "retrieve_composition", fake_retrieve_composition)
    monkeypatch.setattr(service, "find_deletion_contribution_for_version", fake_find_deletion_contribution_for_version)
    monkeypatch.setattr(service, "add_deletion_contribution_and_update_ehr", fake_add_deletion_contribution_and_update_ehr)

    await service.delete_composition_by_preceding_uid(
        ehr_id="ehr-1",
        preceding_version_uid="comp-1::my-openehr-server::1",
        if_match='"comp-1::my-openehr-server::1"',
        db=object(),
        config=config,
    )

    assert captured["ehr_id"] == "ehr-1"
    assert captured["preceding_version_uid"] == "comp-1::my-openehr-server::1"
    assert captured["config"] is config


@pytest.mark.asyncio
async def test_bulk_delete_compositions_batches_only_valid_items(monkeypatch):
    captured = {}
    config = SimpleNamespace(
        contributions="contributions",
        ehr="ehr",
        flatten_compositions="compositions_rps",
        search_compositions="compositions_search",
        merge_search_docs=False,
    )

    async def fake_retrieve_ehr_by_id(ehr_id, db):
        return SimpleNamespace(
            compositions=[
                SimpleNamespace(id=SimpleNamespace(value="comp-1::my-openehr-server::1")),
                SimpleNamespace(id=SimpleNamespace(value="comp-2::my-openehr-server::1")),
            ]
        )

    async def fake_find_compositions_by_uids(uids, db, cfg):
        assert cfg is config
        return [{"_id": "comp-1::my-openehr-server::1"}, {"_id": "comp-2::my-openehr-server::1"}]

    async def fake_find_deletion_contributions_for_versions(uids, db):
        return [{"versions": [{"preceding_version_uid": "comp-2::my-openehr-server::1"}]}]

    async def fake_add_bulk_deletion_contributions_and_update_ehr(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(service, "retrieve_ehr_by_id", fake_retrieve_ehr_by_id)
    monkeypatch.setattr(service, "find_compositions_by_uids", fake_find_compositions_by_uids)
    monkeypatch.setattr(service, "find_deletion_contributions_for_versions", fake_find_deletion_contributions_for_versions)
    monkeypatch.setattr(service, "add_bulk_deletion_contributions_and_update_ehr", fake_add_bulk_deletion_contributions_and_update_ehr)

    result = await service.bulk_delete_compositions(
        ehr_id="ehr-1",
        preceding_version_uids=[
            "comp-1::my-openehr-server::1",
            "comp-2::my-openehr-server::1",
            "comp-3::my-openehr-server::1",
        ],
        db=object(),
        config=config,
    )

    assert result.deletedCount == 1
    assert result.deletedUids == ["comp-1::my-openehr-server::1"]
    assert [item.uid for item in result.failed] == [
        "comp-3::my-openehr-server::1",
        "comp-2::my-openehr-server::1",
    ]
    assert captured["ehr_id"] == "ehr-1"
    assert captured["preceding_version_uids"] == ["comp-1::my-openehr-server::1"]
    assert captured["config"] is config


@pytest.mark.asyncio
async def test_list_composition_summaries_uses_ehr_refs_and_projects_only_needed_fields(monkeypatch):
    db = _SummaryDb(
        [
            {
                "_id": "comp-2::my-openehr-server::1",
                "data": {
                    "name": {"value": "Second"},
                    "archetype_details": {"template_id": {"value": "template-b"}},
                },
            },
            {
                "_id": "comp-1::my-openehr-server::1",
                "data": {
                    "name": {"value": "First"},
                    "archetype_details": {"template_id": {"value": "template-a"}},
                },
            },
        ]
    )

    async def fake_find_ehr_by_id(ehr_id, _db):
        return {
            "_id": {"value": ehr_id},
            "compositions": [
                {"id": {"value": "comp-1::my-openehr-server::1"}},
                {"id": {"value": "comp-2::my-openehr-server::1"}},
            ],
        }

    monkeypatch.setattr(service, "find_ehr_by_id", fake_find_ehr_by_id)

    rows = await service.list_composition_summaries(
        ehr_id="ehr-1",
        db=db,
        config=SimpleNamespace(compositions="compositions"),
    )

    assert [row.uid for row in rows] == [
        "comp-1::my-openehr-server::1",
        "comp-2::my-openehr-server::1",
    ]
    assert [row.name for row in rows] == ["First", "Second"]
    assert [row.templateId for row in rows] == ["template-a", "template-b"]
    assert db["compositions"].calls == [
        {
            "query": {"_id": {"$in": ["comp-1::my-openehr-server::1", "comp-2::my-openehr-server::1"]}},
            "projection": {
                "_id": 1,
                "data.name.value": 1,
                "data.archetype_details.template_id.value": 1,
            },
        }
    ]

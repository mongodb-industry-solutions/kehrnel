"""Opt-in synthetic integration tests. Never write to the configured source database."""

from copy import deepcopy
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timezone
import json, os
import pytest
from kehrnel.engine.domains.openehr.analytics import (
    Converter,
    RuleSet,
    MongoProjectionStore,
    Backfill,
    run_backfill,
)
from kehrnel.engine.domains.openehr.analytics.connections import (
    read_environment,
    connect,
)
from kehrnel.engine.domains.openehr.analytics.stream import process_events

ROOT = Path(__file__).resolve().parents[3]
AT = datetime(2026, 11, 18, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def mongo():
    envfile = os.environ.get("ANALYTICS_TEST_ENV_FILE")
    if not envfile:
        pytest.skip(
            "Set ANALYTICS_TEST_ENV_FILE to run isolated synthetic MongoDB tests"
        )
    env = read_environment(envfile)
    # The configured target is the only writable database; the source is not read.
    source_client, target_client, source, _ = connect(env)
    prefix = "validation_" + uuid4().hex[:12] + "_"
    store = MongoProjectionStore(
        target_client[env["ANALYTICS_TARGET_DATABASE"]],
        source_namespace="synthetic-validation",
        prefix=prefix,
    )
    rules = RuleSet.compile(
        json.loads(
            (ROOT / "examples/openehr_analytics/config.canonical.json").read_text()
        )["rules"]
    )
    created_source = prefix + "synthetic_source"
    try:
        store.prepare(rules.by_revision.values())
        yield store, rules, created_source
    finally:
        # Exact allowlist of this fixture's generated collections, never a database drop.
        for name in [*store.names.values(), created_source]:
            store.db.drop_collection(name)
        source_client.close()
        target_client.close()


@pytest.fixture
def specimen():
    return json.loads(
        (ROOT / "examples/openehr_analytics/composition.canonical.json").read_text()
    )


def converter(rules):
    return Converter(rules, source_namespace="synthetic-validation")


def projection(rules, doc):
    return converter(rules).convert(doc, accepted_at=AT)


def test_transaction_retry_update_removal_and_counts(mongo, specimen):
    store, rules, _ = mongo
    specimen["_id"] = "atomic-" + uuid4().hex
    p = projection(rules, specimen)
    assert store.write(p)["decision"] == "write"
    assert store.write(p)["decision"] == "unchanged"
    assert (
        store.collection("facts").count_documents(
            {"context.composition_id": p.composition_id}
        )
        == 4
    )
    before = sum(
        d["metrics"]["compositions"]
        for d in store.collection("activity_rollups").find({})
    )
    specimen["composition_version"] = "2"
    specimen["canonicalJSON"]["content"] = specimen["canonicalJSON"]["content"][:1]
    assert store.write(projection(rules, specimen))["decision"] == "write"
    assert (
        store.collection("facts").count_documents(
            {"context.composition_id": p.composition_id}
        )
        == 2
    )
    assert (
        sum(
            d["metrics"]["compositions"]
            for d in store.collection("activity_rollups").find({})
        )
        == before
    )
    specimen["composition_version"] = "3"
    tombstone = converter(rules).convert(specimen, accepted_at=AT, deleted=True)
    assert store.write(tombstone)["decision"] == "write"
    assert (
        store.collection("facts").count_documents(
            {"context.composition_id": p.composition_id}
        )
        == 0
    )
    assert store.collection("activity").find_one({"_id": p.composition_id}) is None
    assert (
        sum(
            d["metrics"]["compositions"]
            for d in store.collection("activity_rollups").find({})
        )
        == before - 1
    )
    assert store.write(p)["decision"] == "stale"


def test_mixed_batch_rollback(mongo, specimen):
    store, rules, _ = mongo
    a = deepcopy(specimen)
    b = deepcopy(specimen)
    a["_id"] = "rollback-a-" + uuid4().hex
    b["_id"] = "rollback-b-" + uuid4().hex
    old = projection(rules, b)
    store.write(old)
    b["provider_id"] = "Changed without version"
    with pytest.raises(ValueError, match="content changed"):
        store.write_batch([projection(rules, a), projection(rules, b)])
    assert store.collection("activity").find_one({"_id": a["_id"]}) is None


def test_historical_rule_removes_no_longer_selected_fact(mongo, specimen):
    store, rules, _ = mongo
    specimen["_id"] = "rules-" + uuid4().hex
    store.write(projection(rules, specimen))
    original = next(iter(rules.by_revision.values())).document()
    change = {
        k: deepcopy(v) for k, v in original.items() if k not in ("_id", "revision")
    }
    change["epoch"] = 2
    change["effective_at"] = "2027-01-01T00:00:00Z"
    change["catalog"]["templates"][0]["groups"] = []
    revised = RuleSet.compile([original, change])
    r = revised.by_template[specimen["template_name"]][-1]
    store.prepare([r])
    p = converter(revised).convert(specimen, accepted_at=AT, rule_revision=r.revision)
    store.write(p)
    assert (
        store.collection("facts").count_documents(
            {"context.composition_id": specimen["_id"]}
        )
        == 0
    )
    assert store.collection("activity").find_one({"_id": specimen["_id"]}) is not None


def test_keyset_resume_and_template_filter(mongo, specimen):
    store, rules, source_name = mongo
    source = store.db[source_name]
    prefix = uuid4().hex
    for n in range(5):
        doc = deepcopy(specimen)
        doc["_id"] = prefix + str(n)
        source.insert_one(doc)
    different = deepcopy(specimen)
    different.update(_id=prefix + "5", template_name="Another template")
    source.insert_one(different)
    revision = next(iter(rules.by_revision))
    job = Backfill(
        "backfill-" + prefix,
        specimen["template_name"],
        revision,
        AT,
        "synthetic-validation",
        lower_id=prefix,
        upper_id=prefix + "z",
        batch_size=2,
    )
    state = run_backfill(source, store, converter(rules), job, max_batches=1)
    assert state["processed"] == 2 and state["status"] == "ready"
    state = run_backfill(source, store, converter(rules), job)
    assert state["processed"] == 5 and state["status"] == "complete"
    assert store.collection("activity").find_one({"_id": different["_id"]}) is None
    assert run_backfill(source, store, converter(rules), job)["processed"] == 5


def test_stream_acknowledges_after_idempotent_publication(mongo, specimen):
    store, rules, _ = mongo
    specimen["_id"] = "stream-" + uuid4().hex
    event = {"token": {"offset": 1}, "document": specimen, "accepted_at": AT}
    stream_id = uuid4().hex
    process_events([event], store, converter(rules), stream_id=stream_id)
    result = process_events([event], store, converter(rules), stream_id=stream_id)
    assert result["state"]["token"] == event["token"]
    assert (
        store.collection("facts").count_documents(
            {"context.composition_id": specimen["_id"]}
        )
        == 4
    )
    malformed = {"token": {"offset": 2}, "accepted_at": AT}
    with pytest.raises(ValueError):
        process_events([malformed], store, converter(rules), stream_id=stream_id)
    assert (
        store.collection("streams").find_one({"_id": stream_id})["token"]
        == event["token"]
    )


def test_failure_after_fact_insert_rolls_back_every_collection(
    mongo, specimen, monkeypatch
):
    store, rules, _ = mongo
    specimen["_id"] = "abort-" + uuid4().hex
    original = store.collection

    class BrokenActivity:
        def bulk_write(self, *args, **kwargs):
            raise RuntimeError("Injected failure after inserting facts")

    with monkeypatch.context() as patch:
        patch.setattr(
            store,
            "collection",
            lambda key: BrokenActivity() if key == "activity" else original(key),
        )
        with pytest.raises(RuntimeError, match="Injected failure"):
            store.write(projection(rules, specimen))
    assert (
        original("facts").count_documents({"context.composition_id": specimen["_id"]})
        == 0
    )
    assert original("projection_state").find_one({"_id": specimen["_id"]}) is None


def test_crash_after_publication_replays_without_double_counting(
    mongo, specimen, monkeypatch
):
    from types import SimpleNamespace

    store, rules, source_name = mongo
    source = store.db[source_name]
    prefix = uuid4().hex
    for n in range(3):
        doc = deepcopy(specimen)
        doc["_id"] = prefix + str(n)
        source.insert_one(doc)
    job = Backfill(
        "crash-" + prefix,
        specimen["template_name"],
        next(iter(rules.by_revision)),
        AT,
        "synthetic-validation",
        lower_id=prefix,
        upper_id=prefix + "z",
        batch_size=3,
    )
    original = store.collection

    class LostCheckpoint:
        def __getattr__(self, key):
            return getattr(original("jobs"), key)

        def update_one(self, query, update, **kwargs):
            if "cursor" in update.get("$set", {}):
                return SimpleNamespace(matched_count=0)
            return original("jobs").update_one(query, update, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(
            store,
            "collection",
            lambda key: LostCheckpoint() if key == "jobs" else original(key),
        )
        with pytest.raises(RuntimeError, match="after write"):
            run_backfill(source, store, converter(rules), job)
    count_before = sum(
        d["metrics"]["compositions"] for d in original("activity_rollups").find({})
    )
    state = run_backfill(source, store, converter(rules), job)
    assert state["processed"] == 3 and state["status"] == "complete"
    assert (
        sum(d["metrics"]["compositions"] for d in original("activity_rollups").find({}))
        == count_before
    )


def test_sdk_ingest_uses_the_same_transaction_store(mongo, specimen):
    import asyncio
    from types import SimpleNamespace
    from kehrnel.engine.core.types import StrategyContext
    from kehrnel.engine.strategies.openehr.analytics.strategy import (
        OpenEHRAnalyticsStrategy,
    )

    store, rules, _ = mongo
    cfg = json.loads(
        (ROOT / "examples/openehr_analytics/config.canonical.json").read_text()
    )
    cfg.update(
        database=store.db.name,
        source_namespace=store.namespace,
        collection_prefix=store.names["facts"][: -len("facts")],
    )
    # The default runtime database may return naive BSON dates; the adapter must
    # normalize them or updates could decrement a different daily counter key.
    db = store.db.with_options(
        codec_options=store.db.codec_options.with_options(tz_aware=False)
    )
    ctx = StrategyContext(
        "analytics", cfg, adapters={"storage": SimpleNamespace(db=db)}
    )
    strategy = OpenEHRAnalyticsStrategy()
    specimen["_id"] = "sdk-" + uuid4().hex
    request = {"document": specimen, "accepted_at": AT.isoformat()}
    assert (
        asyncio.run(strategy.run_op(ctx, "analytics_ingest", request))["decision"]
        == "write"
    )
    before = sum(
        d["metrics"]["compositions"]
        for d in store.collection("activity_rollups").find({})
    )
    specimen["composition_version"] = "2"
    assert (
        asyncio.run(strategy.run_op(ctx, "analytics_ingest", request))["decision"]
        == "write"
    )
    assert (
        sum(
            d["metrics"]["compositions"]
            for d in store.collection("activity_rollups").find({})
        )
        == before
    )

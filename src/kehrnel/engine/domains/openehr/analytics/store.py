"""Bounded MongoDB transactions: facts, activity, lineage, receipts and count deltas."""

from collections import Counter
from datetime import datetime, timezone
from copy import deepcopy
from .source import digest

COLLECTIONS = (
    "facts",
    "activity",
    "patients",
    "source_refs",
    "activity_rollups",
    "projection_state",
    "rule_revisions",
    "jobs",
    "issues",
    "model_binding",
    "streams",
)


def decide(current, incoming):
    """Componentwise ordering prevents a stale rule or stale source overwriting newer work."""
    if current is None:
        return "write"
    source_order = incoming["source_version"] - current["source_version"]
    same_template = incoming["template"] == current["template"]
    rule_order = incoming["rule_epoch"] - current["rule_epoch"] if same_template else 0
    if source_order * rule_order < 0:
        raise ValueError(
            "Concurrent source/rule advancement: reread the latest source and pin at least the stored rule epoch"
        )
    if source_order < 0 or rule_order < 0:
        return "stale"
    if (
        source_order == 0
        and incoming["source_fingerprint"] != current["source_fingerprint"]
    ):
        raise ValueError("Source content changed without a new composition version")
    if (
        same_template
        and rule_order == 0
        and incoming["rule_revision"] != current["rule_revision"]
    ):
        raise ValueError("A rule epoch was reused with different immutable contents")
    if source_order == 0 and rule_order == 0:
        if incoming["decoder_revision"] != current["decoder_revision"]:
            raise ValueError(
                "Decoder changed; approve a new rule epoch before reprocessing"
            )
        if incoming["status"] != current["status"]:
            raise ValueError("Outcome changed without a source/rule version change")
        return "unchanged"
    return "write"


def activity_dimensions(activity):
    if activity is None:
        return None
    c = activity["context"]
    day = (
        c["event_at"]
        .astimezone(timezone.utc)
        .replace(hour=0, minute=0, second=0, microsecond=0)
    )
    return {
        "provider_id": c["provider_id"],
        "template_name": c["template_name"],
        "day": day,
    }


class MongoProjectionStore:
    def __init__(
        self,
        database,
        *,
        source_namespace,
        prefix="analytics_",
        max_facts=50000,
        max_bytes=32 * 1024 * 1024,
        rollup_partitions=64
    ):
        if not prefix or not prefix.replace("_", "").isalnum():
            raise ValueError("Use a simple analytics collection prefix")
        if database is not None:
            from bson.binary import UuidRepresentation

            database = database.with_options(
                codec_options=database.codec_options.with_options(
                    tz_aware=True, uuid_representation=UuidRepresentation.STANDARD
                )
            )
        self.db = database
        self.namespace = source_namespace
        self.names = {key: prefix + key for key in COLLECTIONS}
        if type(rollup_partitions) is not int or not 1 <= rollup_partitions <= 4096:
            raise ValueError("Use 1–4096 fixed count partitions")
        self.rollup_partitions = rollup_partitions
        self.max_facts = max_facts
        self.max_bytes = max_bytes
        self.bound = False

    def collection(self, key):
        return self.db[self.names[key]]

    def ensure_binding(self, session=None):
        from pymongo import ReturnDocument

        value = self.collection("model_binding").find_one_and_update(
            {"_id": "source"},
            {
                "$setOnInsert": {
                    "namespace": self.namespace,
                    "schema_version": 1,
                    "rollup_partitions": self.rollup_partitions,
                }
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
            session=session,
        )
        if value["namespace"] != self.namespace:
            raise ValueError(
                "Target collections already belong to a different source namespace"
            )
        if value.get("rollup_partitions") != self.rollup_partitions:
            raise ValueError(
                "Count partition configuration is immutable for this target"
            )
        self.bound = True

    def plan(self):
        return {
            "facts": [
                {"keys": [("context.composition_id", 1)], "name": "composition"},
                {
                    "keys": [
                        ("context.patient_id", 1),
                        ("context.subject_id", 1),
                        ("context.kind", 1),
                        ("context.event_at", 1),
                    ],
                    "name": "patient_subject_kind_time",
                },
                {
                    "keys": [("context.kind", 1), ("context.event_at", 1)],
                    "name": "kind_time",
                },
                {
                    "keys": [("context.provider_id", 1), ("context.event_at", 1)],
                    "name": "provider_time",
                },
                {
                    "keys": [("expires_at", 1)],
                    "name": "fact_expiry",
                    "expireAfterSeconds": 0,
                },
            ],
            "activity": [
                {
                    "keys": [("context.template_name", 1), ("context.event_at", 1)],
                    "name": "template_time",
                },
                {
                    "keys": [("context.patient_id", 1), ("context.event_at", 1)],
                    "name": "patient_time",
                },
            ],
            "projection_state": [
                {
                    "keys": [("status", 1), ("template", 1), ("_id", 1)],
                    "name": "mapping_coverage",
                }
            ],
            "activity_rollups": [
                {
                    "keys": [
                        ("context.day", 1),
                        ("context.provider_id", 1),
                        ("context.template_name", 1),
                    ],
                    "name": "daily_activity",
                }
            ],
            "rule_revisions": [
                {
                    "keys": [("template", 1), ("epoch", 1)],
                    "unique": True,
                    "name": "template_epoch",
                }
            ],
            "jobs": [
                {"keys": [("status", 1), ("lease_until", 1)], "name": "job_leases"}
            ],
        }

    def prepare(self, rules=()):
        self.ensure_binding()
        existing = set(self.db.list_collection_names())
        from pymongo.errors import CollectionInvalid

        for name in self.names.values():
            if name not in existing:
                try:
                    self.db.create_collection(name)
                except CollectionInvalid:
                    pass
        for key, indexes in self.plan().items():
            for index in indexes:
                self.collection(key).create_index(
                    index["keys"], **{k: v for k, v in index.items() if k != "keys"}
                )
        for r in rules:
            record = r.document()
            record["template"] = r.template
            self.collection("rule_revisions").update_one(
                {"_id": r.revision}, {"$setOnInsert": record}, upsert=True
            )
        return self.names

    def write(self, projection):
        return self.write_batch([projection])[0]

    def write_bounded(self, projections):
        """Split an iterator by count AND BSON bytes; each composition remains atomic.

        Already acknowledged chunks can be replayed if a later chunk fails.
        The caller advances its input checkpoint only after this method returns.
        """
        batch, results, size, facts = [], [], 0, 0
        for projection in projections:
            nbytes = self.projection_bytes(projection)
            nfacts = len(projection.facts)
            if nbytes > self.max_bytes or nfacts > self.max_facts:
                raise ValueError(
                    "One composition exceeds the transaction budget; review before retrying"
                )
            if batch and (
                len(batch) >= 128
                or size + nbytes > self.max_bytes
                or facts + nfacts > self.max_facts
                or any(p.composition_id == projection.composition_id for p in batch)
            ):
                results.extend(self.write_batch(batch))
                batch, size, facts = [], 0, 0
            batch.append(projection)
            size += nbytes
            facts += nfacts
        if batch:
            results.extend(self.write_batch(batch))
        return results

    @staticmethod
    def projection_bytes(projection):
        from bson import BSON

        total = 0
        for document in [
            projection.activity,
            projection.source_ref,
            projection.state(),
            *projection.facts,
        ]:
            if document is not None:
                length = len(BSON.encode(document))
                if length >= 16 * 1024 * 1024:
                    raise ValueError(
                        "A projected document exceeds the MongoDB document limit"
                    )
                total += length
        return total

    def write_batch(self, projections):
        from bson import BSON
        from pymongo import DeleteMany, InsertOne, ReplaceOne, UpdateOne
        from pymongo.errors import DuplicateKeyError
        from pymongo.read_concern import ReadConcern
        from pymongo.write_concern import WriteConcern

        rows = list(projections)
        if not rows:
            return []
        if len(rows) > 128 or len({p.composition_id for p in rows}) != len(rows):
            raise ValueError("Write at most 128 different compositions per transaction")
        fact_count = sum(len(p.facts) for p in rows)
        size = 0
        for p in rows:
            for doc in [p.activity, p.source_ref, p.state(), *p.facts]:
                if doc is None:
                    continue
                length = len(BSON.encode(doc))
                if length >= 16 * 1024 * 1024:
                    raise ValueError(
                        "A projected document exceeds the MongoDB document limit"
                    )
                size += length
        if fact_count > self.max_facts or size > self.max_bytes:
            raise ValueError(
                "Projection exceeds the configured transaction budget; split the input batch or review this composition"
            )
        if not self.bound:
            self.ensure_binding()

        def transact(session):
            current = {
                r["_id"]: r
                for r in self.collection("projection_state").find(
                    {"_id": {"$in": [p.composition_id for p in rows]}}, session=session
                )
            }
            operations = {
                k: [] for k in ("facts", "activity", "source_refs", "projection_state")
            }
            deltas = Counter()
            dimensions = {}
            results = []
            for p in rows:
                old = current.get(p.composition_id)
                state = p.state()
                decision = decide(old, state)
                results.append(
                    {
                        "composition_id": p.composition_id,
                        "decision": decision,
                        "status": p.status,
                        "facts": len(p.facts),
                    }
                )
                if decision != "write":
                    continue
                new_dim = activity_dimensions(p.activity)
                if new_dim is not None:
                    new_dim["partition"] = (
                        int(digest(p.composition_id)[:8], 16) % self.rollup_partitions
                    )
                old_dim = old.get("activity_dimensions") if old else None
                for dim, delta in [(old_dim, -1), (new_dim, 1)]:
                    if dim is not None:
                        key = digest(dim)
                        dimensions[key] = dim
                        deltas[key] += delta
                state["activity_dimensions"] = new_dim
                # Deleting prior output and replacing all collections in one transaction
                # removes obsolete facts when selection, boundaries or source records change.
                if old:
                    operations["facts"].append(
                        DeleteMany({"context.composition_id": p.composition_id})
                    )
                operations["facts"].extend(InsertOne(f) for f in p.facts)
                for key, doc in [
                    ("activity", p.activity),
                    ("source_refs", p.source_ref),
                ]:
                    operations[key].append(
                        ReplaceOne({"_id": p.composition_id}, doc, upsert=True)
                        if doc is not None
                        else DeleteMany({"_id": p.composition_id})
                    )
                operations["projection_state"].append(
                    ReplaceOne({"_id": p.composition_id}, state, upsert=True)
                )
            for key, ops in operations.items():
                if ops:
                    self.collection(key).bulk_write(ops, ordered=True, session=session)
            rollups = [
                UpdateOne(
                    {"_id": key},
                    {
                        "$setOnInsert": {"context": dimensions[key]},
                        "$inc": {"metrics.compositions": delta},
                    },
                    upsert=True,
                )
                for key, delta in deltas.items()
                if delta
            ]
            if rollups:
                self.collection("activity_rollups").bulk_write(
                    rollups, ordered=True, session=session
                )
            return results

        # Replica-set/sharded transactions are mandatory: no non-atomic fallback.
        for attempt in range(3):
            try:
                with self.db.client.start_session() as session:
                    return session.with_transaction(
                        transact,
                        read_concern=ReadConcern("snapshot"),
                        write_concern=WriteConcern("majority"),
                    )
            except DuplicateKeyError:
                if attempt == 2:
                    raise

    def upsert_patient(self, document):
        """Patient snapshots are a separate input, never copied into every fact."""
        from pymongo.errors import DuplicateKeyError

        if not document.get("_id") or not isinstance(
            document.get("snapshot", {}).get("as_of"), datetime
        ):
            raise ValueError("Patient snapshot needs _id and a BSON as_of date")
        self.ensure_binding()
        try:
            result = self.collection("patients").replace_one(
                {
                    "_id": document["_id"],
                    "$or": [
                        {"snapshot.as_of": {"$lt": document["snapshot"]["as_of"]}},
                        {"snapshot.as_of": {"$exists": False}},
                    ],
                },
                deepcopy(document),
                upsert=True,
            )
            return (
                "written"
                if result.modified_count or result.upserted_id
                else "unchanged"
            )
        except DuplicateKeyError:
            current = self.collection("patients").find_one({"_id": document["_id"]})
            if (
                current["snapshot"]["as_of"] == document["snapshot"]["as_of"]
                and current != document
            ):
                raise ValueError("Conflicting patient snapshot at the same timestamp")
            return "stale_or_unchanged"

"""Template-scoped keyset backfills with leases and pinned, resumable rule choices."""

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from .projection import utc
from .source import digest, SourceError


@dataclass(frozen=True)
class Backfill:
    job_id: str
    template: str
    rule_revision: str
    accepted_at: datetime
    source_namespace: str
    lower_id: object = None
    upper_id: object = None
    committed_from: datetime | None = None
    committed_until: datetime | None = None
    batch_size: int = 64

    def __post_init__(self):
        if (
            not self.job_id
            or not self.template
            or not self.rule_revision
            or not self.source_namespace
        ):
            raise ValueError(
                "Pin job identity, template, rule revision and source namespace"
            )
        if type(self.batch_size) is not int or not 1 <= self.batch_size <= 128:
            raise ValueError("Batch size is 1–128 compositions")
        if (
            self.committed_from
            and self.committed_until
            and utc(self.committed_from) >= utc(self.committed_until)
        ):
            raise ValueError("Use a non-empty half-open committed-time window")

    @property
    def revision(self):
        return digest(asdict(self))

    def query(self, fields, after=None):
        q = {} if self.template == "*" else {fields.template: self.template}
        bounds = {}
        if self.lower_id is not None:
            bounds["$gte"] = self.lower_id
        if after is not None:
            bounds["$gt"] = after
        if self.upper_id is not None:
            bounds["$lt"] = self.upper_id
        if bounds:
            q[fields.composition_id] = bounds
        date = {}
        if self.committed_from:
            date["$gte"] = utc(self.committed_from)
        if self.committed_until:
            date["$lt"] = utc(self.committed_until)
        if date:
            q[fields.committed_at] = date
        return q


def run_backfill(
    source_collection,
    store,
    converter,
    job,
    *,
    owner=None,
    lease_seconds=300,
    max_batches=None
):
    """One bounded partition per invocation. Resume the same immutable job after failures."""
    from pymongo import ReturnDocument

    if not 1 <= lease_seconds <= 3600 or (max_batches is not None and max_batches < 1):
        raise ValueError("Use a positive batch limit and a 1–3600 second lease")
    if (
        source_collection.database.client is store.db.client
        and source_collection.database.name == store.db.name
        and source_collection.name in store.names.values()
    ):
        raise ValueError("The source must not be an analytical output collection")
    if (
        job.source_namespace != converter.namespace
        or job.source_namespace != store.namespace
    ):
        raise ValueError("Job source namespace differs from its converter/target")
    if job.template == "*":
        if job.rule_revision != converter.rules.revision:
            raise ValueError("Initial migration pins the full rule-set revision")
    else:
        converter.rules.select(job.template, job.accepted_at, job.rule_revision)
    runtime_revision = digest(
        {
            "decoder": (
                converter.decoder.revision
                if converter.decoder
                else "canonical-openEHR/v1"
            ),
            "fields": converter.fields.__dict__,
            "namespace": converter.namespace,
        }
    )
    owner = owner or str(uuid4())
    jobs = store.collection("jobs")
    now = datetime.now(timezone.utc)
    jobs.update_one(
        {"_id": job.job_id},
        {
            "$setOnInsert": {
                "spec": asdict(job),
                "spec_revision": job.revision,
                "runtime_revision": runtime_revision,
                "status": "ready",
                "processed": 0,
            }
        },
        upsert=True,
    )
    current = jobs.find_one({"_id": job.job_id})
    if (
        current["spec_revision"] != job.revision
        or current.get("runtime_revision") != runtime_revision
    ):
        raise ValueError(
            "A resumed job cannot change its template, revision, bounds or clock"
        )
    if current["status"] == "complete":
        return current
    current = jobs.find_one_and_update(
        {
            "_id": job.job_id,
            "spec_revision": job.revision,
            "$or": [
                {"lease_until": {"$lte": now}},
                {"lease_until": {"$exists": False}},
            ],
        },
        {
            "$set": {
                "owner": owner,
                "status": "running",
                "lease_until": now + timedelta(seconds=lease_seconds),
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if not current:
        raise ValueError("This job partition already has an active worker")
    after = current.get("cursor")
    batches = 0
    try:
        while max_batches is None or batches < max_batches:
            now = datetime.now(timezone.utc)
            renewed = jobs.update_one(
                {"_id": job.job_id, "owner": owner, "lease_until": {"$gt": now}},
                {"$set": {"lease_until": now + timedelta(seconds=lease_seconds)}},
            )
            if not renewed.matched_count:
                raise RuntimeError("Backfill lease lost")
            cursor = (
                source_collection.find(job.query(converter.fields, after))
                .sort(converter.fields.composition_id, 1)
                .limit(job.batch_size)
                .batch_size(job.batch_size)
                .max_time_ms(60000)
            )
            consumed = 0
            last_id = after

            def converted():
                nonlocal consumed, last_id
                for doc in cursor:
                    projection = converter.convert(
                        doc,
                        accepted_at=job.accepted_at,
                        rule_revision=(
                            None if job.template == "*" else job.rule_revision
                        ),
                    )
                    consumed += 1
                    last_id = doc[converter.fields.composition_id]
                    yield projection

            try:
                results = store.write_bounded(converted())
            except (ValueError, SourceError) as exc:
                issue = {
                    "job": job.job_id,
                    "template": job.template,
                    "cursor": after,
                    "code": type(exc).__name__,
                    "message": str(exc),
                    "at": datetime.now(timezone.utc),
                }
                store.collection("issues").update_one(
                    {"_id": digest({"job": job.job_id, "cursor": after})},
                    {"$set": issue},
                    upsert=True,
                )
                raise
            finally:
                cursor.close()
            if not consumed:
                jobs.update_one(
                    {"_id": job.job_id, "owner": owner},
                    {
                        "$set": {"status": "complete"},
                        "$unset": {"owner": "", "lease_until": ""},
                    },
                )
                return jobs.find_one({"_id": job.job_id})
            after = last_id
            checkpoint = jobs.update_one(
                {
                    "_id": job.job_id,
                    "owner": owner,
                    "lease_until": {"$gt": datetime.now(timezone.utc)},
                },
                {
                    "$set": {"cursor": after},
                    "$inc": {
                        "processed": consumed,
                        "written": sum(r["decision"] == "write" for r in results),
                    },
                },
            )
            if not checkpoint.matched_count:
                raise RuntimeError(
                    "Backfill lease lost after write; retry safely from the previous checkpoint"
                )
            batches += 1
        jobs.update_one(
            {"_id": job.job_id, "owner": owner},
            {"$set": {"status": "ready"}, "$unset": {"owner": "", "lease_until": ""}},
        )
        return jobs.find_one({"_id": job.job_id})
    except Exception:
        jobs.update_one(
            {"_id": job.job_id, "owner": owner},
            {"$set": {"status": "failed"}, "$unset": {"owner": "", "lease_until": ""}},
        )
        raise

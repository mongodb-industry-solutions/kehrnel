"""Consume versioned source events and advance a durable token only after publication."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4
from .source import digest
from .projection import utc


def process_events(
    events, store, converter, *, stream_id, owner=None, lease_seconds=300
):
    """Events: token, document, accepted_at, optional deleted.

    The caller resumes its source stream after the token returned by stream_state.
    Input must be ordered, replayable and include the exact versioned full image.
    Hard deletes without a versioned envelope are deliberately unsupported.
    """
    from pymongo import ReturnDocument

    if converter.namespace != store.namespace or not stream_id:
        raise ValueError("Stream identity and converter/target namespace must match")
    if not 1 <= lease_seconds <= 3600:
        raise ValueError("Use a lease of 1–3600 seconds")
    owner = owner or str(uuid4())
    states = store.collection("streams")
    now = datetime.now(timezone.utc)
    states.update_one(
        {"_id": stream_id},
        {"$setOnInsert": {"namespace": converter.namespace}},
        upsert=True,
    )
    if states.find_one({"_id": stream_id})["namespace"] != converter.namespace:
        raise ValueError("Stream belongs to another source")
    state = states.find_one_and_update(
        {
            "_id": stream_id,
            "$or": [
                {"lease_until": {"$lte": now}},
                {"lease_until": {"$exists": False}},
            ],
        },
        {
            "$set": {
                "owner": owner,
                "lease_until": now + timedelta(seconds=lease_seconds),
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if not state:
        raise ValueError("This stream already has an active consumer")
    count = 0
    try:
        for event in events:
            now = datetime.now(timezone.utc)
            lease = states.update_one(
                {"_id": stream_id, "owner": owner, "lease_until": {"$gt": now}},
                {"$set": {"lease_until": now + timedelta(seconds=lease_seconds)}},
            )
            if not lease.matched_count:
                raise RuntimeError(
                    "Stream lease expired; resume from the acknowledged token"
                )
            if not all(k in event for k in ("token", "document", "accepted_at")):
                raise ValueError(
                    "A stream event requires a token, versioned document and stable accepted_at"
                )
            projection = converter.convert(
                event["document"],
                accepted_at=utc(event["accepted_at"]),
                deleted=event.get("deleted", False),
            )
            store.write(projection)
            checkpoint = states.update_one(
                {
                    "_id": stream_id,
                    "owner": owner,
                    "lease_until": {"$gt": datetime.now(timezone.utc)},
                },
                {
                    "$set": {
                        "token": event["token"],
                        "rules_revision": converter.rules.revision,
                    },
                    "$inc": {"acknowledged": 1},
                },
            )
            if not checkpoint.matched_count:
                raise RuntimeError("Stream lease lost after write; replay is safe")
            count += 1
        return {"processed": count, "state": states.find_one({"_id": stream_id})}
    finally:
        states.update_one(
            {"_id": stream_id, "owner": owner},
            {"$unset": {"owner": "", "lease_until": ""}},
        )


def mongo_events(changes):
    """Adapt a MongoDB change stream opened with full_document='required'.

    Update post-images must be enabled on the source by its operator. Never use
    updateLookup here: it may return a later version than the event being mapped.
    Resume tokens expire with change-stream history; recovery needs a new backfill.
    """
    for change in changes:
        if change.get("operationType") not in (
            "insert",
            "replace",
            "update",
        ) or not change.get("fullDocument"):
            raise ValueError(
                "Source event lacks an exact versioned post-image; use a versioned tombstone for deletion"
            )
        yield {
            "token": change["_id"],
            "document": change["fullDocument"],
            "accepted_at": change["clusterTime"].as_datetime(),
        }

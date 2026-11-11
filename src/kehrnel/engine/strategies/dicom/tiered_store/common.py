from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import jsonschema
from bson import BSON, json_util

from kehrnel.engine.core.errors import KehrnelError

ROOT = Path(__file__).parent
DEFAULTS = json.loads((ROOT / "defaults.json").read_text())
STRATEGY_VERSION = json.loads((ROOT / "manifest.json").read_text())["version"]
SCHEMA = json.loads((ROOT / "schema.json").read_text())


def fail(code, message, status=400):
    raise KehrnelError(code=code, status=status, message=message)


def merge(base, extra):
    result = copy.deepcopy(base)
    for key, value in extra.items():
        result[key] = merge(result[key], value) if isinstance(value, dict) and isinstance(result.get(key), dict) else copy.deepcopy(value)
    return result


def config(ctx):
    cfg = merge(DEFAULTS, ctx.config or {})
    try:
        jsonschema.Draft7Validator(SCHEMA).validate(cfg)
    except jsonschema.ValidationError:
        fail("CONFIG_INVALID", "DICOM configuration does not satisfy its published schema.")
    names = list(cfg["collections"].values())
    if len(names) != len(set(names)):
        fail("CONFIG_INVALID", "DICOM collection names must be distinct.")
    if cfg["query"]["default_limit"] > cfg["query"]["max_limit"]:
        fail("CONFIG_INVALID", "The default query limit cannot exceed the maximum.")
    tags = [item["tag"] for item in cfg["projections"]["tags"]]
    if len(tags) != len(set(tags)):
        fail("CONFIG_INVALID", "Promoted DICOM tags must be unique.")
    return cfg


def digest(value):
    return hashlib.sha256(json_util.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def version(cfg):
    return "v1:" + digest(cfg["projections"])


def centers(ctx, cfg):
    allowed = set(cfg["allowed_centers"])
    # Only trusted runtime context may narrow authorization. Request payloads
    # cannot add centers or change the tenant/database.
    restricted = (ctx.meta or {}).get("authorized_centers")
    if restricted is not None:
        allowed &= set(restricted)
    if not allowed:
        fail("DICOM_SCOPE_REQUIRED", "No authorized DICOM center is available.", 403)
    return sorted(allowed)


def storage(ctx):
    adapter = (ctx.adapters or {}).get("storage")
    if adapter is None:
        fail("STORAGE_REQUIRED", "A MongoDB storage adapter is required.", 503)
    return adapter


def artifacts(ctx):
    adapter = (ctx.adapters or {}).get("artifact")
    if adapter is None:
        fail("ARTIFACT_STORE_REQUIRED", "Metadata bundle mode requires an artifact adapter.", 503)
    return adapter


def check_cancel(ctx):
    cancelled = (ctx.meta or {}).get("should_cancel")
    if callable(cancelled) and cancelled():
        fail("OPERATION_CANCELLED", "The DICOM operation was cancelled.", 409)


async def write(ctx, collection, document, *, preserve_runtime=False):
    envelope = document["_kehrnel"]
    # Preserve source UIDs, but never replace a UID belonging to another tenant
    # or center. A duplicate UID conflict fails rather than overwriting it.
    try:
        identity = {"_id": document["_id"], "_kehrnel.tenantId": envelope["tenantId"],
                    "_kehrnel.centerId": envelope["centerId"],
                    "_kehrnel.studyUid": envelope["studyUid"]}
        if "seriesUid" in envelope:
            identity["_kehrnel.seriesUid"] = envelope["seriesUid"]
        if preserve_runtime:
            return await storage(ctx).update_one(collection, identity, {
                "$set": {"source": document["source"],
                         **{"_kehrnel." + key: value for key, value in envelope.items()}},
                "$setOnInsert": {"_id": document["_id"]},
            }, upsert=True)
        return await storage(ctx).replace_one(
            collection,
            identity,
            document, upsert=True,
        )
    except Exception as exc:
        from pymongo.errors import DuplicateKeyError
        if isinstance(exc, DuplicateKeyError):
            fail("DICOM_UID_SCOPE_CONFLICT", "The source UID already belongs to a different scope.", 409)
        raise


def bounded_bson(document, maximum=15 * 1024 * 1024):
    if len(BSON.encode(document)) > maximum:
        fail("DICOM_DOCUMENT_TOO_LARGE", "The DICOM document exceeds the configured byte bound.")

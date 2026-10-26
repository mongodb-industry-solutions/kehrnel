"""Governed, single-resource FHIR create and update execution.

This boundary is intentionally distinct from bulk ingestion.  It adds FHIR
logical identity, version preconditions and authoritative acknowledgements,
then reuses the strategy's reviewed validation/projection persistence core.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.resource_store.scripts import bridge
from kehrnel.engine.strategies.fhir.resource_store.scripts.import_resources import (
    fhir_import_resources,
)
from kehrnel.engine.strategies.fhir.resource_store.scripts.storage_adapter import (
    MongoFHIRStorageAdapter,
)


def _etag_version(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r'(?:W/)?"([^"\\]+)"', value.strip())
    return match.group(1) if match else None


async def _read_current(
    ctx: StrategyContext,
    resource_type: str,
    resource_id: str,
) -> dict[str, Any] | None:
    cfg = bridge.resolve_strategy_config(ctx)
    uri, database, prefix = bridge.resolve_mongo(ctx)
    search_cfg = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
    mql_ctx = bridge.build_mql_context(
        uri,
        database,
        prefix,
        search_cfg.get("config_dir"),
        search_cfg.get("compartment_definitions_dir"),
    )
    try:
        adapter = MongoFHIRStorageAdapter(mql_ctx.db, collection_prefix=prefix)
        return await asyncio.to_thread(adapter.read, resource_type, resource_id)
    finally:
        bridge.close_mql_context(mql_ctx)


async def execute_authoritative_write(
    ctx: StrategyContext,
    request: dict[str, Any],
) -> dict[str, Any]:
    """Execute one FHIR create or version-checked update."""

    behavior = str(request.get("behavior") or "").lower()
    if behavior not in {"create", "update"}:
        raise KehrnelError(
            code="AUTHORITATIVE_WRITE_BEHAVIOR_UNSUPPORTED",
            status=422,
            message="The FHIR Resource Store supports governed create and update writes only.",
            details={"behavior": behavior},
        )
    idempotency_key = request.get("idempotencyKey")
    if not isinstance(idempotency_key, str) or not idempotency_key.strip():
        raise KehrnelError(
            code="AUTHORITATIVE_WRITE_IDEMPOTENCY_REQUIRED",
            status=422,
            message="An idempotency key is required for authoritative FHIR writes.",
        )
    request_payload = request.get("requestPayload")
    request_payload = request_payload if isinstance(request_payload, dict) else {}
    supplied_resource = request_payload.get("resource", request_payload)
    if not isinstance(supplied_resource, dict):
        raise KehrnelError(
            code="AUTHORITATIVE_WRITE_RESOURCE_REQUIRED",
            status=422,
            message="requestPayload.resource must be a canonical FHIR resource.",
        )
    resource = deepcopy(supplied_resource)
    resource_type = resource.get("resourceType")
    if not isinstance(resource_type, str) or not resource_type:
        raise KehrnelError(
            code="AUTHORITATIVE_WRITE_RESOURCE_TYPE_REQUIRED",
            status=422,
            message="The FHIR resourceType is required.",
        )
    resource_id = resource.get("id")
    if behavior == "create" and not resource_id:
        resource_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"kehrnel:{ctx.environment_id}:{idempotency_key.strip()}",
            )
        )
        resource["id"] = resource_id
    if not isinstance(resource_id, str) or not resource_id:
        raise KehrnelError(
            code="AUTHORITATIVE_WRITE_RESOURCE_ID_REQUIRED",
            status=422,
            message="A FHIR logical id is required for update.",
        )

    current = await _read_current(ctx, resource_type, resource_id)
    expected_versions: dict[tuple[str, str], str] | None = None
    if behavior == "create":
        if current is not None:
            raise KehrnelError(
                code="FHIR_WRITE_CONFLICT",
                status=409,
                message=f"{resource_type}/{resource_id} already exists.",
            )
        next_version = "1"
        mode = "create"
        change_type = "creation"
    else:
        if current is None:
            raise KehrnelError(
                code="FHIR_RESOURCE_NOT_FOUND",
                status=404,
                message=f"{resource_type}/{resource_id} does not exist.",
            )
        current_meta = current.get("meta")
        current_meta = current_meta if isinstance(current_meta, dict) else {}
        current_version = str(current_meta.get("versionId") or "")
        supplied_version = _etag_version(request_payload.get("ifMatch"))
        if not current_version or supplied_version != current_version:
            raise KehrnelError(
                code="FHIR_VERSION_CONFLICT",
                status=409,
                message="ifMatch must identify the current FHIR resource version.",
                details={"expected": current_version or None},
            )
        try:
            next_version = str(int(current_version) + 1)
        except ValueError as exc:
            raise KehrnelError(
                code="FHIR_VERSION_UNSUPPORTED",
                status=409,
                message="The current resource version is not managed by this strategy.",
            ) from exc
        expected_versions = {(resource_type, resource_id): current_version}
        mode = "upsert"
        change_type = "modification"

    meta = resource.get("meta")
    meta = deepcopy(meta) if isinstance(meta, dict) else {}
    meta["versionId"] = next_version
    meta["lastUpdated"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    resource["meta"] = meta
    report = await fhir_import_resources(
        ctx,
        {
            "resource": resource,
            "mode": mode,
            "fail_on_error": True,
        },
        provenance={
            "source": "governed-runtime",
            "operation": "authoritative_write",
            "interaction_id": request.get("interactionId"),
        },
        expected_versions=expected_versions,
    )
    if not report.get("committed"):
        raise KehrnelError(
            code="FHIR_AUTHORITATIVE_WRITE_REJECTED",
            status=422,
            message="FHIR validation rejected the authoritative write.",
            details={"validation": report.get("validation") or {}},
        )

    version_uid = f"{resource_type}/{resource_id}/_history/{next_version}"
    return {
        "resourceType": resource_type,
        "resourceId": resource_id,
        "versionUid": version_uid,
        "version": next_version,
        "etag": f'W/"{next_version}"',
        "changeType": change_type,
        "acknowledgement": "durable_authoritative",
    }

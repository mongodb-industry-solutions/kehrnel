from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, Request, status
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import ValidationError

from kehrnel.api.bridge.app.core.config_models import CompositionCollectionNames
from kehrnel.api.bridge.app.core.database import (
    ensure_active_openehr_dictionaries,
    get_mongodb_ehr_db,
    resolve_active_openehr_context,
)
from kehrnel.api.domains.openehr.aql.service import execute_aql_query_payload
from kehrnel.api.domains.openehr.composition.dependencies import get_composition_config
from kehrnel.api.domains.openehr.composition.models import CompositionCreate
from kehrnel.api.domains.openehr.composition.service import add_composition, retrieve_composition
from kehrnel.api.domains.openehr.contribution.repository import (
    find_contribution_by_version_uid,
    find_deletion_contribution_for_version,
)
from kehrnel.engine.domains.openehr.aql.aql_to_ast import ParseError
from kehrnel.engine.domains.openehr.aql.parser import AQLParser
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener

from .models import (
    AqlExecuteRequest,
    AqlExecuteResponse,
    CompositionCreateRequest,
    CompositionCreateResponse,
    CompositionGetRequest,
    CompositionGetResponse,
)


router = APIRouter(prefix="/internal/v1", tags=["Internal Runtime"])


def _extract_env_id(request: Request) -> str:
    return (
        request.headers.get("x-active-env")
        or request.headers.get("x-env-id")
        or request.headers.get("x-environment-id")
        or request.query_params.get("env_id")
        or request.query_params.get("environment")
        or request.path_params.get("env_id")
        or ""
    ).strip()


async def _get_flattener(request: Request) -> CompositionFlattener:
    context = await resolve_active_openehr_context(request, ensure_ingestion=True)
    await ensure_active_openehr_dictionaries(request, context=context)
    flattener = getattr(request.app.state, "flattener", None)
    if flattener is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Ingestion runtime is not initialized. Configure strategy ingestion first.",
        )
    return flattener


def _parse_version_uid(uid: str) -> tuple[str, int]:
    parts = uid.split("::")
    if len(parts) != 3:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Malformed composition UID '{uid}' found in persistence.",
        )

    try:
        return parts[0], int(parts[2])
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Malformed composition version '{uid}' found in persistence.",
        ) from exc


async def _resolve_requested_uid(
    payload: CompositionGetRequest,
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames,
) -> str:
    composition_uid = (payload.compositionUid or "").strip()
    requested_version = payload.version

    if not composition_uid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="compositionUid is required.",
        )

    if "::" in composition_uid:
        _, actual_version = _parse_version_uid(composition_uid)
        if requested_version is not None and requested_version != actual_version:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "version does not match the version encoded in compositionUid. "
                    f"Requested version={requested_version}, uid version={actual_version}."
                ),
            )
        return composition_uid

    if requested_version is None:
        return composition_uid

    version_doc = await db[config.compositions].find_one(
        {
            "_id": {
                "$regex": f"^{re.escape(composition_uid)}::[^:]+::{requested_version}$",
            }
        },
        {"_id": 1},
    )

    if not version_doc or not version_doc.get("_id"):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Composition with id '{composition_uid}' and version '{requested_version}' "
                f"not found in EHR '{payload.ehrId}'"
            ),
        )

    return str(version_doc["_id"])


def _extract_template_id(composition_data: dict | None) -> str | None:
    template_id = (
        ((((composition_data or {}).get("archetype_details") or {}).get("template_id") or {}).get("value"))
        or ""
    ).strip()
    return template_id or None


def _validate_internal_context(request: Request, context) -> str:
    header_env_id = _extract_env_id(request)
    body_env_id = (context.environmentId or "").strip()

    if not body_env_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="context.environmentId is required.",
        )

    if header_env_id and header_env_id != body_env_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-Active-Env header must match context.environmentId.",
        )

    return body_env_id


@router.post(
    "/compositions",
    response_model=CompositionCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Internal runtime composition creation",
)
async def create_composition(
    payload: CompositionCreateRequest,
    request: Request,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db),
    config: CompositionCollectionNames = Depends(get_composition_config),
    flattener: CompositionFlattener = Depends(_get_flattener),
) -> CompositionCreateResponse:
    _validate_internal_context(request, payload.context)

    if payload.ifNoneExist and payload.ifNoneExist.strip():
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="ifNoneExist is reserved in the internal contract but not implemented yet.",
        )

    if payload.idempotencyKey and payload.idempotencyKey.strip():
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="idempotencyKey is reserved in the internal contract but not implemented yet.",
        )

    try:
        composition_create = CompositionCreate.model_validate(payload.composition)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Request body must contain a valid canonical COMPOSITION: {exc}",
        ) from exc

    body_template_id = composition_create.template_id
    if body_template_id != payload.templateId:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "templateId does not match the canonical composition payload. "
                f"Request templateId='{payload.templateId}', payload templateId='{body_template_id}'."
            ),
        )

    composition = await add_composition(
        ehr_id=payload.ehrId,
        composition_create=composition_create,
        db=db,
        config=config,
        flattener=flattener,
        merge_search_docs=config.merge_search_docs,
        committer_name=(payload.context.principal or "Facade"),
    )

    _, version = _parse_version_uid(composition.uid)
    contribution_doc = await find_contribution_by_version_uid(composition.uid, db)
    if not contribution_doc or not contribution_doc.get("_id"):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Contribution for composition '{composition.uid}' was not found after creation.",
        )

    time_committed = (
        ((contribution_doc or {}).get("audit") or {}).get("time_committed")
        or composition.time_created
    )

    return CompositionCreateResponse(
        ehrId=payload.ehrId,
        compositionUid=composition.uid,
        version=version,
        contributionId=str(contribution_doc["_id"]),
        timeCommitted=time_committed,
        representation=composition.data if payload.returnMode == "canonical" else None,
    )


@router.post(
    "/compositions/get",
    response_model=CompositionGetResponse,
    status_code=status.HTTP_200_OK,
    summary="Internal runtime composition retrieval",
)
async def get_composition(
    payload: CompositionGetRequest,
    request: Request,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db),
    config: CompositionCollectionNames = Depends(get_composition_config),
) -> CompositionGetResponse:
    _validate_internal_context(request, payload.context)

    resolved_uid = await _resolve_requested_uid(payload, db, config)
    composition = await retrieve_composition(
        ehr_id=payload.ehrId,
        uid_based_id=resolved_uid,
        db=db,
        config=config,
    )

    _, version = _parse_version_uid(composition.uid)
    contribution_doc = await find_contribution_by_version_uid(composition.uid, db)
    deleted = await find_deletion_contribution_for_version(composition.uid, db) is not None
    time_committed = (
        ((contribution_doc or {}).get("audit") or {}).get("time_committed")
        or composition.time_created
    )

    return CompositionGetResponse(
        ehrId=payload.ehrId,
        compositionUid=composition.uid,
        version=version,
        templateId=_extract_template_id(composition.data),
        deleted=deleted,
        timeCommitted=time_committed,
        representation=composition.data if payload.view == "canonical" else None,
    )


@router.post(
    "/aql/execute",
    response_model=AqlExecuteResponse,
    status_code=status.HTTP_200_OK,
    summary="Internal runtime AQL execution",
)
async def execute_aql(
    payload: AqlExecuteRequest,
    request: Request,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db),
) -> AqlExecuteResponse:
    _validate_internal_context(request, payload.context)

    aql = (payload.aql or "").strip()
    if not aql:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="aql is required.",
        )

    context = await resolve_active_openehr_context(request, ensure_ingestion=False)
    await ensure_active_openehr_dictionaries(request, context=context)

    try:
        result = await execute_aql_query_payload(
            ast_query=AQLParser(aql).parse_with_method("handwritten"),
            request_url=str(request.url),
            db=db,
            ehr_id=payload.ehrId,
            request=request,
            executed_aql=aql,
            include_executed_aql=payload.includeExecutedAql,
            include_explain=payload.includeExplain or payload.context.includeDebug,
            query_parameters=payload.parameters,
            fetch=payload.fetch,
            offset=payload.offset,
            timeout_ms=payload.timeoutMs,
            feature_mode=payload.featureMode,
        )
    except (ParseError, ValueError, NotImplementedError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error processing AQL query: {exc}",
        ) from exc

    columns = {
        str(column.get("name")): (
            str(column.get("path")) if column.get("path") is not None else None
        )
        for column in result["columns"]
    }
    return AqlExecuteResponse(
        columns=columns,
        rows=result["rows"],
        meta=result["meta"],
        debug=result["debug"],
    )

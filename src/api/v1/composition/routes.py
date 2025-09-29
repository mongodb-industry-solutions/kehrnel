from fastapi import APIRouter, Depends, status, Body, Response, Header, Query, Request
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional, Dict, Any
from email.utils import formatdate
import logging
from uuid import UUID
from src.transform.flattener_g import CompositionFlattener


from src.api.v1.composition.service import (
    add_composition, 
    retrieve_composition, 
    update_composition,
    delete_composition_by_preceding_uid,
    retrieve_revision_history,
    retrieve_versioned_composition,
    retrieve_composition_version,
)

from src.api.v1.common.models import RevisionHistory, OriginalVersionResponse
from src.api.v1.composition.models import Composition, CompositionCreate, VersionedComposition


from src.app.core.database import get_mongodb_ehr_db


from src.api.v1.composition.api_responses import (
    create_composition_responses,
    get_composition_responses,
    update_composition_responses,
    delete_composition_responses,
    get_revision_history_responses,
    get_versioned_composition_responses,
    get_composition_version_at_time_responses,
)


router = APIRouter(
    prefix="/ehr/{ehr_id}",
    tags=["Composition"]
)


def get_flattener(request: Request) -> CompositionFlattener:
    """
    Dependency to retrieve the globally initialized CompositionFlattener
    """
    return request.app.state.flattener


@router.post(
    "/composition",
    response_model = Composition,
    response_model_by_alias = False,
    status_code = status.HTTP_201_CREATED,
    summary = "Create Composition",
    responses = create_composition_responses
)
async def create_composition_endpoint(
    ehr_id: str,
    response: Response,
    composition_create: CompositionCreate = Body(
        ...,
        description = "The composition object to be created, structured according to a template"
    ),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db),
    flattener: CompositionFlattener = Depends(get_flattener)
):
    """
    Creates a new `composition` and adds it to the specified EHR.

    This endpoint creates the first version of a new `composition`.
    - A new `contribution` is created to audit this change.
    - The new `composition` is atomically stored and linked to the EHR.
    - A semi-flattened version of the composition is also created and stored.

    Upon successfull creation, the new `composition` object is returned, and the `Location`, `ETag`, and `Last-Modified` headers are set.
    """

    new_composition = await add_composition(
        ehr_id = ehr_id,
        composition_create = composition_create,
        db = db,
        flattener = flattener
    )

    # Response Headers
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/composition/{new_composition.uid}"
    response.headers["ETag"] = f'"{new_composition.uid}"'
    last_modified_gmt = formatdate(new_composition.time_created.timestamp(), usegmt = True)
    response.headers["Last-Modified"] = last_modified_gmt

    return new_composition


@router.get(
    "/composition/{uid_based_id}",
    status_code = status.HTTP_200_OK,
    summary = "Get Composition by version or object ID",
    responses = get_composition_responses
)
async def get_composition_by_id(
    ehr_id: str,
    uid_based_id: str,
    response: Response,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves a version of a `COMPOSITION` from a given EHR.

    The composition is identified by its `uid_based_id`, which can be either:
    - A `version_uid` (e.g., `...::server::1`) to fetch a specific version.
    - A `versioned_object_uid` (e.g., `...`) to fetch the latest version.

    The full canonical `COMPOSITION` object is returned in the response body.
    The `ETag`, `Last-Modified`, and `Location` headers are set for the specific
    version that is returned.
    """
    composition = await retrieve_composition(
        ehr_id = ehr_id,
        uid_based_id = uid_based_id,
        db = db
    )

    # Return explicit JSONResponse with headers and status code
    # The 'composition.uid' will always be the full version_uid of the returned object
    last_modified_gmt = formatdate(composition.time_created.timestamp(), usegmt = True)
    return JSONResponse(
        content=composition.data,
        status_code=status.HTTP_200_OK,
        headers={
            "ETag": f'"{composition.uid}"',
            "Location": f"/v1/ehr/{ehr_id}/composition/{composition.uid}",
            "Last-Modified": last_modified_gmt
        }
    )


@router.put(
    "/composition/{preceding_version_uid}",
    response_model=Composition,
    response_model_by_alias=False,
    status_code = status.HTTP_200_OK,
    summary = "Update Composition by version ID",
    responses = update_composition_responses
)
async def update_composition_endpoint(
    ehr_id: str,
    preceding_version_uid: str,
    response: Response,
    composition_data: CompositionCreate = Body(..., description="The new version of the canonical COMPOSITION object"),
    if_match: str = Header(..., alias="If-Match", description="The UID of the preceding version to be updated. Must match the UID in the URL"),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Updates an existing `COMPOSITION` by creating a new version.
    This operation is used for making corrections or additions to a clinical document.
    The `preceding_version_uid` in the path identifies the version to be replaced.

    Optimistic locking is enforced via the mandatory `If-Match` header, which must
    be set to the `preceding_version_uid`.

    A new `CONTRIBUTION` with `change_type: modification` is created to audit
    this change. The new version of the `COMPOSITION` is returned in the body.
    """

    new_composition = await update_composition(
        ehr_id=ehr_id,
        preceding_version_uid=preceding_version_uid,
        if_match=if_match,
        new_composition_data=composition_data,
        db=db
    )

    # Set the headers on the response object
    last_modified_gmt = formatdate(new_composition.time_created.timestamp(), usegmt=True)
    response.headers["ETag"] = f'"{new_composition.uid}"'
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/composition/{new_composition.uid}"
    response.headers["Last-Modified"] = last_modified_gmt

    return new_composition


@router.delete(
    "/composition/{preceding_version_uid}",
    status_code = status.HTTP_204_NO_CONTENT,
    summary = "Delete composition by version ID",
    responses = delete_composition_responses
)
async def delete_composition_endpoint(
    ehr_id: str,
    preceding_version_uid: str,
    response: Response,
    if_match: str = Header(..., alias = "If-Match", description = "The UID of the preceding version to be deleted. Must match the UID in the URL"),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Logically deletes a `COMPOSITION` from the EHR.

    This operation doesn't physically delete the data. Instead, it creates a new `CONTRIBUTION` with `change_type: deleted` to audit the action
    This makes the specified version of the composition no longer the "latest" version.

    Optimistic locking is enforced via the mandatory `If-Match` header, which must be set to the `preceding_version_uid`.

    On success, this endpoint returns a `204 No Content` status.
    The `ETag`, `Location`, and `Last-Modified` headers are updated to reflect the new state.
    """
    result = await delete_composition_by_preceding_uid(
        ehr_id = ehr_id,
        preceding_version_uid = preceding_version_uid,
        if_match = if_match,
        db = db
    )

    # Return explicit Response with headers and 204 status code
    last_modified_gmt = formatdate(result["time_committed"].timestamp(), usegmt=True)
    return Response(
        status_code=status.HTTP_204_NO_CONTENT,
        headers={
            "ETag": f'"{result["new_audit_uid"]}"',
            "Location": f"/v1/ehr/{ehr_id}/composition/{result['versioned_object_locator']}",
            "Last-Modified": last_modified_gmt
        }
    )


@router.get(
    "/versioned_composition/{versioned_object_uid}",
    response_model=VersionedComposition,
    response_model_by_alias=True,
    status_code=status.HTTP_200_OK,
    summary="Get Versioned Composition metadata",
    responses=get_versioned_composition_responses
)
async def get_versioned_composition_endpoint(
    ehr_id: str,
    versioned_object_uid: str,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves metadata about a VERSIONED_COMPOSITION, which is the container for all versions of a single clinical document.

    This includes its unique identifier (the versioned_object_uid), the EHR that owns it, and the time the very first version was created.
    """
    versioned_composition = await retrieve_versioned_composition(
        ehr_id=ehr_id,
        versioned_object_uid=versioned_object_uid,
        db=db
    )
    return versioned_composition


@router.get(
    "/versioned_composition/{versioned_object_uid}/revision_history",
    response_model=RevisionHistory,
    status_code=status.HTTP_200_OK,
    summary="Get revision history of a Composition",
    responses=get_revision_history_responses
)
async def get_revision_history_endpoint(
    ehr_id: str,
    versioned_object_uid: str,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves the revision history of a VERSIONED_COMPOSITION, which provides
    a list of audits for each version created for that composition.

    This endpoint provides a complete audit trail for a single clinical document
    (identified by its `versioned_object_uid`). It returns a chronological list
    of all changes, including the initial creation, all subsequent modifications,
    and any deletions.
    """

    revision_history = await retrieve_revision_history(
        ehr_id=ehr_id,
        versioned_object_uid=versioned_object_uid,
        db=db
    )

    return revision_history


@router.get(
    "/versioned_composition/{versioned_object_uid}/version",
    response_model=OriginalVersionResponse,
    response_model_by_alias=True,
    status_code=status.HTTP_200_OK,
    summary="Get Composition version at time",
    responses=get_composition_version_at_time_responses,
)
async def get_composition_version_endpoint(
    ehr_id: str,
    versioned_object_uid: str,
    response: Response,
    version_at_time: Optional[str] = Query(None, alias="version_at_time", description="A given time in the extended ISO 8601 format."),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db),
):
    """
    Retrieves a VERSION from the VERSIONED_COMPOSITION identified by `versioned_object_uid`.

    - If `version_at_time` is supplied, it retrieves the VERSION that was extant at the specified time.
    - If `version_at_time` is not supplied, it retrieves the latest (current) VERSION.

    The response is a full VERSION object, which includes the canonical composition data
    along with commit audit details.
    """

    version_response = await retrieve_composition_version(
        ehr_id=ehr_id,
        versioned_object_uid=versioned_object_uid,
        version_at_time=version_at_time,
        db=db,
    )

    version_uid = version_response.uid.value
    
    # Set headers on the injected Response object
    response.headers["ETag"] = f'"{version_uid}"'
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/composition/{version_uid}"

    return version_response



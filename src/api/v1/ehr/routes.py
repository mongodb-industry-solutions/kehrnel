# ehr/routes.py

from fastapi import APIRouter, Depends, status, Body, Query, HTTPException, Response, Header
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional, List, Union, Dict, Any
from email.utils import formatdate
from datetime import datetime, timezone
from pydantic import ValidationError

from src.api.v1.ehr.service import (
    create_ehr, retrieve_ehr_by_id, 
    update_ehr_status, 
    retrieve_ehr_list,
    create_ehr_with_id,
    add_composition, 
    retrieve_composition, 
    update_composition,
    delete_composition_by_preceding_uid,
    retrieve_ehr_by_subject,
    retrieve_ehr_status_by_ehr_id,
    retrieve_ehr_status_by_version_uid,
    retrieve_contribution,
    retrieve_revision_history,
    retrieve_versioned_composition,
    retrieve_composition_version,
    retrieve_versioned_ehr_status,
    retrieve_ehr_status_revision_history,
    retrieve_ehr_status_version
)

from src.api.v1.ehr.models import EHRCreationResponse, EHRStatusCreate, EHRStatus, ErrorResponse, EHR, Composition, CompositionCreate, RevisionHistory, VersionedComposition, OriginalVersionResponse, VersionedEHRStatus

from src.app.core.database import get_mongodb_ehr_db
from src.app.core.models import Contribution
from src.api.v1.ehr.api_responses import (
    get_ehr_by_id_responses,
    create_ehr_api_responses,
    ehr_status_example, 
    update_ehr_status_responses,
    get_ehr_list_responses,
    create_composition_responses,
    get_composition_responses,
    update_composition_responses,
    delete_composition_responses,
    get_ehr_by_subject_responses,
    get_ehr_status_responses,
    get_ehr_status_by_version_id_responses,
    get_contribution_responses,
    get_revision_history_responses,
    get_versioned_composition_responses,
    get_composition_version_at_time_responses,
    get_versioned_ehr_status_responses,
    get_ehr_status_revision_history_responses,
    get_ehr_status_version_at_time_responses
)

router = APIRouter(
    prefix="/ehr",
    tags=["EHR"]
)

@router.put(
    "/{ehr_id}",
    response_model=EHRCreationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create EHR with specified ID",
    responses=create_ehr_api_responses
)
async def create_ehr_with_id_endpoint(
    ehr_id: str,
    response: Response,
    prefer: str = Header("return=minimal", alias="Prefer"),
    payload: Optional[Dict[str, Any]] = Body(default=None),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Creates a new EHR with a client-specified `ehr_id`.
    An optional `EHR_STATUS` can be provided in the body.
    - `Prefer: return=representation` returns the full resource.
    - `Prefer: return=minimal` returns an empty body with headers.
    """
    initial_status: Optional[EHRStatusCreate] = None
    if payload and payload != {}:
        try:
            initial_status = EHRStatusCreate.model_validate(payload)
        except ValidationError as e:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=e.errors())

    ehr_response = await create_ehr_with_id(
        ehr_id=ehr_id, db=db, initial_status=initial_status
    )

    response.headers["Location"] = f"/v1/ehr/{ehr_response.ehr_id.value}"
    response.headers["ETag"] = f'"{ehr_response.ehr_id.value}"'

    if prefer == "return=representation":
        return ehr_response
    else:
        return Response(status_code=status.HTTP_201_CREATED, headers=response.headers)


@router.delete(
    "/{ehr_id}/composition/{preceding_version_uid}",
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


# Endpoint to update a composition by creating a new version
@router.put(
    "/{ehr_id}/composition/{preceding_version_uid}",
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


@router.get(
    "/{ehr_id}/composition/{uid_based_id}",
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


@router.get(
    "/{ehr_id}/versioned_ehr_status",
    response_model=VersionedEHRStatus,
    response_model_by_alias=True,
    status_code=status.HTTP_200_OK,
    summary="Get Versioned EHR_STATUS metadata",
    responses=get_versioned_ehr_status_responses
)
async def get_versioned_ehr_status_endpoint(
    ehr_id: str,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves metadata about a VERSIONED_EHR_STATUS, which is the container 
    for all versions of a single EHR's status.

    This includes its unique identifier (the base versioned_object_uid), the EHR 
    that owns it, and the time the very first version was created.
    """
    versioned_ehr_status = await retrieve_versioned_ehr_status(
        ehr_id=ehr_id,
        db=db
    )

    return versioned_ehr_status


@router.get(
    "/{ehr_id}/versioned_ehr_status/revision_history",
    response_model=RevisionHistory,
    status_code=status.HTTP_200_OK,
    summary="Get revision history of the EHR_STATUS",
    responses=get_ehr_status_revision_history_responses
)
async def get_ehr_status_revision_history_endpoint(
    ehr_id: str,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves the revision history of the VERSIONED_EHR_STATUS, which provides
    a list of audits for each version created for that EHR's status.

    This endpoint provides a complete audit trail for the EHR_STATUS.
    It returns a chronological list of all changes, including the initial
    creation and all subsequent modifications.
    """
    revision_history = await retrieve_ehr_status_revision_history(
        ehr_id=ehr_id,
        db=db
    )

    return revision_history
    

@router.get(
    "/{ehr_id}/ehr_status/{version_uid}",
    response_model=EHRStatus,
    response_model_by_alias=False,
    status_code=status.HTTP_200_OK,
    summary="GET EHR Status by version ID",
    responses=get_ehr_status_by_version_id_responses
)
async def get_ehr_status_by_version_id_endpoint(
    ehr_id: str, 
    version_uid: str,
    response: Response,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves a specific version of the `EHR_STATUS` for a given EHR

    The response includes the full `EHR_STATUS` object for the specified version and sets the `ETag`, `Location`, and `Last-Modified`
    headers for proper resource versioning and caching
    """

    ehr_status, time_committed = await retrieve_ehr_status_by_version_uid(
        ehr_id=ehr_id,
        version_uid=version_uid,
        db=db
    )

    # Set the response headers 
    response.headers["ETag"] = f'"{version_uid}"'
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/ehr_status/{version_uid}"

    last_modified_gmt = formatdate(time_committed.timestamp(), usegmt=True)
    response.headers["Last-Modified"] = last_modified_gmt

    return ehr_status


@router.get(
    "/{ehr_id}/ehr_status",
    response_model = EHRStatus,
    response_model_by_alias = False,
    status_code = status.HTTP_200_OK,
    summary = "Get latest EHR Status",
    responses = get_ehr_status_responses
)
async def get_ehr_status_endpoint(
    ehr_id: str,
    response: Response,
    version_at_time: Optional[str] = Query(None, alias="version_at_time", description="A given time in the extended ISO 8601 format"),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves a version of the `EHR_STATUS` for a given EHR.

    - If `version_at_time` is supplied, retrieves the version extant at the specified time.
    - If `version_at_time` is not supplied, retrieves the latest version.

    The response includes the full `EHR_STATUS` object and sets the `ETag`,
    `Location`, and `Last-Modified` headers for proper resource versioning and caching.
    """
    ehr_status, time_committed = await retrieve_ehr_status_by_ehr_id(
        ehr_id = ehr_id,
        db = db,
        version_at_time=version_at_time
    )

    version_uid = ehr_status.uid.value

    # Set the response headers
    response.headers["ETag"] = f'"{version_uid}"'
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/ehr_status/{version_uid}"

    last_modified_gmt = formatdate(time_committed.timestamp(), usegmt = True)
    response.headers["Last-Modified"] = last_modified_gmt

    return ehr_status

# PUT endpoint to update the EHR_STATUS
@router.put(
    "/{ehr_id}/ehr_status",
    response_model = EHRStatus,
    response_model_by_alias = False,
    status_code = status.HTTP_200_OK,
    summary = "Update EHR Status",
    responses = update_ehr_status_responses
)
async def update_ehr_status_endpoint(
    ehr_id: str,
    response: Response,
    status_update: EHRStatus,
    # The If-Match header is mandatory for safe, concurrent updates
    if_match: str = Header(..., alias = "If-Match", description = "The ETag of the concurrent EHR_STATUS version. Required for optimistic locking"),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Updates the mutable status of a specific EHR (e.g., to make it no longer modifiable).

    To ensure data consistency, this endpoint **requires** the `If-Match` header to be set
    to the ETag (the version UID) of the current `EHR_STATUS`.

    A new `CONTRIBUTION` is created to audit this change. The new `EHR_STATUS` version
    is returned in the body, and the `ETag`, `Last-Modified`, and `Location` headers are updated.
    """

    new_status, time_committed = await update_ehr_status(
        ehr_id = ehr_id,
        status_update_request = status_update,
        if_match = if_match,
        db = db
    )

    # Set the response headers with the new version's details
    new_version_uid = new_status.uid.value
    response.headers["ETag"] = f'"{new_version_uid}"'

     # The Location header now points to the specific version of the EHR_STATUS resource
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/ehr_status/{new_version_uid}"

    # Use the actual commit time for the Last-Modified header for accuracy
    last_modified_gmt = formatdate(time_committed.timestamp(), usegmt = True)
    response.headers["Last-Modified"] = last_modified_gmt

    return new_status

@router.get(
    "",
    response_model=Union[EHR, List[EHR]],
    response_model_by_alias=True,
    summary="Get EHR by subject ID or list EHRs",
    responses={**get_ehr_by_subject_responses, **get_ehr_list_responses}
)
async def get_ehr_by_subject_or_list(
    subject_id: Optional[str] = Query(None, description="The EHR subject id."),
    subject_namespace: Optional[str] = Query(None, description="The EHR subject id namespace."),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves EHR resources. This endpoint has dual functionality:

    - **Find by Subject:** If `subject_id` and `subject_namespace` query parameters
      are provided, it returns the single EHR matching that subject.
    
    - **List EHRs:** If no query parameters are provided, it retrieves a list
      of the 50 most recently created EHRs.
    """

    # If both query parameters are present, find the specific EHR
    if subject_id and subject_namespace:
        ehr_data = await retrieve_ehr_by_subject(
            subject_id=subject_id,
            subject_namespace=subject_namespace,
            db=db
        )
        return ehr_data
    
    # Otherwise, fall back to the original list functionality
    ehr_list_data = await retrieve_ehr_list(db=db, limit=50)
    return ehr_list_data


@router.get(
    "/{ehr_id}",
    response_model=EHR,
    status_code=status.HTTP_200_OK,
    summary="Get EHR by ID",
    responses=get_ehr_by_id_responses
)
async def get_ehr_by_id(
    ehr_id: str,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves a single EHR resource based on its unique identifier (`ehr_id`).

    If the EHR is found, the full EHR document is returned, including its status,
    creation time, and lists of associated contributions and compositions.

    If no EHR with the given `ehr_id` exists, a 404 Not Found error is returned.
    """
    ehr_data = await retrieve_ehr_by_id(ehr_id=ehr_id, db=db)
    return ehr_data


@router.get(
    "/{ehr_id}/contribution/{contribution_uid}",
    response_model=Contribution,
    response_model_by_alias=False,
    status_code=status.HTTP_200_OK,
    summary="Get Contribution by ID",
    responses=get_contribution_responses
)
async def get_contribution_endpoint(
    ehr_id: str,
    contribution_uid: str,
    response: Response,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves a contribution by its unique identifier for a specific EHR.

    Contributions are the audit entries for every change made to an EHR.
    This endpoint allows clients to inspect the details of a change, such as
    the committer, the time, and what versions were created.
    """
    contribution = await retrieve_contribution(
        ehr_id=ehr_id, contribution_uid=contribution_uid, db=db
    )

    response.headers["Location"] = f"/v1/ehr/{ehr_id}/contribution/{contribution_uid}"
    response.headers["ETag"] = f'"{contribution_uid}"'

    return contribution

@router.post(
    "/{ehr_id}/composition",
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
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Creates a new `composition` and adds it to the specified EHR.

    This endpoint creates the first version of a new `composition`.
    - A new `contribution` is created to audit this change.
    - The new `composition` is atomically stored and linked to the EHR.

    Upon successfull creation, the new `composition` object is returned, and the `Location`, `ETag`, and `Last-Modified` headers are set.
    """

    new_composition = await add_composition(
        ehr_id = ehr_id,
        composition_create = composition_create,
        db = db
    )

    # Response Headers
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/composition/{new_composition.uid}"
    response.headers["ETag"] = f'"{new_composition.uid}"'
    last_modified_gmt = formatdate(new_composition.time_created.timestamp(), usegmt = True)
    response.headers["Last-Modified"] = last_modified_gmt

    return new_composition


@router.get(
    "/{ehr_id}/versioned_composition/{versioned_object_uid}/revision_history",
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
    "/{ehr_id}/versioned_composition/{versioned_object_uid}",
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
    "/{ehr_id}/versioned_composition/{versioned_object_uid}/version",
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


@router.post(
    "",
    response_model=EHRCreationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create EHR",
    responses=create_ehr_api_responses
)
async def create_ehr_endpoint(
    response: Response,
    prefer: str = Header("return=minimal", alias="Prefer"),
    payload: Optional[Dict[str, Any]] = Body(
        default=None, 
        description="Optional initial EHR_STATUS.",
        examples=ehr_status_example,
    ),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Creates a new EHR with a server-generated `ehr_id`.
    - `Prefer: return=representation` returns the full resource representation.
    - `Prefer: return=minimal` (default) returns an empty body with headers.
    """
    initial_status: Optional[EHRStatusCreate] = None

    if payload and payload != {}:
        try:
            initial_status = EHRStatusCreate.model_validate(payload)
        except ValidationError as e:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=e.errors())

    ehr_response = await create_ehr(
        db=db,
        initial_status=initial_status
    )

    # Set the response headers using the data from the service layer

    # 1. Set the Location header
    # This points to where the client can retrieve the newly created resource.
    # Note: Ideally, this would use `request.url_for`, but that requires a named "GET" route.
    # For now, we construct it manually.
    response.headers["Location"] = f"/v1/ehr/{ehr_response.ehr_id.value}"

    # 2. Set the ETag header
    # The ETag is used for caching and conditional requests (e.g., If-None-Match).
    # The HTTP spec requires the value to be in double quotes.
    response.headers["ETag"] = f'"{ehr_response.ehr_id.value}"'

    if prefer == "return=representation":
        return ehr_response
    else:
        return Response(status_code=status.HTTP_201_CREATED, headers=response.headers)
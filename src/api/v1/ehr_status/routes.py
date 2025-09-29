from fastapi import APIRouter, Depends, status, Query, Response, Header
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional
from email.utils import formatdate

from src.app.core.database import get_mongodb_ehr_db
from src.api.v1.ehr_status.models import EHRStatus, VersionedEHRStatus
from src.api.v1.common.models import RevisionHistory, OriginalVersionResponse

from src.api.v1.ehr_status.service import (
    update_ehr_status,
    retrieve_ehr_status_by_ehr_id,
    retrieve_ehr_status_by_version_uid,
    retrieve_versioned_ehr_status,
    retrieve_ehr_status_revision_history,
    retrieve_ehr_status_version,
    retrieve_ehr_status_version_by_uid,
)

from src.api.v1.ehr_status.api_responses import (
    get_ehr_status_by_version_id_responses,
    get_ehr_status_responses,
    update_ehr_status_responses,
    get_versioned_ehr_status_responses,
    get_ehr_status_revision_history_responses,
    get_ehr_status_version_at_time_responses,
    get_ehr_status_version_by_id_responses
)


router = APIRouter(
    prefix="/ehr/{ehr_id}",
    tags=["EHR_STATUS"]
)


@router.get(
    "/versioned_ehr_status",
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
    "/versioned_ehr_status/revision_history",
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
    "/versioned_ehr_status/version",
    response_model=OriginalVersionResponse,
    response_model_by_alias=True,
    status_code=status.HTTP_200_OK,
    summary="Get EHR_STATUS version at time",
    responses=get_ehr_status_version_at_time_responses
)
async def get_ehr_status_version_endpoint(
    ehr_id: str,
    response: Response,
    version_at_time: Optional[str] = Query(None, alias="version_at_time", description="A given time in the extended ISO 8601 format"),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves a version from the VERSIONED_EHR_STATUS identified by `ehr_id`

    - If `version_at_time` is supplied, it retrieves the VERSION that was extant at the specified time.
    - If `version_at_time` is not supplied, it retrieves the latest (current) VERSION.

    The response is a full VERSION object, which includes the canonical EHR_STATUS data
    along with commit audit details
    """
    version_response = await retrieve_ehr_status_version(
        ehr_id=ehr_id,
        version_at_time=version_at_time,
        db=db
    )

    version_uid = version_response.uid.value

    # Set headers on the injected Response object
    response.headers["ETag"] = f'"{version_uid}"'
    # The canonical location for a specific version of an EHR_STATUS is this endpoint
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/ehr_status/{version_uid}"

    return version_response


@router.get(
    "/versioned_ehr_status/version/{version_uid}",
    response_model=OriginalVersionResponse,
    response_model_by_alias=True,
    status_code=status.HTTP_200_OK,
    summary="Get EHR_STATUS version by ID",
    responses=get_ehr_status_version_by_id_responses
)
async def get_ehr_status_version_by_id_endpoint(
    ehr_id: str,
    version_uid: str,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves a specific VERSION of the EHR_STATUS identified by `version_uid`.

    The response is a full VERSION object, which includes the canonical EHR_STATUS data
    along with commit audit details.
    """
    version_response = await retrieve_ehr_status_version_by_uid(
        ehr_id=ehr_id,
        version_uid=version_uid,
        db=db
    )
    return version_response


@router.get(
    "/ehr_status/{version_uid}",
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
    "/ehr_status",
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


@router.put(
    "/ehr_status",
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